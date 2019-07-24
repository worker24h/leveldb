// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <algorithm>
#include <set>
#include <string>
#include <stdint.h>
#include <stdio.h>
#include <vector>
#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"

namespace leveldb {

const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
struct DBImpl::Writer {
  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv; // port_posix.cc 创建条件变量

  explicit Writer(port::Mutex* mu) : cv(mu) { }
};

struct DBImpl::CompactionState {
  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  // Files produced by compaction
  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
  };
  std::vector<Output> outputs;

  // State kept for output being generated
  WritableFile* outfile;
  TableBuilder* builder;

  uint64_t total_bytes;

  Output* current_output() { return &outputs[outputs.size()-1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        outfile(NULL),
        builder(NULL),
        total_bytes(0) {
  }
};

// Fix user-supplied options to be reasonable
template <class T,class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}

Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != NULL) ? ipolicy : NULL;
  ClipToRange(&result.max_open_files,    64 + kNumNonTableCacheFiles, 50000);
  ClipToRange(&result.write_buffer_size, 64<<10,                      1<<30);
  ClipToRange(&result.max_file_size,     1<<20,                       1<<30);
  ClipToRange(&result.block_size,        1<<10,                       4<<20);
  if (result.info_log == NULL) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = NULL;
    }
  }
  if (result.block_cache == NULL) {
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result;
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      db_lock_(NULL),
      shutting_down_(NULL),
      bg_cv_(&mutex_),
      mem_(NULL),
      imm_(NULL),
      logfile_(NULL),
      logfile_number_(0),
      log_(NULL),
      seed_(0),
      tmp_batch_(new WriteBatch),
      bg_compaction_scheduled_(false),
      manual_compaction_(NULL) {
  has_imm_.Release_Store(NULL);

  // Reserve ten files or so for other uses and give the rest to TableCache.
  // 查找时才会存储到cache中 方便以后在查找 使用LRU算法 局部性原理
  const int table_cache_size = options_.max_open_files - kNumNonTableCacheFiles;
  table_cache_ = new TableCache(dbname_, &options_, table_cache_size);
  
  // 创建VersionSet集合对象
  versions_ = new VersionSet(dbname_, &options_, table_cache_,
                             &internal_comparator_);
}

DBImpl::~DBImpl() {
  // Wait for background work to finish
  mutex_.Lock();
  shutting_down_.Release_Store(this);  // Any non-NULL value is ok
  while (bg_compaction_scheduled_) {
    bg_cv_.Wait();
  }
  mutex_.Unlock();

  if (db_lock_ != NULL) {
    env_->UnlockFile(db_lock_);
  }

  delete versions_;
  if (mem_ != NULL) mem_->Unref();
  if (imm_ != NULL) imm_->Unref();
  delete tmp_batch_;
  delete log_;
  delete logfile_;
  delete table_cache_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }
}

/**
 * 创建数据库
 */
Status DBImpl::NewDB() {
  //设置VersionSet属性
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);
  // 创建MANIFEST文件
  const std::string manifest = DescriptorFileName(dbname_, 1);
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file);// util/env_posix.cc 调用fopen创建文件
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file);
    std::string record;
    //生成记录 对VersionSet进行编码
    new_db.EncodeTo(&record);
    //将记录写入到MANIFEST文件中
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);//创建CURRENT文件指向最新的MANIFEST文件
  } else {
    env_->DeleteFile(manifest);
  }
  return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}
/**
 * 删除废弃文件
 */
void DBImpl::DeleteObsoleteFiles() {
  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames); // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        if (type == kTableFile) {
          table_cache_->Evict(number);
        }
        Log(options_.info_log, "Delete type=%d #%lld\n",
            int(type),
            static_cast<unsigned long long>(number));
        env_->DeleteFile(dbname_ + "/" + filenames[i]);
      }
    }
  }
}

/**
 * 数据库环境恢复
 * @param edit 
 * @param save_manifest 
 */
Status DBImpl::Recover(VersionEdit* edit, bool *save_manifest) {
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(dbname_);
  assert(db_lock_ == NULL);
  // 创建文件锁 支持多进程并发访问同一个数据库 env_指向env_posix.cc
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!s.ok()) {
    return s;
  }
  // 判断CURRENT文件是否存在
  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) {
      s = NewDB(); //创建数据库，主要包含CURRENT、MANIFEST文件，不包含*.log文件
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(
          dbname_, "exists (error_if_exists is true)");
    }
  }
  /**
   * 以上是数据文件校验 如果不存在则创建新的数据库文件  以下内容是从数据库文件 
   * 中恢复数据,如版本信息、MemTable
   */
  s = versions_->Recover(save_manifest);//version_set.cc 
  if (!s.ok()) {
    return s;
  }
  SequenceNumber max_sequence(0);

  // Recover from all newer log files than the ones named in the
  // descriptor (new log files may have been added by the previous
  // incarnation without registering them in the descriptor).
  //
  // Note that PrevLogNumber() is no longer used, but we pay
  // attention to it in case we are recovering a database
  // produced by an older version of leveldb.
  const uint64_t min_log = versions_->LogNumber();
  const uint64_t prev_log = versions_->PrevLogNumber();
  //获取目录下所有目录或文件名字
  std::vector<std::string> filenames;
  s = env_->GetChildren(dbname_, &filenames);
  if (!s.ok()) {
    return s;
  }
  std::set<uint64_t> expected;
  versions_->AddLiveFiles(&expected);
  uint64_t number;
  FileType type;
  std::vector<uint64_t> logs;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      expected.erase(number);
      if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
        logs.push_back(number);
    }
  }
  if (!expected.empty()) {//不空表示有文件丢失 出错
    char buf[50];
    snprintf(buf, sizeof(buf), "%d missing files; e.g.",
             static_cast<int>(expected.size()));
    return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
  }

  // Recover in the order in which the logs were generated
  // 按从小到大顺序读取.log文件 生成MemTable结构
  std::sort(logs.begin(), logs.end());
  for (size_t i = 0; i < logs.size(); i++) {
    s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
                       &max_sequence);
    if (!s.ok()) {
      return s;
    }

    // The previous incarnation may not have written any MANIFEST
    // records after allocating this log number.  So we manually
    // update the file number allocation counter in VersionSet.
    versions_->MarkFileNumberUsed(logs[i]);
  }

  if (versions_->LastSequence() < max_sequence) {
    versions_->SetLastSequence(max_sequence);//保存最大记录号
  }

  return Status::OK();
}

/**
 * 读取.log文件 恢复MemTable
 * @param log_number .log文件编号
 * @param last_log   是否为最后一个.log文件
 * @param save_manifest 是否需要重新创建一个MANIFEST文件
 * @param edit 操作
 * @param max_sequence 最大序号 输出参数
 */
Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // NULL if options_.paranoid_checks==false
    virtual void Corruption(size_t bytes, const Status& s) {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == NULL ? "(ignoring error) " : ""),
          fname, static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != NULL && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : NULL);
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true/*checksum*/,
                     0/*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long) log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  int compactions = 0;
  MemTable* mem = NULL;
  while (reader.ReadRecord(&record, &scratch) &&
         status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(
          record.size(), Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    if (mem == NULL) {
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, mem);
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq =
        WriteBatchInternal::Sequence(&batch) +
        WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      compactions++;
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, NULL);
      mem->Unref();
      mem = NULL;
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
    }
  }

  delete file;

  // See if we should keep reusing the last log file.
  if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
    assert(logfile_ == NULL);
    assert(log_ == NULL);
    assert(mem_ == NULL);
    uint64_t lfile_size;
    if (env_->GetFileSize(fname, &lfile_size).ok() &&
        env_->NewAppendableFile(fname, &logfile_).ok()) {
      Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
      log_ = new log::Writer(logfile_, lfile_size);
      logfile_number_ = log_number;
      if (mem != NULL) {
        mem_ = mem;
        mem = NULL;
      } else {
        // mem can be NULL if lognum exists but was empty.
        mem_ = new MemTable(internal_comparator_);
        mem_->Ref();
      }
    }
  }

  if (mem != NULL) {
    // mem did not get reused; compact it.
    if (status.ok()) {
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, NULL);
    }
    mem->Unref();
  }

  return status;
}

/**
 * 将MemTable写入到level0文件中
 * @param mem   MemTable对象
 * @param edit  用于写入到MANIFEST文件 版本信息  输出参数
 * @param base  当前db version信息
 */
Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;// 保存level0文件 元数据
  meta.number = versions_->NewFileNumber();// 要创建新文件 获取新文件编号
  pending_outputs_.insert(meta.number);
  Iterator* iter = mem->NewIterator();
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long) meta.number);

  Status s;
  {
    mutex_.Unlock();//将memtable中数据写入到ldb文件中 并返回元数据meta
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long) meta.number,
      (unsigned long long) meta.file_size,
      s.ToString().c_str());
  delete iter;
  pending_outputs_.erase(meta.number);


  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;//默认放到level0
  if (s.ok() && meta.file_size > 0) {
    /* 获取用户数据 key 非内部internal_key */
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();

    /**
     * 虽然函数名字是将数据写到level0中 但是最终是否写到level0中取决于
     * PickLevelForMemTableOutput返回值  该函数会根据key选择合适的层
     * 通常会保存在level0，本次存储的最小key和最大key不在level0范围内 就有可能
     * 存储到更高层 为什么要这样做呢? 如果可以放到更高层 可以减少压缩频率
     */
    if (base != NULL) {
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }
    //保存metafiledata 到了这里就确定了文件所属层次
    edit->AddFile(level, meta.number, meta.file_size,
                  meta.smallest, meta.largest);
  }

  /* 压缩统计信息 */
  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);
  return s;
}

/**
 * 压缩MemTable
 * 将imm_数据压缩到level0中sstable文件里
 */
void DBImpl::CompactMemTable() {
  mutex_.AssertHeld();
  assert(imm_ != NULL);

  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  Status s = WriteLevel0Table(imm_, &edit, base); //将imm_写入到文件 将version信息写到MANIFEST中
  base->Unref();

  if (s.ok() && shutting_down_.Acquire_Load()) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, &mutex_);//更新到Manifest文件中
  }

  if (s.ok()) {//释放Immutable Table内存
    // Commit to the new state
    imm_->Unref();
    imm_ = NULL;
    has_imm_.Release_Store(NULL);
    DeleteObsoleteFiles();//删除残余文件
  } else {
    RecordBackgroundError(s);//后台压缩线程 遇到问题 发起通知
  }
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  TEST_CompactMemTable(); // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin,const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == NULL) {
    manual.begin = NULL;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == NULL) {
    manual.end = NULL;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.Acquire_Load() && bg_error_.ok()) {
    if (manual_compaction_ == NULL) {  // Idle
      manual_compaction_ = &manual;
      MaybeScheduleCompaction();
    } else {  // Running either my compaction or another compaction.
      bg_cv_.Wait();
    }
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = NULL;
  }
}

Status DBImpl::TEST_CompactMemTable() {
  // NULL batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), NULL);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != NULL && bg_error_.ok()) {
      bg_cv_.Wait();
    }
    if (imm_ != NULL) {
      s = bg_error_;
    }
  }
  return s;
}

void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    bg_cv_.SignalAll();
  }
}
/**
 * 尝试调度压缩流程
 * 压缩场景原因:
 *    一种是某一层级的文件数过多或者文件总大小超过预定门限，
 *    另一种是level n 和level n+1重叠严重，无效seek次数太多。(level n 和level n＋1的文件，key的范围可能交叉导致)
 */
void DBImpl::MaybeScheduleCompaction() {
  mutex_.AssertHeld();
  if (bg_compaction_scheduled_) {
    // Already scheduled
  } else if (shutting_down_.Acquire_Load()) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (imm_ == NULL &&
             manual_compaction_ == NULL &&
             !versions_->NeedsCompaction()) {
    // No work to be done
  } else {
    bg_compaction_scheduled_ = true;
    env_->Schedule(&DBImpl::BGWork, this); //创建线程 执行压缩
  }
}

/**
 * 压缩线程线程函数 回调函数
 */
void DBImpl::BGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}

/**
 * 压缩处理
 */
void DBImpl::BackgroundCall() {
  MutexLock l(&mutex_);
  assert(bg_compaction_scheduled_);
  if (shutting_down_.Acquire_Load()) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundCompaction();//压缩处理
  }

  bg_compaction_scheduled_ = false; //表示压缩完成

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  MaybeScheduleCompaction(); //再次尝试压缩 因为有可能上一次压缩产生的文件比较多 所以在此进行压缩
  bg_cv_.SignalAll();
}
/**
 * 压缩真正入口函数
 * 1、imm_ MemTable不空则压缩MemTable 生成ldb(sstable)文件
 * 2、对现有level文件进行压缩，避免各个level文件过多以及清理掉标记为删除的数据
 */
void DBImpl::BackgroundCompaction() {
  mutex_.AssertHeld();
  //压缩Immutable MemTable 不是空 说明存在则进行dump到文件中
  if (imm_ != NULL) {
    CompactMemTable();
    return;
  }

  Compaction* c;//保存压缩相关数据
  bool is_manual = (manual_compaction_ != NULL);
  InternalKey manual_end;
  if (is_manual) {//手动压缩 一般用于测试debug
    ManualCompaction* m = manual_compaction_;
    c = versions_->CompactRange(m->level, m->begin, m->end);
    m->done = (c == NULL);
    if (c != NULL) {
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level,
        (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else {//选出 最适合压缩的level以及文件元数据 此函数并没有进行压缩
    c = versions_->PickCompaction();
  }

  Status status;
  if (c == NULL) {
    // Nothing to do
  } else if (!is_manual && c->IsTrivialMove()) {//文件不需要合并 只需要修改所属层次
    // Move file to next level
    assert(c->num_input_files(0) == 1);
    FileMetaData* f = c->input(0, 0);
    c->edit()->DeleteFile(c->level(), f->number);
    c->edit()->AddFile(c->level() + 1, f->number, f->file_size,
                       f->smallest, f->largest);
    status = versions_->LogAndApply(c->edit(), &mutex_);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
        static_cast<unsigned long long>(f->number),
        c->level() + 1,
        static_cast<unsigned long long>(f->file_size),
        status.ToString().c_str(),
        versions_->LevelSummary(&tmp));
  } else {//level和level+1中有key值重合 因此需要合并
    CompactionState* compact = new CompactionState(c);
    status = DoCompactionWork(compact);//执行压缩
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    CleanupCompaction(compact);
    c->ReleaseInputs();
    DeleteObsoleteFiles();
  }
  delete c;

  if (status.ok()) {
    // Done
  } else if (shutting_down_.Acquire_Load()) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log,
        "Compaction error: %s", status.ToString().c_str());
  }

  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      m->tmp_storage = manual_end;
      m->begin = &m->tmp_storage;
    }
    manual_compaction_ = NULL;
  }
}

void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->builder != NULL) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == NULL);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}

Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != NULL);
  assert(compact->builder == NULL);
  uint64_t file_number;
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);
    mutex_.Unlock();
  }

  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);
  Status s = env_->NewWritableFile(fname, &compact->outfile);
  if (s.ok()) {
    compact->builder = new TableBuilder(options_, compact->outfile);
  }
  return s;
}

Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != NULL);
  assert(compact->outfile != NULL);
  assert(compact->builder != NULL);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = NULL;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = NULL;

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    Iterator* iter = table_cache_->NewIterator(ReadOptions(),
                                               output_number,
                                               current_bytes);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      Log(options_.info_log,
          "Generated table #%llu@%d: %lld keys, %lld bytes",
          (unsigned long long) output_number,
          compact->compaction->level(),
          (unsigned long long) current_entries,
          (unsigned long long) current_bytes);
    }
  }
  return s;
}


Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();
  Log(options_.info_log,  "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(
        level + 1,
        out.number, out.file_size, out.smallest, out.largest);
  }
  return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
}

/**
 * 执行压缩
 * @param compact 压缩信息
 */
Status DBImpl::DoCompactionWork(CompactionState* compact) {
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Log(options_.info_log,  "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == NULL);
  assert(compact->outfile == NULL);

  if (snapshots_.empty()) {//如果没有快照，则重复的旧k/v数据都可以删掉
    compact->smallest_snapshot = versions_->LastSequence();
  } else {//如果有快照，则只有sequenceNumber小于最老的快照的sequenceNumber的旧k/v数据才可以删掉
    compact->smallest_snapshot = snapshots_.oldest()->number_;
  }

  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();

  //创建迭代器 这个创建迭代器的过程非常重要  为下面判断数据是否有效 起到至关重要的作用
  Iterator* input = versions_->MakeInputIterator(compact->compaction);
  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  
  for (; input->Valid() && !shutting_down_.Acquire_Load(); ) {//循环遍历
    // Prioritize immutable compaction work
    if (has_imm_.NoBarrier_Load() != NULL) {
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();
      if (imm_ != NULL) {
        CompactMemTable();//优先压缩immutable memtable
        bg_cv_.SignalAll();  // Wakeup MakeRoomForWrite() if necessary
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);
    }

    Slice key = input->key();//InternalKey
    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != NULL) {
      status = FinishCompactionOutputFile(compact, input);
      if (!status.ok()) {
        break;
      }
    }

    // Handle key/value, add to state, etc.
    bool drop = false;//代表记录是否要被删除 true 要从数据库中删除
    if (!ParseInternalKey(key, &ikey)) {//解析key 失败
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {//解析key 成功
      /**
       * 经过排序之后 相同的user key被放到迭代器 组织到一起了(相同user key放到一起)
       * current_user_key 保存上次user key
       * ikey.user_key是本次循环中 处理的user key
       * 如果compare返回非0 表示这两个user key不一样
       */
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key,
                                     Slice(current_user_key)) != 0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;//表示第一次出现 设置为current user key
        last_sequence_for_key = kMaxSequenceNumber; //设置为最大序列号
      }

      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;    // (A)
      } else if (ikey.type == kTypeDeletion &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
        // IsBaseLevelForKey函数返回true表示user_key不存在于高层中   false 表示存在于高层中
        // For this user key 对于当前user key来说:
        // (1) there is no data in higher levels 它不存在高层ldb文件中
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        //要删除该记录 && 序列号比之前数据库中还要小 && 当前记录不存在于高层次文件中 所以标记为删除
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;
    }
#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

    if (!drop) {//不是删除操作 写入文件
      // Open output file if necessary
      if (compact->builder == NULL) {
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }
      }
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
      }
      compact->current_output()->largest.DecodeFrom(key);
      compact->builder->Add(key, input->value());

      // Close output file if it is big enough
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
        status = FinishCompactionOutputFile(compact, input);
        if (!status.ok()) {
          break;
        }
      }
    }

    input->Next(); //获取下一条记录
  }

  if (status.ok() && shutting_down_.Acquire_Load()) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && compact->builder != NULL) {
    status = FinishCompactionOutputFile(compact, input);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = NULL;

  // 统计信息
  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }

  mutex_.Lock();
  stats_[compact->compaction->level() + 1].Add(stats);

  if (status.ok()) {
    status = InstallCompactionResults(compact);
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log,
      "compacted to: %s", versions_->LevelSummary(&tmp));
  return status;
}

namespace {
struct IterState {
  port::Mutex* mu;
  Version* version;
  MemTable* mem;
  MemTable* imm;
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != NULL) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}
}  // namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  IterState* cleanup = new IterState;
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  if (imm_ != NULL) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }
  versions_->current()->AddIterators(options, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  cleanup->mu = &mutex_;
  cleanup->mem = mem_;
  cleanup->imm = imm_;
  cleanup->version = versions_->current();
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, NULL);

  *seed = ++seed_;
  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

/**
 * 查询
 * @param options 查询选项
 * @param key 查询key
 * @param value 输出参数 如果找到则赋值给value
 */
Status DBImpl::Get(const ReadOptions& options,
                   const Slice& key,
                   std::string* value) {
  Status s;
  MutexLock l(&mutex_);
  SequenceNumber snapshot;
  if (options.snapshot != NULL) {
    snapshot = reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_;
  } else {
    snapshot = versions_->LastSequence();//查询操作 不增加序列号
  }

  MemTable* mem = mem_;
  MemTable* imm = imm_;
  Version* current = versions_->current();
  mem->Ref();
  if (imm != NULL) imm->Ref();
  current->Ref();

  bool have_stat_update = false;
  Version::GetStats stats;

  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    LookupKey lkey(key, snapshot);
    if (mem->Get(lkey, value, &s)) {//查找mem table
      // Done
    } else if (imm != NULL && imm->Get(lkey, value, &s)) {//查找immutable mem table
      // Done
    } else {
      // 查找ldb文件 最新的版本信息Version都保存在current中
      s = current->Get(options, lkey, value, &stats);
      have_stat_update = true;
    }
    mutex_.Lock();
  }
  //是否需要启动压缩流程
  if (have_stat_update && current->UpdateStats(stats)) {
    MaybeScheduleCompaction();//表示由于某个文件seek次数过多需要合并
  }
  mem->Unref();
  if (imm != NULL) imm->Unref();
  current->Unref();
  return s;
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
  return NewDBIterator(
      this, user_comparator(), iter,
      (options.snapshot != NULL
       ? reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_
       : latest_snapshot),
      seed);
}

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) {
    MaybeScheduleCompaction();
  }
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* s) {
  MutexLock l(&mutex_);
  snapshots_.Delete(reinterpret_cast<const SnapshotImpl*>(s));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

/**
 * 写操作
 * @param options  写操作选项
 * @param my_batch 批任务
 */
Status DBImpl::Write(const WriteOptions& options, WriteBatch* my_batch) {
  Writer w(&mutex_); // 结构Writer在本文件 创建条件变量
  w.batch = my_batch;
  w.sync = options.sync;
  w.done = false;

  MutexLock l(&mutex_); // 创建互斥锁
  writers_.push_back(&w);

  //生产者消费者模式
  //多线程并发时  需要等待队列首是本线程添加的批任务 才可继续往下执行 否则进行等待
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  if (w.done) {
    return w.status;
  }

  // May temporarily unlock and wait. 
  // 保证有足够空间可写. 该方正常返回一定保证有足够空间可写入数据
  Status status = MakeRoomForWrite(my_batch == NULL);
  
  uint64_t last_sequence = versions_->LastSequence();//表示上次已经使用的序列号
  Writer* last_writer = &w;
  
  if (status.ok() && my_batch != NULL) {  // NULL batch is for compactions
    WriteBatch* updates = BuildBatchGroup(&last_writer);//将多个WriteBatch合并成一个WriteBatch
    WriteBatchInternal::SetSequence(updates, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(updates); //下一个批任务的起始序号为last_sequence+1

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    {
      mutex_.Unlock();
      //先将record写入到文件中 再将record插入到mem_表中
      // 1、Contents方法是返回Slice对象
      // 2、此处AddRecord方法调用log_writer.cc中AddRecord方法
      status = log_->AddRecord(WriteBatchInternal::Contents(updates));
      bool sync_error = false;
      if (status.ok() && options.sync) {
        status = logfile_->Sync();
        if (!status.ok()) {
          sync_error = true;
        }
      }
      // 只有在写入文件成功后才 插入到mem_表中
      if (status.ok()) {
        status = WriteBatchInternal::InsertInto(updates, mem_);
      }
      mutex_.Lock();
      if (sync_error) {
        // The state of the log file is indeterminate: the log record we
        // just added may or may not show up when the DB is re-opened.
        // So we force the DB into a mode where all future writes fail.
        RecordBackgroundError(status);
      }
    }
    if (updates == tmp_batch_) tmp_batch_->Clear();

    versions_->SetLastSequence(last_sequence);
  }
  // 生产者消费者模式 通知生产者
  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }

  return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-NULL batch
/**
 * 创建BatchGroup  将多个WriteBatch合并成一个WriteBatch
 * @param last_writer 最近要执行write操作的对象
 * @return 
 */
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != NULL);

  size_t size = WriteBatchInternal::ByteSize(first->batch); //返回batch字节数

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  // 避免每次写入过大
  size_t max_size = 1 << 20;//1M
  if (size <= (128<<10)) {
    max_size = size + (128<<10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {// 如果同步类型不一致 则退出循环
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != NULL) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *result 统一合并到成员变量tmp_batch_中 做为返回值返回给上层
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_; //空的batch
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
/*
 * 确保有足够空间可写
 * @param force true表示强制立刻写入  false表示延迟写入
 * 如果mem_没有可用空间可写，则会重新生成mem_ 旧的mem_转成imm_ 然后启动后台线程
 * 进行压缩处理等相关操作
 */
Status DBImpl::MakeRoomForWrite(bool force) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  bool allow_delay = !force; //是否延迟写入
  Status s;
  while (true) {
    if (!bg_error_.ok()) {//后台线程background 在将imm_写入磁盘(level0时)发生错误
      // Yield previous error
      s = bg_error_;
      break;
    } else if (
        allow_delay &&
        versions_->NumLevelFiles(0) >= config::kL0_SlowdownWritesTrigger) {
      // We are getting close to hitting a hard limit on the number of
      // L0 files.  Rather than delaying a single write by several
      // seconds when we hit the hard limit, start delaying each
      // individual write by 1ms to reduce latency variance.  Also,
      // this delay hands over some CPU to the compaction thread in
      // case it is sharing the same core as the writer.
      // 当文件数目达到8个则进行延迟写入  延迟写入只进行一次
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000);//直接睡眠1000ms
      allow_delay = false;  // Do not delay a single write more than once
      mutex_.Lock();
    } else if (!force &&
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
      // There is room in current memtable
      // MemTable的内存是动态扩展，
      //    当MemTable已经使用的空间达到了阈值(4M),则不再继续向当前MemTable对象追加数据(需要重新创建)  
      //    当MemTable已经使用的空间没有达到阈值(4M), 则继续使用
      break;
    } else if (imm_ != NULL) {/* 表示mem_已满 需要等待imm_持久化到磁盘 */
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      Log(options_.info_log, "Current memtable full; waiting...\n");
      bg_cv_.Wait();
    } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
      // There are too many level-0 files.   level-0文件数目太多需要等待压缩
      Log(options_.info_log, "Too many L0 files; waiting...\n");
      bg_cv_.Wait();
    } else {//imm_为空，mem_没有空间可写
      // Attempt to switch to a new memtable and trigger compaction of old
      assert(versions_->PrevLogNumber() == 0);
      uint64_t new_log_number = versions_->NewFileNumber();
      WritableFile* lfile = NULL;
      s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);//创建新的log文件
      if (!s.ok()) {
        // Avoid chewing through file number space in a tight loop.
        versions_->ReuseFileNumber(new_log_number);
        break;
      }
      delete log_;
      delete logfile_;
      logfile_ = lfile;
      logfile_number_ = new_log_number;
      log_ = new log::Writer(lfile);
      imm_ = mem_;
      has_imm_.Release_Store(imm_);
      mem_ = new MemTable(internal_comparator_);//创建新的MemTable对象
      mem_->Ref();
      force = false;   // Do not force another compaction if have room
      MaybeScheduleCompaction();//启动后台线程 将imm_写到level0
    }
  }
  return s;
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "%d",
               versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    char buf[200];
    snprintf(buf, sizeof(buf),
             "                               Compactions\n"
             "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
             "--------------------------------------------------\n"
             );
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        snprintf(
            buf, sizeof(buf),
            "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
            level,
            files,
            versions_->NumLevelBytes(level) / 1048576.0,
            stats_[level].micros / 1e6,
            stats_[level].bytes_read / 1048576.0,
            stats_[level].bytes_written / 1048576.0);
        value->append(buf);
      }
    }
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  } else if (in == "approximate-memory-usage") {
    size_t total_usage = options_.block_cache->TotalCharge();
    if (mem_) {
      total_usage += mem_->ApproximateMemoryUsage();
    }
    if (imm_) {
      total_usage += imm_->ApproximateMemoryUsage();
    }
    char buf[50];
    snprintf(buf, sizeof(buf), "%llu",
             static_cast<unsigned long long>(total_usage));
    value->append(buf);
    return true;
  }

  return false;
}

void DBImpl::GetApproximateSizes(
    const Range* range, int n,
    uint64_t* sizes) {
  // TODO(opt): better implementation
  Version* v;
  {
    MutexLock l(&mutex_);
    versions_->current()->Ref();
    v = versions_->current();
  }

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  {
    MutexLock l(&mutex_);
    v->Unref();
  }
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
/**
 * 向leveldb中添加数据
 * @param opt 写操作选项
 * @param key 关键字
 * @param value 值
 */
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  // 创建一个批任务 将数据保存到批处理中
  WriteBatch batch;
  batch.Put(key, value);

  // 执行写操作
  return Write(opt, &batch); //方法定义在当前文件
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() { }

Status DB::Open(const Options& options, const std::string& dbname,
                DB** dbptr) {
  *dbptr = NULL;

  DBImpl* impl = new DBImpl(options, dbname);
  impl->mutex_.Lock();
  VersionEdit edit;
  // Recover handles create_if_missing, error_if_exists 恢复数据
  bool save_manifest = false;
  Status s = impl->Recover(&edit, &save_manifest);
  
  if (s.ok() && impl->mem_ == NULL) {
    // Create new log and a corresponding memtable.
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    // 创建*.log文件
    WritableFile* lfile;
    s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                     &lfile);
    if (s.ok()) {
      edit.SetLogNumber(new_log_number);
      impl->logfile_ = lfile;
      impl->logfile_number_ = new_log_number;
      impl->log_ = new log::Writer(lfile);
      impl->mem_ = new MemTable(impl->internal_comparator_);
      impl->mem_->Ref();
    }
  }
  
  if (s.ok() && save_manifest) {//save_manifest为true表示需要重新创建一个MANIFEST文件
    edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
    edit.SetLogNumber(impl->logfile_number_);
    s = impl->versions_->LogAndApply(&edit, &impl->mutex_);//由于log相关数据变化 所以要写回Version信息
  }
  
  if (s.ok()) {
    impl->DeleteObsoleteFiles();
    impl->MaybeScheduleCompaction();
  }
  
  impl->mutex_.Unlock();
  
  if (s.ok()) {
    assert(impl->mem_ != NULL);
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

Snapshot::~Snapshot() {
}

Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  // Ignore error in case directory does not exist
  env->GetChildren(dbname, &filenames);
  if (filenames.empty()) {
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  Status result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->DeleteFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->DeleteFile(lockname);
    env->DeleteDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

}  // namespace leveldb
