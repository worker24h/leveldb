// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"

namespace leveldb {

struct TableAndFile {
  RandomAccessFile* file;
  Table* table;
};

static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

TableCache::TableCache(const std::string& dbname,
                       const Options* options,
                       int entries)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)) {//LRUCache算法 cache容量默认值990
}

TableCache::~TableCache() {
  delete cache_;
}

/**
 * 查找Table
 * @param file_number ldb文件编号  来自FileMetaData
 * @param file_size   ldb文件大小  来自FileMetaData
 * @param handle      cache handle对象 输出参数
 */
Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle) {
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);//按照key进行查找 如果没有找到则说明是新文件
  if (*handle == NULL) {
    std::string fname = TableFileName(dbname_, file_number);//读取ldb文件
    RandomAccessFile* file = NULL;
    Table* table = NULL;
    s = env_->NewRandomAccessFile(fname, &file);
    if (!s.ok()) {// 兼容 1.13版本(含)之前是sst文件作为后缀
      std::string old_fname = SSTTableFileName(dbname_, file_number);
      if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
        s = Status::OK();
      }
    }
    if (s.ok()) {
      s = Table::Open(*options_, file, file_size, &table);//打开文件 创建table对象
    }

    if (!s.ok()) {
      assert(table == NULL);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      TableAndFile* tf = new TableAndFile;//对文件对象和Table对象进行包装
      tf->file = file;
      tf->table = table;//保存table对象
      
      //插入cache中 默认返回LRUHandle对象 此处key是文件编号
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
    }
  }
  return s;
}

Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number,
                                  uint64_t file_size,
                                  Table** tableptr) {
  if (tableptr != NULL) {
    *tableptr = NULL;
  }

  Cache::Handle* handle = NULL;
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  Iterator* result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != NULL) {
    *tableptr = table;
  }
  return result;
}
/**
 * 从ldb中获取数据
 * @param options 选项
 * @param file_number ldb文件编号  来自FileMetaData
 * @param file_size   ldb文件大小  来自FileMetaData
 * @param k           查找key值
 * @param arg         回调函数参数
 * @param saver       回调函数
 */
Status TableCache::Get(const ReadOptions& options,
                       uint64_t file_number,
                       uint64_t file_size,
                       const Slice& k,
                       void* arg,
                       void (*saver)(void*, const Slice&, const Slice&)) {
  Cache::Handle* handle = NULL;
  Status s = FindTable(file_number, file_size, &handle);// 打开ldb文件并且读取出data index block以及meta index block
  if (s.ok()) {
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    s = t->InternalGet(options, k, arg, saver);// 在table中查找k 如果找到则通过saver回调函数进行保存
    cache_->Release(handle);//必须调用 因此在FindTable的时候对Ref加1了 所以要主动释放
  }
  return s;
}

/**
 * 从cache中删除指定文件的所有entry
 */
void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace leveldb
