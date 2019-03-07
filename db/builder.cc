// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/filename.h"
#include "db/dbformat.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

namespace leveldb {

/**
 * 创建ldb文件
 * @param dbname 数据库根目录
 * @param env    环境变量
 * @param options  数据操作选项
 * @param table_cache cache缓存
 * @param iter  迭代器
 * @param meta  文件元数据 -- 输出参数
 */
Status BuildTable(const std::string& dbname,
                  Env* env,
                  const Options& options,
                  TableCache* table_cache,
                  Iterator* iter,
                  FileMetaData* meta) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();//Iterator迭代遍历跳表SkipList 然后跳到第一个节点

  //生成.ldb文件名称 V1.14版本开始 名字叫做ldb
  std::string fname = TableFileName(dbname, meta->number);
  if (iter->Valid()) {
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);// 创建.ldb文件
    if (!s.ok()) {
      return s;
    }
    // 迭代遍历 key-value 存储到TableBuilder对象中
    TableBuilder* builder = new TableBuilder(options, file);
    meta->smallest.DecodeFrom(iter->key());// 第一个key 一定是最小的
    for (; iter->Valid(); iter->Next()) {
      Slice key = iter->key();//这里的Key是InternalKey中rep_,并非用户数据中key
      meta->largest.DecodeFrom(key);//因此key是有序存储的所以遍历迭代保存最大key
      builder->Add(key, iter->value());//将key-value插入到TableBuilder中
    }

    // Finish and check for builder errors
    if (s.ok()) {
      s = builder->Finish();// 写入filter_block metaindex_block index_block以及footer字段数据
      if (s.ok()) {
        meta->file_size = builder->FileSize();
        assert(meta->file_size > 0);
      }
    } else {
      builder->Abandon();
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = NULL;

    if (s.ok()) {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(ReadOptions(),
                                              meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->DeleteFile(fname);
  }
  return s;
}

}  // namespace leveldb
