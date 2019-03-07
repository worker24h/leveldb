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
 * ����ldb�ļ�
 * @param dbname ���ݿ��Ŀ¼
 * @param env    ��������
 * @param options  ���ݲ���ѡ��
 * @param table_cache cache����
 * @param iter  ������
 * @param meta  �ļ�Ԫ���� -- �������
 */
Status BuildTable(const std::string& dbname,
                  Env* env,
                  const Options& options,
                  TableCache* table_cache,
                  Iterator* iter,
                  FileMetaData* meta) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();//Iterator������������SkipList Ȼ��������һ���ڵ�

  //����.ldb�ļ����� V1.14�汾��ʼ ���ֽ���ldb
  std::string fname = TableFileName(dbname, meta->number);
  if (iter->Valid()) {
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);// ����.ldb�ļ�
    if (!s.ok()) {
      return s;
    }
    // �������� key-value �洢��TableBuilder������
    TableBuilder* builder = new TableBuilder(options, file);
    meta->smallest.DecodeFrom(iter->key());// ��һ��key һ������С��
    for (; iter->Valid(); iter->Next()) {
      Slice key = iter->key();//�����Key��InternalKey��rep_,�����û�������key
      meta->largest.DecodeFrom(key);//���key������洢�����Ա��������������key
      builder->Add(key, iter->value());//��key-value���뵽TableBuilder��
    }

    // Finish and check for builder errors
    if (s.ok()) {
      s = builder->Finish();// д��filter_block metaindex_block index_block�Լ�footer�ֶ�����
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
