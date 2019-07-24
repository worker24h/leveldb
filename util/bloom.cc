// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/filter_policy.h"

#include "leveldb/slice.h"
#include "util/hash.h"

namespace leveldb {

namespace {
static uint32_t BloomHash(const Slice& key) {
  return Hash(key.data(), key.size(), 0xbc9f1d34);
}

class BloomFilterPolicy : public FilterPolicy {
 private:
  size_t bits_per_key_;
  size_t k_; /* 表示需要k_个hash函数 最大30个*/

 public:
  explicit BloomFilterPolicy(int bits_per_key)
      : bits_per_key_(bits_per_key) {
    // We intentionally round down to reduce probing cost a little bit
    k_ = static_cast<size_t>(bits_per_key * 0.69);  // 0.69 =~ ln(2)
    if (k_ < 1) k_ = 1;
    if (k_ > 30) k_ = 30;
  }

  virtual const char* Name() const {
    return "leveldb.BuiltinBloomFilter2";
  }
  /**
   * 生成过滤器
   * @param keys 数组
   * @param n    数组keys元素个数
   * @param dst  保存bitmap
   */
  virtual void CreateFilter(const Slice* keys, int n, std::string* dst) const {
    // Compute bloom filter size (in both bits and bytes)
    size_t bits = n * bits_per_key_; //表示bit数组中包含多少位

    // For small n, we can see a very high false positive rate.  Fix it
    // by enforcing a minimum bloom filter length.
    if (bits < 64) bits = 64;//避免误报率过高

    size_t bytes = (bits + 7) / 8;//8bit对齐
    bits = bytes * 8;

    const size_t init_size = dst->size();
    dst->resize(init_size + bytes, 0);
    dst->push_back(static_cast<char>(k_));  // Remember # of probes in filter
    char* array = &(*dst)[init_size];
    for (int i = 0; i < n; i++) {
      // Use double-hashing to generate a sequence of hash values.
      // See analysis in [Kirsch,Mitzenmacher 2006].
      // 对一个key进行两次hash
      uint32_t h = BloomHash(keys[i]);
      const uint32_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
      //对于一个hash值（这里的hash可能很大数字）,通过for循环模拟k个hash场景
      for (size_t j = 0; j < k_; j++) {
        const uint32_t bitpos = h % bits;//对bit数组求余 获取存储的bit
        array[bitpos/8] |= (1 << (bitpos % 8));
        h += delta;
      }
    }
  }

  /**
   * 匹配key是存在当前过滤器中
   * @param key 关键值
   * @param bloom_filter 过滤器
   * @return  false 表示不存在
   *          true  表示key可能存在  是否真的存在还需要对key进行比较
   */
  virtual bool KeyMayMatch(const Slice& key, const Slice& bloom_filter) const {
    const size_t len = bloom_filter.size();
    if (len < 2) return false;

    const char* array = bloom_filter.data();
    const size_t bits = (len - 1) * 8;

    // Use the encoded k so that we can read filters generated by
    // bloom filters created using different parameters.
    const size_t k = array[len-1];
    if (k > 30) {
      // Reserved for potentially new encodings for short bloom filters.
      // Consider it a match.
      return true;
    }

    uint32_t h = BloomHash(key);
    const uint32_t delta = (h >> 17) | (h << 15);  // Rotate right 17 bits
    for (size_t j = 0; j < k; j++) {
      const uint32_t bitpos = h % bits;
      if ((array[bitpos/8] & (1 << (bitpos % 8))) == 0) return false;
      h += delta;
    }
    return true;
  }
};
}

const FilterPolicy* NewBloomFilterPolicy(int bits_per_key) {
  return new BloomFilterPolicy(bits_per_key);
}

}  // namespace leveldb
