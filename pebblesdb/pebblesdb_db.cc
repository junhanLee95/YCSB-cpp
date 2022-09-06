//
//  rocksdb_db.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#include "pebblesdb_db.h"

#include "core/core_workload.h"
#include "core/db_factory.h"
#include "core/properties.h"
#include "core/utils.h"

#include <pebblesdb/cache.h>
#include <pebblesdb/status.h>
#include <pebblesdb/write_batch.h>
#include <pebblesdb/cache.h>
#include <iostream>

#include <stdio.h>
using leveldb::NewLRUCache;
namespace {
  const std::string PROP_NAME = "pebblesdb.dbname";
  const std::string PROP_NAME_DEFAULT = "";

  const std::string PROP_FORMAT = "pebblesdb.format";
  const std::string PROP_FORMAT_DEFAULT = "single";

  const std::string PROP_WRITE_BUFFER_SIZE = "pebblesdb.memtablesz";
  const std::string PROP_WRITE_BUFFER_SIZE_DEFAULT = "67108864";

  const std::string PROP_BLOCK_SIZE = "pebblesdb.blocksz";
  const std::string PROP_BLOCK_SIZE_DEFAULT = "4096";

  const std::string PROP_CACHE_SIZE = "pebblesdb.cachesz";
  const std::string PROP_CACHE_SIZE_DEFAULT = "1024";

  const std::string PROP_DESTROY = "pebblesdb.cachesz";
  const std::string PROP_DESTROY_DEFAULT = "false";
} // anonymous

namespace ycsbc {

leveldb::DB *PebblesdbDB::db_ = nullptr;
int PebblesdbDB::ref_cnt_ = 0;
std::mutex PebblesdbDB::mu_;

void PebblesdbDB::Init() {
// merge operator disabled by default due to link error
#ifdef USE_MERGEUPDATE
  class YCSBUpdateMerge : {
   public:
    virtual bool Merge(const rocksdb::Slice &key, const rocksdb::Slice *existing_value,
                       const rocksdb::Slice &value, std::string *new_value,
                       rocksdb::Logger *logger) const override {
      assert(existing_value);

      std::vector<Field> values;
      const char *p = existing_value->data();
      const char *lim = p + existing_value->size();
      DeserializeRow(values, p, lim);

      std::vector<Field> new_values;
      p = value.data();
      lim = p + value.size();
      DeserializeRow(new_values, p, lim);

      for (Field &new_field : new_values) {
        bool found = false;
        for (Field &field : values) {
          if (field.name == new_field.name) {
            found = true;
            field.value = new_field.value;
            break;
          }
        }
        if (!found) {
          values.push_back(new_field);
        }
      }

      SerializeRow(values, *new_value);
      return true;
    }

    virtual const char *Name() const override {
      return "YCSBUpdateMerge";
    }
  };
#endif
  const std::lock_guard<std::mutex> lock(mu_);

  const utils::Properties &props = *props_;
  const std::string format = props.GetProperty(PROP_FORMAT, PROP_FORMAT_DEFAULT);
  if (format == "single") {
    format_ = kSingleRow;
    method_read_ = &PebblesdbDB::ReadSingle;
    method_scan_ = &PebblesdbDB::ScanSingle;
    method_update_ = &PebblesdbDB::UpdateSingle;
    method_insert_ = &PebblesdbDB::InsertSingle;
    method_delete_ = &PebblesdbDB::DeleteSingle;
#ifdef USE_MERGEUPDATE
    if (props.GetProperty(PROP_MERGEUPDATE, PROP_MERGEUPDATE_DEFAULT) == "true") {
      method_update_ = &PebblesdbDB::MergeSingle;
    }
#endif
  } else {
    throw utils::Exception("unknown format");
  }
  fieldcount_ = std::stoi(props.GetProperty(CoreWorkload::FIELD_COUNT_PROPERTY,
                                            CoreWorkload::FIELD_COUNT_DEFAULT));

  ref_cnt_++;
  if (db_) {
    return;
  }
  const std::string &db_path = props.GetProperty(PROP_NAME, PROP_NAME_DEFAULT);
  if (db_path == "") {
    throw utils::Exception("Pebblesdb db path is missing");
  }
  
  leveldb::Options opts;
  opts.create_if_missing = true;
  opts.write_buffer_size = std::stoi(props.GetProperty(PROP_WRITE_BUFFER_SIZE,PROP_WRITE_BUFFER_SIZE_DEFAULT));
  opts.block_size = std::stoi(props.GetProperty(PROP_BLOCK_SIZE,PROP_BLOCK_SIZE_DEFAULT));
  size_t cache_size = std::stoul(props.GetProperty(PROP_CACHE_SIZE,PROP_CACHE_SIZE_DEFAULT));
  opts.block_cache = NewLRUCache(cache_size);
  //opts.filter_policy = leveldb::NewBloomFilterPolicy(10);

  leveldb::Status s;
  if (props.GetProperty(PROP_DESTROY, PROP_DESTROY_DEFAULT) == "true") {
    s = leveldb::DestroyDB(db_path, opts);
    if (!s.ok()) {
      throw utils::Exception(std::string("RocksDB DestroyDB: ") + s.ToString());
    }
  }

  s = leveldb::DB::Open(opts, db_path, &db_);
  if (!s.ok()) {
    throw utils::Exception(std::string("PebblesDB Open Failed\n"));
  } 
}

void PebblesdbDB::Cleanup() {
  const std::lock_guard<std::mutex> lock(mu_);
  if (--ref_cnt_) {
    return;
  }
	delete db_;
}

void PebblesdbDB::SerializeRow(const std::vector<Field> &values, std::string &data) {
  for (const Field &field : values) {
    uint32_t len = field.name.size();
    data.append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data.append(field.name.data(), field.name.size());
    len = field.value.size();
    data.append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data.append(field.value.data(), field.value.size());
  }
}

void PebblesdbDB::DeserializeRowFilter(std::vector<Field> &values, const char *p, const char *lim,
                                     const std::vector<std::string> &fields) {
  std::vector<std::string>::const_iterator filter_iter = fields.begin();
  while (p != lim && filter_iter != fields.end()) {
    assert(p < lim);
    uint32_t len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string field(p, static_cast<const size_t>(len));
    p += len;
    len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string value(p, static_cast<const size_t>(len));
    p += len;
    if (*filter_iter == field) {
      values.push_back({field, value});
      filter_iter++;
    }
  }
  assert(values.size() == fields.size());
}

void PebblesdbDB::DeserializeRowFilter(std::vector<Field> &values, const std::string &data,
                                     const std::vector<std::string> &fields) {
  const char *p = data.data();
  const char *lim = p + data.size();
  DeserializeRowFilter(values, p, lim, fields);
}

void PebblesdbDB::DeserializeRow(std::vector<Field> &values, const char *p, const char *lim) {
  while (p != lim) {
    assert(p < lim);
    uint32_t len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string field(p, static_cast<const size_t>(len));
    p += len;
    len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string value(p, static_cast<const size_t>(len));
    p += len;
    values.push_back({field, value});
  }
}

void PebblesdbDB::DeserializeRow(std::vector<Field> &values, const std::string &data) {
  const char *p = data.data();
  const char *lim = p + data.size();
  DeserializeRow(values, p, lim);
}

DB::Status PebblesdbDB::ReadSingle(const std::string &table, const std::string &key,
                                 const std::vector<std::string> *fields,
                                 std::vector<Field> &result) {
  std::string data;
	leveldb::Status s = db_->Get(leveldb::ReadOptions(), key, &data);
  if (s.IsNotFound()) {
    return kNotFound;
  } else if (!s.ok()) {
		throw utils::Exception(std::string("PebblesDB Get: ") + s.ToString());
	}
	if (fields != nullptr) {
    DeserializeRowFilter(result, data, *fields);
  } else {
    DeserializeRow(result, data);
    assert(result.size() == static_cast<size_t>(fieldcount_));
  }
  return kOK;
}

DB::Status PebblesdbDB::ScanSingle(const std::string &table, const std::string &key, int len,
                                 const std::vector<std::string> *fields,
                                 std::vector<std::vector<Field>> &result) {
  leveldb::Iterator *db_iter = db_->NewIterator(leveldb::ReadOptions());
	db_iter->Seek(key);
  for (int i = 0; db_iter->Valid() && i < len; i++) {
		std::string data = db_iter->value().ToString();
		result.push_back(std::vector<Field>());
		std::vector<Field> &values = result.back();
    if (fields != nullptr) {
      DeserializeRowFilter(values, data, *fields);
    } else {
      DeserializeRow(values, data);
      assert(values.size() == static_cast<size_t>(fieldcount_));
    }
		db_iter->Next();
  }

	delete db_iter;
  return kOK;
}

DB::Status PebblesdbDB::UpdateSingle(const std::string &table, const std::string &key,
                                   std::vector<Field> &values) {
	std::string data;
	leveldb::Status s = db_->Get(leveldb::ReadOptions(), key, &data);
  if (s.IsNotFound()) {
    return kNotFound;
  } else if (!s.ok()) {
		throw utils::Exception(std::string("RocksDB Get: ") + s.ToString());
	}
  std::vector<Field> current_values;
  DeserializeRow(current_values, data);
  assert(current_values.size() == static_cast<size_t>(fieldcount_));
  for (Field &new_field : values) {
    bool found __attribute__((unused)) = false;
    for (Field &cur_field : current_values) {
      if (cur_field.name == new_field.name) {
        found = true;
        cur_field.value = new_field.value;
        break;
      }
    }
    assert(found);
  }
  leveldb::WriteOptions wopt;

  data.clear();
  SerializeRow(current_values, data);
  s = db_->Put(wopt, key, data);
	if (!s.ok()) {
    throw utils::Exception(std::string("Pebblesdb Put: ")+ key);
  }
  return kOK;
}

DB::Status PebblesdbDB::MergeSingle(const std::string &table, const std::string &key,
                                  std::vector<Field> &values) {
  fprintf(stderr, "PebblesDB does not support Merge\n");
  return kError;
}

DB::Status PebblesdbDB::InsertSingle(const std::string &table, const std::string &key,
                                   std::vector<Field> &values) {
  std::string data;
  SerializeRow(values, data);
	leveldb::WriteOptions wopt;
	leveldb::Status s = db_->Put(wopt, key, data);
	if (!s.ok()) { 
		throw utils::Exception(std::string("PebblesDB Put: ") + s.ToString());
	}
	return kOK;
}

DB::Status PebblesdbDB::DeleteSingle(const std::string &table, const std::string &key) {
	leveldb::WriteOptions wopt;
	leveldb::Status s = db_->Delete(wopt, key);
	if (!s.ok()) { 
		throw utils::Exception(std::string("PebblesDB Delete: ") + s.ToString());
	}
	return kOK;
}

DB *NewPebblesdbDB() {
  return new PebblesdbDB;
}

const bool registered = DBFactory::RegisterDB("pebblesdb", NewPebblesdbDB);

} // ycsbc
