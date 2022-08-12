//
//  rocksdb_db.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#include "remixdb_db.h"

#include "core/core_workload.h"
#include "core/db_factory.h"
#include "core/properties.h"
#include "core/utils.h"
#include "remixdb/ctypes.h"
#include "remixdb/lib.h"
#include "remixdb/kv.h"
#include "remixdb/sst.h"
#include "remixdb/xdb.h"
#include <stdio.h>

namespace {
  const std::string PROP_NAME = "remixdb.dbname";
  const std::string PROP_NAME_DEFAULT = "";

  const std::string PROP_FORMAT = "remixdb.format";
  const std::string PROP_FORMAT_DEFAULT = "single";

  const std::string PROP_CACHE_SIZE = "remixdb.cachesz";
  const std::string PROP_CACHE_SIZE_DEFAULT = "1024";

  const std::string PROP_MT_SIZE = "remixdb.mtsz";
  const std::string PROP_MT_SIZE_DEFAULT = "1024";

} // anonymous

namespace ycsbc {

struct xdb *RemixdbDB::db_ = nullptr;
//struct xdb_ref *RemixdbDB::ref_ = nullptr;
int RemixdbDB::ref_cnt_ = 0;
std::mutex RemixdbDB::mu_;

void RemixdbDB::Init() {
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
    method_read_ = &RemixdbDB::ReadSingle;
    method_scan_ = &RemixdbDB::ScanSingle;
    method_update_ = &RemixdbDB::UpdateSingle;
    method_insert_ = &RemixdbDB::InsertSingle;
    method_delete_ = &RemixdbDB::DeleteSingle;
#ifdef USE_MERGEUPDATE
    if (props.GetProperty(PROP_MERGEUPDATE, PROP_MERGEUPDATE_DEFAULT) == "true") {
      method_update_ = &RemixdbDB::MergeSingle;
    }
#endif
  } else {
    throw utils::Exception("unknown format");
  }
  fieldcount_ = std::stoi(props.GetProperty(CoreWorkload::FIELD_COUNT_PROPERTY,
                                            CoreWorkload::FIELD_COUNT_DEFAULT));

  ref_cnt_++;

  if (!db_) {
    const std::string &db_path = props.GetProperty(PROP_NAME, PROP_NAME_DEFAULT);
    if (db_path == "") {
      throw utils::Exception("Remixdb db path is missing");
    }

    const u64 cachesz = a2u64(props.GetProperty(PROP_CACHE_SIZE, PROP_CACHE_SIZE_DEFAULT).c_str());
    const u64 mtsz = a2u64(props.GetProperty(PROP_MT_SIZE, PROP_MT_SIZE_DEFAULT).c_str());

    db_ = remixdb_open(db_path.c_str(), cachesz, mtsz, true);
    if (!db_) {
      throw utils::Exception(std::string("Remixdb Open Failed\n"));
    }
  }

  ref_ = remixdb_ref(db_);
  //printf("RemixdbInit: %p %p\n", db_, ref_);
}

void RemixdbDB::Cleanup() {
  const std::lock_guard<std::mutex> lock(mu_);
  //printf("RemixdbCleanup: %p %p %d\n", db_, ref_, ref_cnt_);
  remixdb_unref(ref_);
  if (--ref_cnt_) {
    return;
  }
  remixdb_close(db_);
}

void RemixdbDB::SerializeRow(const std::vector<Field> &values, std::string &data) {
  for (const Field &field : values) {
    uint32_t len = field.name.size();
    data.append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data.append(field.name.data(), field.name.size());
    len = field.value.size();
    data.append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data.append(field.value.data(), field.value.size());
  }
}

void RemixdbDB::DeserializeRowFilter(std::vector<Field> &values, const char *p, const char *lim,
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

void RemixdbDB::DeserializeRowFilter(std::vector<Field> &values, const std::string &data,
                                     const std::vector<std::string> &fields) {
  const char *p = data.data();
  const char *lim = p + data.size();
  DeserializeRowFilter(values, p, lim, fields);
}

void RemixdbDB::DeserializeRow(std::vector<Field> &values, const char *p, const char *lim) {
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

void RemixdbDB::DeserializeRow(std::vector<Field> &values, const std::string &data) {
  const char *p = data.data();
  const char *lim = p + data.size();
  DeserializeRow(values, p, lim);
}

DB::Status RemixdbDB::ReadSingle(const std::string &table, const std::string &key,
                                 const std::vector<std::string> *fields,
                                 std::vector<Field> &result) {
  bool r;
  void* vbuf_out = malloc(sizeof(1024));
  u32 vlen_out;
  r = remixdb_get(ref_, (void*)key.c_str(), key.size(), vbuf_out, &vlen_out);
  if (!r) {
    return kNotFound;
  }
  std::string data(static_cast<char*>(vbuf_out), static_cast<const size_t>(vlen_out));

  if (fields != nullptr) {
    DeserializeRowFilter(result, data, *fields);
  } else {
    DeserializeRow(result, data);
    assert(result.size() == static_cast<size_t>(fieldcount_));
  }
  return kOK;
}

DB::Status RemixdbDB::ScanSingle(const std::string &table, const std::string &key, int len,
                                 const std::vector<std::string> *fields,
                                 std::vector<std::vector<Field>> &result) {
  struct xdb_iter* const db_iter = remixdb_iter_create(ref_);

  remixdb_iter_seek(db_iter, (void*)key.c_str(), key.size());

  for (int i = 0; remixdb_iter_valid(db_iter) && i < len; i++) {
    void* kbuf_out = malloc(sizeof(1024));
    void* vbuf_out = malloc(sizeof(1024));
    u32 klen_out;
    u32 vlen_out;

    remixdb_iter_peek(db_iter, kbuf_out, &klen_out, vbuf_out, &vlen_out); // Get KV
    std::string data(static_cast<char*>(vbuf_out), static_cast<const size_t>(vlen_out));

    result.push_back(std::vector<Field>());
    std::vector<Field> &values = result.back();
    if (fields != nullptr) {
      DeserializeRowFilter(values, data, *fields);
    } else {
      DeserializeRow(values, data);
      assert(values.size() == static_cast<size_t>(fieldcount_));
    }
    remixdb_iter_skip1(db_iter); // Next
  }

  remixdb_iter_destroy(db_iter);
  return kOK;
}

DB::Status RemixdbDB::UpdateSingle(const std::string &table, const std::string &key,
                                   std::vector<Field> &values) {
  bool r;
  void* out = malloc(sizeof(1024));
  u32 vlen_out = 0;
  r = remixdb_get(ref_, (void*)key.c_str(), key.size(), out, &vlen_out );
  if (!r) {
    return kNotFound;
  }
  std::string data(static_cast<char*>(out), static_cast<const size_t>(vlen_out)); 

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

  data.clear();
  SerializeRow(current_values, data);
  r = remixdb_put(ref_, (void*)key.c_str(), key.size(), (void*)data.c_str(), data.size());
  if (!r) {
    throw utils::Exception(std::string("Remixdb Put: ")+ key);
  }
  return kOK;
}

DB::Status RemixdbDB::MergeSingle(const std::string &table, const std::string &key,
                                  std::vector<Field> &values) {
  fprintf(stderr, "RemixDB does not support Merge\n");
  return kError;
}

DB::Status RemixdbDB::InsertSingle(const std::string &table, const std::string &key,
                                   std::vector<Field> &values) {
  std::string data;
  SerializeRow(values, data);
  remixdb_put(ref_, (void*)key.c_str(), key.size(), (void*)data.c_str(), data.size());
  return kOK;
}

DB::Status RemixdbDB::DeleteSingle(const std::string &table, const std::string &key) {
  remixdb_del(ref_, (void*)key.c_str(), key.size());
  return kOK;
}

DB *NewRemixdbDB() {
  return new RemixdbDB;
}

const bool registered = DBFactory::RegisterDB("remixdb", NewRemixdbDB);

} // ycsbc
