//
//  remixdb_db.h
//  YCSB-cpp
//
//  Copyright (c) 2021 Junhan Lee <junahlee2020@gmail.com>.
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//
#ifndef YCSB_C_REMIXDB_DB_H_
#define YCSB_C_REMIXDB_DB_H_

#include <string>
#include <mutex>

#include "core/db.h"
#include "core/properties.h"
#include "remixdb/ctypes.h"
#include "remixdb/lib.h"
#include "remixdb/kv.h"
#include "remixdb/sst.h"
#include "remixdb/xdb.h"
namespace ycsbc {

class RemixdbDB : public DB {
 public:
  RemixdbDB() {}
  ~RemixdbDB() {}

  void Init();

  void Cleanup();

  Status Read(const std::string &table, const std::string &key,
              const std::vector<std::string> *fields, std::vector<Field> &result) {
    return (this->*(method_read_))(table, key, fields, result);
  }

  Status Scan(const std::string &table, const std::string &key, int len,
              const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result) {
    return (this->*(method_scan_))(table, key, len, fields, result);
  }

  Status Update(const std::string &table, const std::string &key, std::vector<Field> &values) {
    return (this->*(method_update_))(table, key, values);
  }

  Status Insert(const std::string &table, const std::string &key, std::vector<Field> &values) {
    return (this->*(method_insert_))(table, key, values);
  }

  Status Delete(const std::string &table, const std::string &key) {
    return (this->*(method_delete_))(table, key);
  }

 private:
  enum RemixFormat {
    kSingleRow,
  };
  RemixFormat format_;

  static void SerializeRow(const std::vector<Field> &values, std::string &data);
  static void DeserializeRowFilter(std::vector<Field> &values, const char *p, const char *lim,
                                   const std::vector<std::string> &fields);
  static void DeserializeRowFilter(std::vector<Field> &values, const std::string &data,
                                   const std::vector<std::string> &fields);
  static void DeserializeRow(std::vector<Field> &values, const char *p, const char *lim);
  static void DeserializeRow(std::vector<Field> &values, const std::string &data);

  Status ReadSingle(const std::string &table, const std::string &key,
                    const std::vector<std::string> *fields, std::vector<Field> &result);
  Status ScanSingle(const std::string &table, const std::string &key, int len,
                    const std::vector<std::string> *fields,
                    std::vector<std::vector<Field>> &result);
  Status UpdateSingle(const std::string &table, const std::string &key,
                      std::vector<Field> &values);
  Status MergeSingle(const std::string &table, const std::string &key,
                     std::vector<Field> &values);
  Status InsertSingle(const std::string &table, const std::string &key,
                      std::vector<Field> &values);
  Status DeleteSingle(const std::string &table, const std::string &key);

  Status (RemixdbDB::*method_read_)(const std::string &, const std:: string &,
                                    const std::vector<std::string> *, std::vector<Field> &);
  Status (RemixdbDB::*method_scan_)(const std::string &, const std::string &,
                                    int, const std::vector<std::string> *,
                                    std::vector<std::vector<Field>> &);
  Status (RemixdbDB::*method_update_)(const std::string &, const std::string &,
                                      std::vector<Field> &);
  Status (RemixdbDB::*method_insert_)(const std::string &, const std::string &,
                                      std::vector<Field> &);
  Status (RemixdbDB::*method_delete_)(const std::string &, const std::string &);

  int fieldcount_;
  static struct xdb* db_;
  /*
  static struct xdb_ref* ref_;
  static int ref_cnt_;
  static std::mutex mu_;
  */
  struct xdb_ref* ref_ = nullptr;
  static int ref_cnt_;
  static std::mutex mu_;
};

DB *NewRemixdbDB();

} // ycsbc

#endif // YCSB_C_ROCKSDB_DB_H_
