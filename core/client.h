//
//  client.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_

#include <string>
#include <map>
#include <fstream>
#include "db.h"
#include "core_workload.h"
#include "utils.h"
#include "countdown_latch.h"

namespace ycsbc {

inline int ClientThread(ycsbc::DB *db, ycsbc::CoreWorkload *wl, const int num_ops, bool is_loading,
                        bool init_db, bool print_key_stat, bool cleanup_db, CountDownLatch *latch) {
  if (init_db) {
    db->Init();
  }

  int oks = 0;
  std::map<uint64_t, uint64_t> key_stats;
  for (int i = 0; i < num_ops; ++i) {
    if (is_loading) {
      oks += wl->DoInsert(*db, print_key_stat, key_stats);
    } else {
      oks += wl->DoTransaction(*db, print_key_stat, key_stats);
    }
  }

  if(print_key_stat) {
     // open file
     std::ofstream ofile;
     ofile.open("/home/ceph/YCSB-cpp/print_key_stats.txt");
     for(auto k: key_stats) {
       ofile << k.first << "\t" << k.second << "\n";
     }
     // close file
     ofile.close();
  }

  if (cleanup_db) {
    db->Cleanup();
  }

  latch->CountDown();
  return oks;
}

} // ycsbc

#endif // YCSB_C_CLIENT_H_
