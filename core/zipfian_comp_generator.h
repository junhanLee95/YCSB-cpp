//
//  scrambled_zipfian_generator.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_ZIPFIAN_COMP_GENERATOR_H_
#define YCSB_C_ZIPFIAN_COMP_GENERATOR_H_

#include "generator.h"

#include <atomic>
#include <random>
#include <cstdint>
#include "utils.h"
#include "zipfian_generator.h"
#include "counter_generator.h"

namespace ycsbc {

class ZipfianCompGenerator : public Generator<uint64_t> {
 public:
  ZipfianCompGenerator(uint64_t min, uint64_t max, double zipfian_const) :
      base_(min), num_items_(max - min + 1), generator_(0, 10000000000LL, zipfian_const), dist_(0, uniform_bit_) { }

  ZipfianCompGenerator(uint64_t min, uint64_t max) :
      base_(min), num_items_(max - min + 1),
      generator_(0, 10000000000LL, ZipfianGenerator::kZipfianConst, kZetan), dist_(0, uniform_bit_) { }

  ZipfianCompGenerator(uint64_t num_items) :
      ZipfianCompGenerator(0, num_items - 1) { }

  uint64_t Next();
  uint64_t Last();

 private:
  static constexpr double kZetan = 26.46902820178302;
  const uint64_t base_;
  const uint64_t num_items_;
  static const uint64_t zipf_bit_ = 1<<14;
  static const uint64_t uniform_bit_ = 1<<18;
  ZipfianGenerator generator_;
  std::mt19937_64 mt_;
  std::uniform_int_distribution<uint64_t> dist_;
  uint64_t last_int_;

  uint64_t Scramble(uint64_t u_value, uint64_t z_value) const;
};

inline uint64_t ZipfianCompGenerator::Scramble(uint64_t u_value, uint64_t z_value) const {
  return base_ +  (utils::FNVHash64(z_value) % zipf_bit_ * uniform_bit_ + u_value % uniform_bit_) % num_items_ ;
}

inline uint64_t ZipfianCompGenerator::Next() {
  return last_int_ = Scramble(dist_(mt_), generator_.Next());
}

inline uint64_t ZipfianCompGenerator::Last() {
  return last_int_;
}

}

#endif // YCSB_C_ZIPFIAN_COMP_GENERATOR_H_
