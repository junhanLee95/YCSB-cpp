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

#include <cstdint>
#include "utils.h"
#include "zipfian_generator.h"
#include "counter_generator.h"

namespace ycsbc {

class ZipfianCompGenerator : public Generator<uint64_t> {
 public:
  ZipfianCompGenerator(uint64_t min, uint64_t max, double zipfian_const) :
      base_(min), num_items_(max - min + 1), generator_(0, 10000000000LL, zipfian_const), counter_(0) { }

  ZipfianCompGenerator(uint64_t min, uint64_t max) :
      base_(min), num_items_(max - min + 1),
      generator_(0, 10000000000LL, ZipfianGenerator::kZipfianConst, kZetan), counter_(0) { }

  ZipfianCompGenerator(uint64_t num_items) :
      ZipfianCompGenerator(0, num_items - 1) { }

  uint64_t Next();
  uint64_t Last();

 private:
  static constexpr double kZetan = 26.46902820178302;
  const uint64_t base_;
  const uint64_t num_items_;
  static const uint64_t zipf_bit_ = 1<<14; // 10 byte in total
  static const uint64_t uniform_bit_ = 1<<18;
  ZipfianGenerator generator_;
  CounterGenerator counter_;

  uint64_t Scramble(uint64_t u_value, uint64_t z_value) const;
};

inline uint64_t ZipfianCompGenerator::Scramble(uint64_t u_value, uint64_t z_value) const {
  return base_ +  utils::FNVHash64(z_value) % zipf_bit_ + utils::FNVHash64(u_value) % uniform_bit_ * zipf_bit_;
}

inline uint64_t ZipfianCompGenerator::Next() {
  return Scramble(counter_.Next(), generator_.Next());
}

inline uint64_t ZipfianCompGenerator::Last() {
  return Scramble(counter_.Last(), generator_.Next());
}

}

#endif // YCSB_C_ZIPFIAN_COMP_GENERATOR_H_
