//
//  scrambled_zipfian_generator.h
//  YCSB-cpp
//
//  Copyright (c) 2021 Junhan Lee <junhanlee2020@gmail.com>.
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_ZIPFIAN_COMP_GENERATOR_H_
#define YCSB_C_ZIPFIAN_COMP_GENERATOR_H_

#include "generator.h"

#include <cstdint>
#include "utils.h"
#include "zipfian_generator.h"

namespace ycsbc {

class ZipfianCompGenerator : public Generator<uint64_t> {
 public:
  ZipfianCompGenerator(uint64_t min, uint64_t max, double zipfian_const) :
      base_(min), num_items_(max - min + 1), generator_(0, 10000000000LL, zipfian_const), zipf_bits(14), uniform_bits(66) { }

  ZipfianCompGenerator(uint64_t min, uint64_t max) :
      base_(min), num_items_(max - min + 1),
      generator_(0, 10000000000LL, ZipfianGenerator::kZipfianConst, kZetan), zipf_bits(14), uniform_bits(66), lcounter_(0) { }

  ZipfianCompGenerator(uint64_t num_items) :
      ZipfianCompGenerator(0, num_items - 1) { }

  uint64_t Next();
  uint64_t Last();

 private:
  static constexpr double kZetan = 26.46902820178302;
  const uint64_t base_;
  const uint64_t num_items_;
  const uint64_t zipf_bits;
  const uint64_t uniform_bits;
  uint64_t lcounter_;
  ZipfianGenerator generator_;

  uint64_t Scramble(uint64_t value) const;
};

inline uint64_t ZipfianCompGenerator::Scramble(uint64_t value) const {
  return base_ + utils::FNVHash64(lcounter_++) % (1<<uniform_bits) << zipf_bits + utils::FNVHash64(value) % num_items_ % (1<<zipf_bits);
}

inline uint64_t ZipfianCompGenerator::Next() {
  return Scramble(generator_.Next());
}

inline uint64_t ZipfianCompGenerator::Last() {
  return Scramble(generator_.Last());
}

}

#endif // YCSB_C_ZIPFIAN_COMP_GENERATOR_H_

