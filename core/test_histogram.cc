#include "histogram.h"
#include <cmath>
#include <iostream>

static const double kIota = 0.1;
static const HistogramBucketMapper bucketMapper;

void PopulateHistogram(Histogram& histogram,
             uint64_t low, uint64_t high, uint64_t loop = 1) {
  for (; loop > 0; loop --) {
    for (uint64_t i = low; i <= high; i++) {
      histogram.Add(i);
    }  
  }  
}

void BasicOperation(Histogram& histogram) {
  PopulateHistogram(histogram, 1, 110, 10); //fill up to bucket [70, 110)
  
  HistogramData data;
  histogram.Data(&data);
  
  assert(fabs(histogram.Percentile(100.0) - 110.0) <= kIota);
  assert(fabs(data.percentile99 - 108.9) <= kIota);
  assert(fabs(data.percentile95 - 104.5) <= kIota);
  assert(fabs(data.median - 55.0) <= kIota);
  assert(fabs(data.average - 55.5) == 0);
}

void EmptyHistogram(Histogram& histogram) {
  assert(histogram.min() == bucketMapper.LastValue());  
  assert(histogram.max() == 0);  
  assert(histogram.num() == 0);  
  assert(histogram.Median() == 0.0);  
  assert(histogram.Percentile(85.0) == 0.0);  
  assert(histogram.Average() == 0.0);  
  assert(histogram.StandardDeviation() == 0.0);  
}

void ClearHistogram(Histogram& histogram) {
  for (uint64_t i = 1; i<= 100; i++) {
    histogram.Add(i);
  }
  histogram.Clear();
  assert(histogram.Empty());  
  assert(histogram.Median() == 0);  
  assert(histogram.Percentile(85.0) == 0.0);  
  assert(histogram.Average() == 0.0);  
}


void TEST_BasicOperation(void) {
  HistogramImpl histogram;
  BasicOperation(histogram); 
}

void TEST_BoundaryValue(void) {
  HistogramImpl histogram;
  histogram.Add(0);
  histogram.Add(1);
  assert(fabs(histogram.Percentile(50.0) - 0.5) <= kIota);
}

void TEST_EmptyHistogram(void) {
  HistogramImpl histogram;
  EmptyHistogram(histogram);
}

void TEST_ClearHistogram(void) {
  HistogramImpl histogram;
  ClearHistogram(histogram);
}


void RUN_TEST(std::string name, void (*f)(void)) {
  std::cout << "[TEST] " << name << "::BEGIN" << std::endl;  
  (*f)();
  std::cout << "[TEST] " << name << "::PASS" << std::endl;  
}

int main(void) {
  RUN_TEST("BasicOperation", TEST_BasicOperation);
  RUN_TEST("BoundaryValue", TEST_BoundaryValue);
  RUN_TEST("EmptyHistogram", TEST_EmptyHistogram);
  RUN_TEST("ClearHistogram", TEST_ClearHistogram);
  return 0;  
}
