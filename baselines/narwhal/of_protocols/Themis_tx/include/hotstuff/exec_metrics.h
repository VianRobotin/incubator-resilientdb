// Pearl-compatible execution metrics for the baseline systems.
//
// Pearl logs, per executed block, how many transactions a replica executed
// and their mean latency from first arrival at that replica (from a client)
// to execution ("executed_batch" in autobahn.cpp).  ExecMetrics logs the same
// line, in the same glog-style format, every 100 ms, so one parser
// (scripts/deploy/performance/baseline_metrics.py) computes every system's
// throughput and latency exactly as Pearl's.  Identical copies live in each
// baseline tree (FairDAG/Tusk, Pompe, Themis).
#pragma once

#include <sys/syscall.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <mutex>
#include <thread>
#include <unordered_map>

template <typename Key, typename Hash = std::hash<Key>>
class ExecMetrics {
 public:
  ExecMetrics() : flusher_([this] { Run(); }) {}
  ~ExecMetrics() {
    stop_ = true;
    flusher_.join();
  }

  // A client transaction, or a client batch of `ntx` transactions, first
  // reached this replica.  Later copies keep the first arrival time.
  void Arrived(const Key& key, uint32_t ntx = 1) {
    int64_t now = NowUs();
    std::lock_guard<std::mutex> lk(mu_);
    if (!started_) {
      started_ = true;
      Print("first_client_txn");
    }
    arrival_.emplace(key, Arrival{now, ntx});
  }

  // `key` executed here.  Only transactions that arrived here from a client
  // contribute latency, as in Pearl; any other key counts as one.
  void Executed(const Key& key) {
    int64_t now = NowUs();
    std::lock_guard<std::mutex> lk(mu_);
    auto it = arrival_.find(key);
    if (it == arrival_.end()) {
      ++count_;
      return;
    }
    const uint32_t ntx = it->second.ntx;
    int64_t lat = now - it->second.time;
    count_ += ntx;
    lat_sum_ += lat * ntx;
    lat_max_ = std::max(lat_max_, lat);
    samples_ += ntx;
    arrival_.erase(it);
  }

 private:
  static int64_t NowUs() {
    timeval tv;
    gettimeofday(&tv, nullptr);
    return static_cast<int64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
  }

  // One line in glog's format ("E20261029 12:32:34.359519 <tid> file] msg"),
  // which Pearl's parser matches.  Caller holds mu_.
  static void Print(const char* msg) {
    timeval tv;
    gettimeofday(&tv, nullptr);
    tm t;
    localtime_r(&tv.tv_sec, &t);
    fprintf(stderr, "E%04d%02d%02d %02d:%02d:%02d.%06ld %ld exec_metrics.h:0] %s\n",
            t.tm_year + 1900, t.tm_mon + 1, t.tm_mday, t.tm_hour, t.tm_min,
            t.tm_sec, static_cast<long>(tv.tv_usec),
            static_cast<long>(syscall(SYS_gettid)), msg);
    fflush(stderr);
  }

  void Run() {
    while (!stop_) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      std::lock_guard<std::mutex> lk(mu_);
      // Like Pearl, which logs every block, log idle intervals too once the
      // clients have started: throughput divides by the logged span.
      if (!started_) continue;
      char line[160];
      snprintf(line, sizeof(line),
               "executed_batch slot:%lu count:%lu avg_latency_us:%ld "
               "max_latency_us:%ld pending:0",
               static_cast<unsigned long>(++slot_),
               static_cast<unsigned long>(count_),
               static_cast<long>(samples_ ? lat_sum_ / samples_ : 0),
               static_cast<long>(lat_max_));
      Print(line);
      count_ = samples_ = 0;
      lat_sum_ = lat_max_ = 0;
    }
  }

  struct Arrival {
    int64_t time;
    uint32_t ntx;
  };

  std::mutex mu_;
  std::unordered_map<Key, Arrival, Hash> arrival_;
  bool started_ = false;
  uint64_t slot_ = 0, count_ = 0, samples_ = 0;
  int64_t lat_sum_ = 0, lat_max_ = 0;
  std::atomic<bool> stop_{false};
  std::thread flusher_;  // last: starts after the members above exist
};
