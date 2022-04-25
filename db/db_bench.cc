// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <stdio.h>
#include <stdlib.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/types.h>
#include <unistd.h>

#include <ctime>
#include <iomanip>
#include <iostream>
#include <map>
#include <unordered_map>

#include "leveldb/cache.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/write_batch.h"
#include "port/port.h"
#include "util/crc32c.h"
#include "util/generator.h"
#include "util/histogram.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/testutil.h"

using namespace rocksdb::util;

using namespace std::chrono;
// Comma-separated list of operations to run in the specified order
//   Actual benchmarks:
// 		TPCxIoT			-- TPCxIoT benchmark (Insert and Scan)
//		WriteTPCxIoT 	-- 
//		ScanTPCxIoT		--
//   Meta operations:
//      stats       -- Print DB stats
//      sstables    -- Print sstable info
static const char* FLAGS_benchmarks =
    "TPCxIoT,"
    "WriteTPCxIoT,"
    //"scanTPCxIoT,"
    //"sstables,
    //"stats,"
    ;

// Number of key/values to place in database

// Number of read operations to do.  If negative, do FLAGS_num reads.
static int FLAGS_reads = -1;

// Number of concurrent threads to run.
static int FLAGS_threads = 1;
static int Total_num = 10000;
static int FLAGS_num = 10000;  // Total_num / FLAGS_threads; // 0000;
static int FLAGS_sensors = 160;
// Size of each value
static int FLAGS_value_size = 1024;

// Arrange to generate values that shrink to this fraction of
// their original size after compression
static double FLAGS_compression_ratio = 1;

// Print histogram of operation timings
static bool FLAGS_histogram = false;

// Number of bytes to buffer in memtable before compacting
// (initialized to default value by "main")
static int FLAGS_write_buffer_size = 0;

// Number of bytes written to each file.
// (initialized to default value by "main")
static int FLAGS_max_file_size = 0;

// Approximate size of user data packed per block (before compression.
// (initialized to default value by "main")
static int FLAGS_block_size = 0;

// Number of bytes to use as a cache of uncompressed data.
// Negative means use default settings.
static int FLAGS_cache_size = -1;

// Maximum number of files to keep open at the same time (use default if == 0)
static int FLAGS_open_files = 0;

// Bloom filter bits per key.
// Negative means use default settings.
static int FLAGS_bloom_bits = -1;

// If true, do not destroy the existing database.  If you set this
// flag and also specify a benchmark that wants a fresh database, that
// benchmark will fail.
static bool FLAGS_use_existing_db = false;

// If true, reuse existing log/MANIFEST files when re-opening a database.
static bool FLAGS_reuse_logs = false;

// Use the db with the following name.
static const char* FLAGS_db = nullptr;

std::string prekeys[160] = {
    "cent_9_Humidity", "side_8_Humidity", "side_7_Humidity", "ang_30_Humidity", "ang_45_Humidity", "ang_60_Humidity", "ang_90_Humidity",
    "bef_1195_Humidity", "aft_1120_Humidity", "mid_1125_Humidity", "cor_4_Humidity", "cor_1_Humidity", "cor_5_Humidity", "cent_9_Power",
    "side_8_Power", "side_7_Power", "ang_30_Power", "ang_45_Power", "ang_60_Power", "ang_90_Power", "bef_1195_Power", "aft_1120_Power",
    "mid_1125_Power", "cor_4_Power", "cor_1_Power", "cor_5_Power", "cent_9_Pressure", "side_8_Pressure", "side_7_Pressure", "ang_30_Pressure",
    "ang_45_Pressure", "ang_60_Pressure", "ang_90_Pressure", "bef_1195_Pressure", "aft_1120_Pressure", "mid_1125_Pressure",
    "cor_4_Pressure", "cor_1_Pressure", "cor_5_Pressure", "cent_9_Flow", "side_8_Flow", "side_7_Flow", "ang_30_Flow", "ang_45_Flow",
    "ang_60_Flow", "ang_90_Flow", "bef_1195_Flow", "aft_1120_Flow", "mid_1125_Flow", "cor_4_Flow", "cor_1_Flow", "cor_5_Flow", "cent_9_Level",
    "side_8_Level", "side_7_Level", "ang_30_Level", "ang_45_Level", "ang_60_Level", "ang_90_Level", "bef_1195_Level", "aft_1120_Level",
    "mid_1125_Level", "cor_4_Level", "cor_1_Level", "cor_5_Level", "cent_9_Temperature", "side_8_Temperature", "side_7_Temperature", "ang_30_Temperature",
    "ang_45_Temperature", "ang_60_Temperature", "ang_90_Temperature", "bef_1195_Temperature", "aft_1120_Temperature", "mid_1125_Temperature",
    "cor_4_Temperature", "cor_1_Temperature", "cor_5_Temperature", "cent_9_vibration", "side_8_vibration", "side_7_vibration", "ang_30_vibration",
    "ang_45_vibration", "ang_60_vibration", "ang_90_vibration", "bef_1195_vibration", "aft_1120_vibration", "mid_1125_vibration",
    "cor_4_vibration", "cor_1_vibration", "cor_5_vibration", "cent_9_tilt", "side_8_tilt", "side_7_tilt", "ang_30_tilt", "ang_45_tilt", "ang_60_tilt",
    "ang_90_tilt", "bef_1195_tilt", "aft_1120_tilt", "mid_1125_tilt", "cor_4_tilt", "cor_1_tilt", "cor_5_tilt", "cent_9_level2", "side_8_level2",
    "side_7_level2", "ang_30_level2", "ang_45_level2", "ang_60_level2", "ang_90_level2", "bef_1195_level2", "aft_1120_level2", "mid_1125_level2",
    "cor_4_level2", "cor_1_level2", "cor_5_level2", "cent_9_level_vibrating", "side_8_level_vibrating", "side_7_level_vibrating", "ang_30_level_vibrating",
    "ang_45_level_vibrating", "ang_60_level_vibrating", "ang_90_level_vibrating", "bef_1195_level_vibrating", "aft_1120_level_vibrating", "mid_1125_level_vibrating",
    "cor_4_level_vibrating", "cor_1_level_vibrating", "cor_5_level_vibrating", "cent_9_level_rotating", "side_8_level_rotating", "side_7_level_rotating",
    "ang_30_level_rotating", "ang_45_level_rotating", "ang_60_level_rotating", "ang_90_level_rotating", "bef_1195_level_rotating", "aft_1120_level_rotating",
    "mid_1125_level_rotating", "cor_4_level_rotating", "cor_1_level_rotating", "cor_5_level_rotating", "cent_9_level_admittance",
    "side_8_level_admittance", "side_7_level_admittance", "ang_30_level_admittance", "ang_45_level_admittance", "ang_60_level_admittance",
    "ang_90_level_admittance", "bef_1195_level_admittance", "aft_1120_level_admittance", "mid_1125_level_admittance", "cor_4_level_admittance",
    "cor_1_level_admittance", "cor_5_level_admittance", "cent_9_Pneumatic_level", "side_8_Pneumatic_level", "side_7_Pneumatic_level", "ang_30_Pneumatic_level"};

namespace leveldb {

namespace {
leveldb::Env* g_env = nullptr;
leveldb::Env* t_env = nullptr;
// Helper for quickly generating random data.
class RandomGenerator {
   private:
    std::string data_;
    int pos_;

   public:
    RandomGenerator() {
        // We use a limited amount of data over and over again and ensure
        // that it is larger than the compression window (32KB), and also
        // large enough to serve all typical value sizes we want to write.
        Random rnd(301);
        std::string piece;
        while (data_.size() < 1048576) {
            // Add a short fragment that is as compressible as specified
            // by FLAGS_compression_ratio.
            test::CompressibleString(&rnd, FLAGS_compression_ratio, 100, &piece);
            data_.append(piece);
        }
        pos_ = 0;
    }

    Slice Generate(size_t len) {
        if (pos_ + len > data_.size()) {
            pos_ = 0;
            assert(len < data_.size());
        }
        pos_ += len;
        return Slice(data_.data() + pos_ - len, len);
    }
};

#if defined(__linux)
static Slice TrimSpace(Slice s) {
    size_t start = 0;
    while (start < s.size() && isspace(s[start])) {
        start++;
    }
    size_t limit = s.size();
    while (limit > start && isspace(s[limit - 1])) {
        limit--;
    }
    return Slice(s.data() + start, limit - start);
}
#endif

static void AppendWithSpace(std::string* str, Slice msg) {
    if (msg.empty()) return;
    if (!str->empty()) {
        str->push_back(' ');
    }
    str->append(msg.data(), msg.size());
}

class Stats {
   private:
    double start_;
    double finish_;
    double seconds_;
    int done_;
    int next_report_;
    int64_t bytes_;
    double last_op_finish_;
    Histogram hist_;
    std::string message_;

   public:
    Stats() { Start(); }

    void Start() {
        next_report_ = 100;
        last_op_finish_ = start_;
        hist_.Clear();
        done_ = 0;
        bytes_ = 0;
        seconds_ = 0;
        start_ = g_env->NowMicros();
        finish_ = start_;
        message_.clear();
    }

    void Merge(const Stats& other) {
        hist_.Merge(other.hist_);
        done_ += other.done_;
        bytes_ += other.bytes_;
        seconds_ += other.seconds_;
        if (other.start_ < start_) start_ = other.start_;
        if (other.finish_ > finish_) finish_ = other.finish_;

        // Just keep the messages from one thread
        if (message_.empty()) message_ = other.message_;
    }

    void Stop() {
        finish_ = g_env->NowMicros();
        seconds_ = (finish_ - start_) * 1e-6;
    }

    void AddMessage(Slice msg) {
        AppendWithSpace(&message_, msg);
    }
    void StartSingleOp() {
        last_op_finish_ = g_env->NowMicros();
    }

    void FinishedSingleOp() {
        if (FLAGS_histogram) {
            double now = g_env->NowMicros();
            double micros = now - last_op_finish_;
            hist_.Add(micros);
            if (micros > 20000) {
                fprintf(stderr, "long op: %.1f micros%30s\r", micros, "");
                fflush(stderr);
            }
            last_op_finish_ = now;
        }

        done_++;
        if (done_ >= next_report_) {
            if (next_report_ < 1000)
                next_report_ += 100;
            else if (next_report_ < 5000)
                next_report_ += 500;
            else if (next_report_ < 10000)
                next_report_ += 1000;
            else if (next_report_ < 50000)
                next_report_ += 5000;
            else if (next_report_ < 100000)
                next_report_ += 10000;
            else if (next_report_ < 500000)
                next_report_ += 50000;
            else
                next_report_ += 100000;
            fprintf(stderr, "... finished %d ops%30s\r", done_, "");
            fflush(stderr);
        }
    }

    void AddBytes(int64_t n) {
        bytes_ += n;
    }

    void Report(const Slice& name) {
        // Pretend at least one op was done in case we are running a benchmark
        // that does not call FinishedSingleOp().
        if (done_ < 1) done_ = 1;

        std::string extra;
        if (bytes_ > 0) {
            // Rate is computed on actual elapsed time, not the sum of per-thread
            // elapsed times.
            double elapsed = (finish_ - start_) * 1e-6;
            char rate[100];
            snprintf(rate, sizeof(rate), "%6.1f MB/s %f MB",
                     (bytes_ / 1048576.0) / elapsed, (bytes_ / 1048576.0));
            extra = rate;
        }
        AppendWithSpace(&extra, message_);

        fprintf(stdout, "%-12s : %11.3f micros/op;%s%s\n",
                name.ToString().c_str(),
                seconds_ * 1e6 / done_,
                (extra.empty() ? "" : " "),
                extra.c_str());
        if (FLAGS_histogram) {
            fprintf(stdout, "Microseconds per op:\n%s\n", hist_.ToString().c_str());
        }
        fflush(stdout);
    }
};

// State shared by all concurrent executions of the same benchmark.
struct SharedState {
    port::Mutex mu;
    port::CondVar cv GUARDED_BY(mu);
    int total GUARDED_BY(mu);

    // Each thread goes through the following states:
    //    (1) initializing
    //    (2) waiting for others to be initialized
    //    (3) running
    //    (4) done

    int num_initialized GUARDED_BY(mu);
    int num_done GUARDED_BY(mu);
    bool start GUARDED_BY(mu);

    SharedState(int total)
        : cv(&mu), total(total), num_initialized(0), num_done(0), start(false) {}
};

// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState {
    int tid;      // 0..n-1 when running in n threads
    Random rand;  // Has different seeds for different threads
    Stats stats;
    SharedState* shared;
    int sensorNum;

    ThreadState(int index) : tid(index), rand(1000 + index), shared(nullptr) {}
};

}  // namespace

class Benchmark {
   private:
    Cache* cache_;
    const FilterPolicy* filter_policy_;
    DB* db_;
    int num_;
    int value_size_;
    int entries_per_batch_;
    WriteOptions write_options_;
    int reads_;
    int heap_counter_;

    void PrintHeader() {
        const int kKeySize = 16;
        PrintEnvironment();
        fprintf(stdout, "Keys:       %d bytes each\n", kKeySize);
        fprintf(stdout, "Values:     %d bytes each (%d bytes after compression)\n",
                FLAGS_value_size,
                static_cast<int>(FLAGS_value_size * FLAGS_compression_ratio + 0.5));
        fprintf(stdout, "Entries:    %d\n", num_);
        fprintf(stdout, "RawSize:    %.1f MB (estimated)\n",
                ((static_cast<int64_t>(kKeySize + FLAGS_value_size) * num_) / 1048576.0));
        fprintf(stdout, "FileSize:   %.1f MB (estimated)\n",
                (((kKeySize + FLAGS_value_size * FLAGS_compression_ratio) * num_) / 1048576.0));
        PrintWarnings();
        fprintf(stdout, "------------------------------------------------\n");
    }

    void PrintWarnings() {
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
        fprintf(stdout,
                "WARNING: Optimization is disabled: benchmarks unnecessarily slow\n");
#endif
#ifndef NDEBUG
        fprintf(stdout,
                "WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
#endif

        // See if snappy is working by attempting to compress a compressible string
        const char text[] = "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy";
        std::string compressed;
        if (!port::Snappy_Compress(text, sizeof(text), &compressed)) {
            fprintf(stdout, "WARNING: Snappy compression is not enabled\n");
        } else if (compressed.size() >= sizeof(text)) {
            fprintf(stdout, "WARNING: Snappy compression is not effective\n");
        }
    }

    void PrintEnvironment() {
        fprintf(stderr, "SEN-STORE:    version %d.%d\n",
                0, 1);

#if defined(__linux)
        time_t now = time(nullptr);
        fprintf(stderr, "Date:       %s", ctime(&now));  // ctime() adds newline

        FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
        if (cpuinfo != nullptr) {
            char line[1000];
            int num_cpus = 0;
            std::string cpu_type;
            std::string cache_size;
            while (fgets(line, sizeof(line), cpuinfo) != nullptr) {
                const char* sep = strchr(line, ':');
                if (sep == nullptr) {
                    continue;
                }
                Slice key = TrimSpace(Slice(line, sep - 1 - line));
                Slice val = TrimSpace(Slice(sep + 1));
                if (key == "model name") {
                    ++num_cpus;
                    cpu_type = val.ToString();
                } else if (key == "cache size") {
                    cache_size = val.ToString();
                }
            }
            fclose(cpuinfo);
            fprintf(stderr, "CPU:        %d * %s\n", num_cpus, cpu_type.c_str());
            fprintf(stderr, "CPUCache:   %s\n", cache_size.c_str());
        }
#endif
    }

   public:
    Benchmark()
        : cache_(FLAGS_cache_size >= 0 ? NewLRUCache(FLAGS_cache_size) : nullptr),
          filter_policy_(FLAGS_bloom_bits >= 0
                             ? NewBloomFilterPolicy(FLAGS_bloom_bits)
                             : nullptr),
          db_(nullptr),
          num_(FLAGS_num),
          value_size_(FLAGS_value_size),
          entries_per_batch_(1),
          reads_(FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads),
          heap_counter_(0) {
        std::vector<std::string> files;
        g_env->GetChildren(FLAGS_db, &files);
        for (size_t i = 0; i < files.size(); i++) {
            if (Slice(files[i]).starts_with("heap-")) {
                g_env->DeleteFile(std::string(FLAGS_db) + "/" + files[i]);
            }
        }
        if (!FLAGS_use_existing_db) {
            DestroyDB(FLAGS_db, Options());
        }
    }

    ~Benchmark() {
        delete db_;
        delete cache_;
        delete filter_policy_;
    }

    void Run() {
        PrintHeader();
        Open();

        const char* benchmarks = FLAGS_benchmarks;  // Workload List
        while (benchmarks != nullptr) {
            const char* sep = strchr(benchmarks, ',');
            Slice name;
            if (sep == nullptr) {
                name = benchmarks;
                benchmarks = nullptr;
            } else {
                name = Slice(benchmarks, sep - benchmarks);
                benchmarks = sep + 1;
            }

            // Reset parameters that may be overridden below
            num_ = FLAGS_num;
            reads_ = (FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads);
            value_size_ = FLAGS_value_size;
            entries_per_batch_ = 1;
            write_options_ = WriteOptions();

            void (Benchmark::*method)(ThreadState*) = nullptr;
            bool fresh_db = false;
            int num_threads = FLAGS_threads;

            if (name == Slice("open")) {
                method = &Benchmark::OpenBench;
                num_ /= 10000;
                if (num_ < 1) num_ = 1;
            } else if (name == Slice("fillseq")) {
                fresh_db = false;
                method = &Benchmark::WriteSeq;
            } else if (name == Slice("TPCxIoT")) {
                fresh_db = false;
                method = &Benchmark::doTPCxIoT;
            } else if (name == Slice("WriteTPCxIoT")) {
                fresh_db = false;
                method = &Benchmark::WriteTPCxIoT;
            } else if (name == Slice("fillbatch")) {
                fresh_db = true;
                entries_per_batch_ = 1000;
                method = &Benchmark::WriteSeq;
            } else if (name == Slice("scanTPCxIoT")) {
                method = &Benchmark::ScanTPCxIoT;

            } else if (name == Slice("fillrandom")) {
                fresh_db = false;
                method = &Benchmark::WriteRandom;
            } else if (name == Slice("overwrite")) {
                fresh_db = false;
                method = &Benchmark::WriteRandom;
            } else if (name == Slice("fillsync")) {
                fresh_db = true;
                num_ /= 1000;
                write_options_.sync = true;
                method = &Benchmark::WriteRandom;
            } else if (name == Slice("fill100K")) {
                fresh_db = true;
                num_ /= 1000;
                value_size_ = 100 * 1000;
                method = &Benchmark::WriteRandom;
            } else if (name == Slice("readseq")) {
                method = &Benchmark::ReadSequential;
            } else if (name == Slice("readreverse")) {
                method = &Benchmark::ReadReverse;
            } else if (name == Slice("readrandom")) {
                method = &Benchmark::ReadRandom;
            } else if (name == Slice("readmissing")) {
                method = &Benchmark::ReadMissing;
            } else if (name == Slice("seekrandom")) {
                method = &Benchmark::SeekRandom;
            } else if (name == Slice("readhot")) {
                method = &Benchmark::ReadHot;
            } else if (name == Slice("readrandomsmall")) {
                reads_ /= 1000;
                method = &Benchmark::ReadRandom;
            } else if (name == Slice("deleteseq")) {
                method = &Benchmark::DeleteSeq;
            } else if (name == Slice("deleterandom")) {
                method = &Benchmark::DeleteRandom;
            } else if (name == Slice("readwhilewriting")) {
                num_threads++;  // Add extra thread for writing
                method = &Benchmark::ReadWhileWriting;
            } else if (name == Slice("compact")) {
                method = &Benchmark::Compact;
            } else if (name == Slice("crc32c")) {
                method = &Benchmark::Crc32c;
            } else if (name == Slice("snappycomp")) {
                method = &Benchmark::SnappyCompress;
            } else if (name == Slice("snappyuncomp")) {
                method = &Benchmark::SnappyUncompress;
            } else if (name == Slice("heapprofile")) {
                HeapProfile();
            } else if (name == Slice("stats")) {
                PrintStats("leveldb.stats");
            } else if (name == Slice("sstables")) {
                PrintStats("leveldb.sstables");
            } else {
                if (name != Slice()) {  // No error message for empty name
                    fprintf(stderr, "unknown benchmark '%s'\n", name.ToString().c_str());
                }
            }

            if (fresh_db) {
                if (FLAGS_use_existing_db) {
                    fprintf(stdout, "%-12s : skipped (--use_existing_db is true)\n",
                            name.ToString().c_str());
                    method = nullptr;
                } else {
                    delete db_;
                    db_ = nullptr;
                    DestroyDB(FLAGS_db, Options());
                    Open();
                }
            }

            if (method != nullptr) {
                RunBenchmark(num_threads, name, method);
            }
        }
    }

   private:
    struct ThreadArg {
        Benchmark* bm;
        SharedState* shared;
        ThreadState* thread;
        int sensorNum;
        void (Benchmark::*method)(ThreadState*);
    };

    static void ThreadBody(void* v) {
        ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
        SharedState* shared = arg->shared;
        ThreadState* thread = arg->thread;
        thread->sensorNum = arg->sensorNum;

        int sensorNum = arg->sensorNum;
        {
            MutexLock l(&shared->mu);
            shared->num_initialized++;
            if (shared->num_initialized >= shared->total) {
                shared->cv.SignalAll();
            }
            while (!shared->start) {
                shared->cv.Wait();
            }
        }

        thread->stats.Start();
        (arg->bm->*(arg->method))(thread);
        thread->stats.Stop();

        {
            MutexLock l(&shared->mu);
            shared->num_done++;
            if (shared->num_done >= shared->total) {
                shared->cv.SignalAll();
            }
        }
    }

    void RunBenchmark(int n, Slice name,
                      void (Benchmark::*method)(ThreadState*)) {
        SharedState shared(n);

        ThreadArg* arg = new ThreadArg[n];
        for (int i = 0; i < n; i++) {
            arg[i].bm = this;
            arg[i].method = method;
            arg[i].shared = &shared;
            arg[i].thread = new ThreadState(i);
            arg[i].thread->shared = &shared;
            arg[i].sensorNum = i + 1;
            g_env->StartThread(ThreadBody, &arg[i]);



			if (i == 0 && name.ToString() == "TPCxIoT") {
				int substationsNum = ((FLAGS_threads-1) / 10) + 1;
				std::cout << "substations # : " << substationsNum << std::endl;
				int a =1;
				for (a=1; a<= substationsNum; a++)
				{
					std::string pathPrefix = std::to_string(a);// + ":";
					std::string partPrefix = std::to_string((a-1)/2) + ":";
					std::string linePrefix = std::to_string((a-1)/4) + ":";
					//pathPrefix = partPrefix + pathPrefix;
					pathPrefix = linePrefix + partPrefix + pathPrefix;
					int k =0;
					/*for (k = 0; k < std::size(prekeys) ; k++) {
						if(k > FLAGS_sensors -1) 
							break;
						std::string path = pathPrefix + prekeys[k];
						db_->RegisterSensors(path);
					}
                    std::cout << " Sensors #: " << k << std::endl;*/
                    db_->addNode(pathPrefix, "sensor_list.txt");
				}
			}

			if (i == 0 && name.ToString() == "WriteTPCxIoT") {
				int substationsNum = FLAGS_threads;

				for (int a=1; a<= substationsNum; a++)
				{
					std::string pathPrefix = std::to_string(a) + ":";
					for (int k = 0; k < std::size(prekeys) ; k++) {
						if(k > FLAGS_sensors -1) 
							break;
						std::string path = pathPrefix + prekeys[k];
						std::cout << "path= " << path << std::endl;
						db_->RegisterSensors(path);
					}
				}
			}

        }

        shared.mu.Lock();
        while (shared.num_initialized < n) {
            shared.cv.Wait();
        }

        shared.start = true;
        shared.cv.SignalAll();
        while (shared.num_done < n) {
            shared.cv.Wait();
        }
        shared.mu.Unlock();

        /*for (int i = 0; i < n; i++) { // each threads
            arg[i].thread->stats.Report(name);
        }*/

        for (int i = 1; i < n; i++) {  // total throughput
            arg[0].thread->stats.Merge(arg[i].thread->stats);
        }
        arg[0].thread->stats.Report(name);

        for (int i = 0; i < n; i++) {

            delete arg[i].thread;
            if (i == 0 && name.ToString() == "TPCxIoT") {
				int substationsNum = ((FLAGS_threads-1) / 10) + 1;
				std::cout << "substations # : " << substationsNum << std::endl;
				int a =1;
				for (a=1; a<= substationsNum; a++)
				{
					std::string pathPrefix = std::to_string(a) + "";
					std::string partPrefix = std::to_string((a-1)/2) + ":";
					std::string linePrefix = std::to_string((a-1)/4) + ":";
					//pathPrefix = partPrefix + pathPrefix;
					pathPrefix = linePrefix + partPrefix + pathPrefix;
					int k =0;
                    if (a==1)
                        db_->RemoveNode("0");
				}
            }

        }
        delete[] arg;
    }

    void Crc32c(ThreadState* thread) {
        // Checksum about 500MB of data total
        const int size = 4096;
        const char* label = "(4K per op)";
        std::string data(size, 'x');
        int64_t bytes = 0;
        uint32_t crc = 0;
        while (bytes < 500 * 1048576) {
            crc = crc32c::Value(data.data(), size);
            thread->stats.FinishedSingleOp();
            bytes += size;
        }
        // Print so result is not dead
        fprintf(stderr, "... crc=0x%x\r", static_cast<unsigned int>(crc));

        thread->stats.AddBytes(bytes);
        thread->stats.AddMessage(label);
    }

    void SnappyCompress(ThreadState* thread) {
        RandomGenerator gen;
        Slice input = gen.Generate(Options().block_size);
        int64_t bytes = 0;
        int64_t produced = 0;
        bool ok = true;
        std::string compressed;
        while (ok && bytes < 1024 * 1048576) {  // Compress 1G
            ok = port::Snappy_Compress(input.data(), input.size(), &compressed);
            produced += compressed.size();
            bytes += input.size();
            thread->stats.FinishedSingleOp();
        }

        if (!ok) {
            thread->stats.AddMessage("(snappy failure)");
        } else {
            char buf[100];
            snprintf(buf, sizeof(buf), "(output: %.1f%%)",
                     (produced * 100.0) / bytes);
            thread->stats.AddMessage(buf);
            thread->stats.AddBytes(bytes);
        }
    }

    void SnappyUncompress(ThreadState* thread) {
        RandomGenerator gen;
        Slice input = gen.Generate(Options().block_size);
        std::string compressed;
        bool ok = port::Snappy_Compress(input.data(), input.size(), &compressed);
        int64_t bytes = 0;
        char* uncompressed = new char[input.size()];
        while (ok && bytes < 1024 * 1048576) {  // Compress 1G
            ok = port::Snappy_Uncompress(compressed.data(), compressed.size(),
                                         uncompressed);
            bytes += input.size();
            thread->stats.FinishedSingleOp();
        }
        delete[] uncompressed;

        if (!ok) {
            thread->stats.AddMessage("(snappy failure)");
        } else {
            thread->stats.AddBytes(bytes);
        }
    }

    void Open() {
        assert(db_ == nullptr);
        Options options;
        options.env = g_env;
        options.create_if_missing = !FLAGS_use_existing_db;
        options.block_cache = cache_;
        options.write_buffer_size = FLAGS_write_buffer_size;
        options.max_file_size = FLAGS_max_file_size;
        options.block_size = FLAGS_block_size;
        options.max_open_files = FLAGS_open_files;
        options.filter_policy = filter_policy_;
        options.reuse_logs = FLAGS_reuse_logs;
        Status s = DB::Open(options, FLAGS_db, &db_);
        if (!s.ok()) {
            fprintf(stderr, "open error: %s\n", s.ToString().c_str());
            exit(1);
        }
    }

    void OpenBench(ThreadState* thread) {
        for (int i = 0; i < num_; i++) {
            delete db_;
            Open();
            thread->stats.FinishedSingleOp();
        }
    }

    void WriteSeq(ThreadState* thread) {
        DoWrite(thread, true);
    }

    std::string random_string(size_t length) {
        auto randchar = []() -> char {
            const char charset[] =
                "01234567890!@#$%^&*()abcdefghijklmnopqestuvwxyz";
            const size_t max_index = (sizeof(charset) - 1);
            return charset[rand() % max_index];
        };
        std::string str(length, 0);
        std::generate_n(str.begin(), length, randchar);
        return str;
    }

    void WriteTPCxIoT(ThreadState* thread) {
        if (num_ != FLAGS_num) {
            char msg[100];
            snprintf(msg, sizeof(msg), "(%d ops)", num_);
            thread->stats.AddMessage(msg);
        }

        int sensorNum = thread->sensorNum;
        std::string edgeNode = std::to_string(sensorNum);
        using namespace std::chrono;
        nanoseconds ns = duration_cast<nanoseconds>(
            system_clock::now().time_since_epoch());

        RandomGenerator gen;
        WriteBatch batch;
        WriteOptions writeOptions = WriteOptions();
        Status s;
        int64_t bytes = 0;
        //std::string temp_string = random_string(1024);
        //value_size_ = 1024;
		std::string temp_string = random_string(1024); // vs. redisEdge
        value_size_ = 1024;

		int threadNum = thread->tid + 1;

        std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

        for (int i = 0; i < num_; i += entries_per_batch_) {
            batch.Clear();
            for (int j = 0; j < entries_per_batch_; j++) {
                // int temp_num = thread->rand.Next() % FLAGS_sensors;
                int temp_num = i % FLAGS_sensors;
                const std::string sensorKey = prekeys[temp_num];
                // std::string id = edgeNode + sensorKey; //edgeNode# + sensorID

                temp_num = FLAGS_sensors * (sensorNum - 1) + temp_num;

                std::string id = std::to_string(temp_num);
				id = std::to_string(threadNum) + ":" + sensorKey;
                std::string key;
                key.append(edgeNode);  // Client Number
                key.append(sensorKey);
                ns = duration_cast<nanoseconds>(system_clock::now().time_since_epoch());
                key.append(std::to_string(ns.count()));
				//std::cout << "put index id = " + id << std::endl;
                batch.Put(key, id, temp_string);

                bytes += value_size_ + key.length();
                thread->stats.FinishedSingleOp();
                writeOptions.sensorID = id;
            }
            // thread->stats.StartSingleOp();
            s = db_->Write(writeOptions, &batch);
            if (!s.ok()) {
                fprintf(stderr, "put error: %s\n", s.ToString().c_str());
                exit(1);
            }
        }
		//std::string begin_key = client + ":" + sensorName + ":" + std::to_string(begin_time);
        //std::string end_key = client + ":" + sensorName + ":" + std::to_string(end_time);
		ReadOptions options;
		std::string id = std::to_string(threadNum) + ":" + prekeys[0];
        options.sensorID = id;
        Iterator* iter = db_->NewIteratorIoT(id, options);
		int iterator_cnt = 0;
		

        for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
                iterator_cnt++;
        }

		std::cout << "iterator cnt = " << iterator_cnt << std::endl;

        delete iter;

        thread->stats.AddBytes(bytes);
        std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
		std::cout << "Time difference = " << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() << "[ms]" << std::endl;
    }

    void WriteRandom(ThreadState* thread) {
        DoWrite(thread, false);
    }

    void DoWrite(ThreadState* thread, bool seq) {
        if (num_ != FLAGS_num) {
            char msg[100];
            snprintf(msg, sizeof(msg), "(%d ops)", num_);
            thread->stats.AddMessage(msg);
        }

        RandomGenerator gen;
        WriteBatch batch;
        Status s;
        int64_t bytes = 0;
        Slice test_value = gen.Generate(value_size_);
        for (int i = 0; i < num_; i += entries_per_batch_) {
            batch.Clear();
            for (int j = 0; j < entries_per_batch_; j++) {
                const int k = seq ? i + j : (thread->rand.Next() % FLAGS_num);
                char key[100];
                snprintf(key, sizeof(key), "%016d", k);
                batch.Put(key, test_value);
                // batch.Put(key, gen.Generate(value_size_));
                bytes += value_size_ + strlen(key);
                thread->stats.FinishedSingleOp();
            }
            s = db_->Write(write_options_, &batch);

            if (!s.ok()) {
                fprintf(stderr, "put error: %s\n", s.ToString().c_str());
                exit(1);
            }
        }
        thread->stats.AddBytes(bytes);
    }

    void doTPCxIoT(ThreadState* thread) {
        int threadID = thread->sensorNum;

        int cnt = 0;
        int target_num = num_ / FLAGS_threads;

        long start_timestamp = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        DiscreteGenerator* operation_chooser_ = new DiscreteGenerator((uint64_t)rand());
        // DiscreteGenerator *operation_chooser_ = new DiscreteGenerator( (uint64_t)time(NULL));
        UnixEpochTimestampGenerator* tt_ = new UnixEpochTimestampGenerator(1.0, 100.0, start_timestamp);  // internal, timeunits (milliseconds), current time

        double insert_p;
        double scan_p;

        scan_p = 0.00005;  // scan proportion
        // scan_p = 0;
        insert_p = 1.0 - scan_p;  // insert proportion

        operation_chooser_->add_value(DBOperation::INSERT, insert_p);
        operation_chooser_->add_value(DBOperation::SCAN, scan_p);

        DBOperation op;

        int ins_count = 0;
        int scan_count = 0;
        long ins_timestamp;

        std::string client = std::to_string((int)((thread->sensorNum - 1) / 10) + 1);
        std::string thread_num = std::to_string(thread->sensorNum);
        std::string ins_key;
        std::string ins_value;
        WriteBatch batch;

        int total_r1_size = 0;
        int total_r2_size = 0;

        int c_id = 1;  // CF #
        // DBWithColumnFamilies* db_with_cfh = SelectDBWithCfh(c_id);

        int value_size = 1024;
        int64_t bytes = 0;
        Status s;
        ins_value = random_string(1024);  // GenerateValue(1024);
        WriteOptions writeOptions = WriteOptions();

        std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
        int index_base = (int)((thread->sensorNum - 1) / 10) * FLAGS_sensors;
        while (cnt < target_num) {
            bytes = 0;
            op = static_cast<DBOperation>(operation_chooser_->next_val());
            // const std::string prekey = prekeys[thread->rand.Next() % FLAGS_sensors]; //per thread?
            int index = (((threadID - 1) % 10) * (FLAGS_sensors / 10)) + (thread->rand.Next() % (FLAGS_sensors / 10));
            const std::string prekey = prekeys[index];

            int temp_id = index_base + index;
            std::string id = std::to_string(temp_id);
			
			// 0410
			id = client + ":" + prekey;
			int cnum = (int)((thread->sensorNum - 1) / 10) + 1;
			//id = std::to_string((cnum-1)/2) + ":" + id;
			id = std::to_string((cnum-1)/4) + ":" + std::to_string((cnum-1)/2) + ":" + id;
            if (op == DBOperation::INSERT) {
                batch.Clear();
                // Building Keys ==>  (clientID : sensorName : Timestamp)
                ins_timestamp = tt_->next_val();
                ins_key = client + ":" + prekey + ":" + std::to_string(ins_timestamp);  // key
                usleep(100);                                                            // Interval
                // std::cout << "ins_key:" << ins_key << ", ID:" << id << "\n";
                batch.Put(ins_key, id, ins_value);
                bytes += value_size + ins_key.length();
                writeOptions.sensorID = id;
                s = db_->Write(writeOptions, &batch);

                if (!s.ok()) {
                    fprintf(stderr, "put error: %s\n", s.ToString().c_str());
                    exit(1);
                }

                ins_count++;
                thread->stats.FinishedSingleOp();
                thread->stats.AddBytes(bytes);

            } else {  // DBOperation::SCAN
                long current_timestamp = tt_->last_val();
                long scan_timestamp = current_timestamp - 5000;
                std::map<std::string, std::string> result1;
                std::map<std::string, std::string> result2;
                uint64_t measure_start_micros = t_env->NowMicros();

                // Q1. SCAN ([scan_timestamp, scan_timestamp+5000])  //current-5s, current
                s = scanHelper(client, prekey, id, scan_timestamp, result1);

                if (!s.ok()) {
                    std::cout << "Q1. SCAN FAILED\n";
                }
                total_r1_size += result1.size();

                long old_timestamp;
                long random_timestamp;
                if (scan_timestamp - start_timestamp > 1800000L) {
                    old_timestamp = scan_timestamp - 1800000L;
                } else {
                    old_timestamp = start_timestamp;
                }
                // Q2. randomly selected 5s
                double r = ((double)rand() / (RAND_MAX));
                random_timestamp = old_timestamp + (r * (scan_timestamp - 10000L - old_timestamp));
                s = scanHelper(client, prekey, id, random_timestamp, result2);

                if (!s.ok()) {
                    std::cout << "Q2. SCAN FAILED\n";
                }
                total_r2_size += result2.size();
                scan_count++;
                uint64_t measure_end_micros = t_env->NowMicros();
                uint64_t diff_micros = measure_end_micros - measure_start_micros;
                // std::cout << "TPCxIoT SCAN(Q1, Q2) Latency: " << std::to_string(long(diff_micros) / 1000.0) << "ms \n";
            }
            cnt++;
            // thread->stats.FinishedOps(db_with_cfh, db_with_cfh->db, 1, kWrite);
        }

        // std::cout << "CLIENT #:" << std::setw(4) << client << ", THREAD #:" << std::setw(4) << thread_num << ", INS_COUNT: " << ins_count << ", SCAN_COUNT:" << scan_count << ", R1_SIZE:" << total_r1_size << ", R2_SIZE:" << total_r2_size << ", TOTAL_COUNT:" << cnt << "\n";
        std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
        // std::cout << "Time difference = " << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() << "[ms]" << std::endl;
    }

    Status scanHelper(std::string client, const std::string sensorName, std::string id, long scan_timestamp, std::map<std::string, std::string>& result) {
        long begin_time, end_time;
        begin_time = scan_timestamp;
        int iterator_cnt = 0;
        end_time = begin_time + 5000L;
        ReadOptions options = ReadOptions();

        std::string begin_key = client + ":" + sensorName + ":" + std::to_string(begin_time);
        std::string end_key = client + ":" + sensorName + ":" + std::to_string(end_time);

        options.sensorID = id;
        Iterator* iter = db_->NewIteratorIoT(id, options);

        for (iter->Seek(begin_key); iter->Valid(); iter->Next()) {
            if (iter->key().ToString() > end_key) {
                break;
            } else if (iter->key().ToString() >= begin_key) {
                result.insert(std::pair<std::string, std::string>(iter->key().ToString(), iter->value().ToString()));
                iterator_cnt++;
            }
        }
        delete iter;
        return Status::OK();
    }
    void ScanTPCxIoT(ThreadState* thread) {
        // Iterator* iter = db_->NewIterator(ReadOptions());
        int fieldNum = thread->sensorNum;

        std::string edgeNode = std::to_string(fieldNum);

        if (fieldNum != 1)
            return;
        int i = 0;
        int64_t bytes = 0;
        // system("sudo ./drop_cache.sh");

        ReadOptions options;

        nanoseconds ns = duration_cast<nanoseconds>(
            system_clock::now().time_since_epoch());

        int temp_random_sensor = thread->rand.Next() % FLAGS_sensors;
        int temp_random_thread = thread->rand.Next() % FLAGS_threads;

        const std::string sensorKey = prekeys[temp_random_sensor];
        edgeNode = std::to_string(temp_random_thread + 1);

        std::string id = edgeNode + sensorKey;  // edgeNode# + sensorID
        std::string end_key;
        end_key.append(id);
        end_key.append(std::to_string(ns.count()));

        // int Q_num = 1;
        int Q_num = 0;
        std::cin.get();

        int temp_iter_id;
        temp_iter_id = FLAGS_sensors * (temp_random_thread) + (temp_random_sensor);

        if (Q_num == 0)  // For Seek Testing
        {
            std::string begin_key = id;
            ReadOptions options;
            options.sensorID = std::to_string(temp_iter_id);
            std::cout << std::setw(20) << "TEST SENSOR: " << id << "\n";
            Iterator* iter = db_->NewIteratorIoT(std::to_string(temp_iter_id), options);
            std::string center_key;
            // for (iter->Seek(begin_key); iter->Valid()  ; iter->Next())
            for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
                if (i == 0)
                    std::cout << std::setw(20) << "START KEY: " << iter->key().ToString() << "\n";
                else if (i == num_ - 1)
                    std::cout << std::setw(20) << "END KEY: " << iter->key().ToString() << "\n";

                bytes += iter->key().size() + iter->value().size();
                thread->stats.FinishedSingleOp();

                if (i == num_ / 2) {
                    center_key = iter->key().ToString();
                    std::cout << std::setw(20) << "CENTER KEY: " << iter->key().ToString() << "\n";
                }
                ++i;
                if (num_ <= 20)
                    std::cout << std::setw(20) << i << " KEY:" << iter->key().ToString() << "\n";
            }
            i = 0;
            iter = db_->NewIteratorIoT(std::to_string(temp_iter_id), options);
            for (iter->Seek(center_key); iter->Valid(); iter->Next()) {
                if (i == 0)
                    std::cout << std::setw(20) << "SEEK START KEY: " << iter->key().ToString() << "\n";
                ++i;
                if (num_ <= 20)
                    std::cout << std::setw(20) << i << "REPEAT KEY:" << iter->key().ToString() << "\n";
            }
            std::cout << std::setw(20) << "CENTER TO LAST KEYS: " << i << "\n";

            delete iter;
        } else if (Q_num == 1) {
            std::string begin_key = id;
            ReadOptions options;
            options.sensorID = std::to_string(temp_iter_id);
            std::cout << "Q1. ONE SPECIFIC SENSOR: " << id << "\n";
            Iterator* iter = db_->NewIteratorIoT(std::to_string(temp_iter_id), options);
            // for (iter->Seek(begin_key); iter->Valid()  ; iter->Next())
            for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
                if (iter->key().ToString() > end_key) {
                    // std::cout << iter->key().ToString() << "\n";
                    break;
                } else {
                    bytes += iter->key().size() + iter->value().size();
                    thread->stats.FinishedSingleOp();
                    if (i < 10)
                        std::cout << iter->key().ToString() << "\n";
                    auto temp = iter->value();
                    ++i;
                }
            }
            delete iter;
        }
        thread->stats.AddBytes(bytes);
    }

    void ReadSequential(ThreadState* thread) {
        Iterator* iter = db_->NewIterator(ReadOptions());
        int i = 0;
        int64_t bytes = 0;
        for (iter->SeekToFirst(); i < reads_ && iter->Valid(); iter->Next()) {
            bytes += iter->key().size() + iter->value().size();
            thread->stats.FinishedSingleOp();
            ++i;
        }
        printf("total read cnt is %d\n", i);
        delete iter;
        thread->stats.AddBytes(bytes);
    }

    void ReadReverse(ThreadState* thread) {
        Iterator* iter = db_->NewIterator(ReadOptions());
        int i = 0;
        int64_t bytes = 0;
        for (iter->SeekToLast(); i < reads_ && iter->Valid(); iter->Prev()) {
            bytes += iter->key().size() + iter->value().size();
            thread->stats.FinishedSingleOp();
            ++i;
        }
        delete iter;
        thread->stats.AddBytes(bytes);
    }

    void ReadRandom(ThreadState* thread) {
        ReadOptions options;
        std::string value;
        int found = 0;
        for (int i = 0; i < reads_; i++) {
            char key[100];
            const int k = thread->rand.Next() % FLAGS_num;
            snprintf(key, sizeof(key), "%016d", k);
            if (db_->Get(options, key, &value).ok()) {
                found++;
            } else {
                printf("key %d not found\n", k);
            }
            thread->stats.FinishedSingleOp();
        }
        char msg[100];
        snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
        thread->stats.AddMessage(msg);
    }

    void ReadMissing(ThreadState* thread) {
        ReadOptions options;
        std::string value;
        for (int i = 0; i < reads_; i++) {
            char key[100];
            const int k = thread->rand.Next() % FLAGS_num;
            snprintf(key, sizeof(key), "%016d.", k);
            db_->Get(options, key, &value);
            thread->stats.FinishedSingleOp();
        }
    }

    void ReadHot(ThreadState* thread) {
        ReadOptions options;
        std::string value;
        const int range = (FLAGS_num + 99) / 100;
        for (int i = 0; i < reads_; i++) {
            char key[100];
            const int k = thread->rand.Next() % range;
            snprintf(key, sizeof(key), "%016d", k);
            db_->Get(options, key, &value);
            thread->stats.FinishedSingleOp();
        }
    }

    void SeekRandom(ThreadState* thread) {
        ReadOptions options;
        int found = 0;
        for (int i = 0; i < reads_; i++) {
            Iterator* iter = db_->NewIterator(options);
            char key[100];
            const int k = thread->rand.Next() % FLAGS_num;
            snprintf(key, sizeof(key), "%016d", k);
            iter->Seek(key);
            if (iter->Valid() && iter->key() == key) found++;
            delete iter;
            thread->stats.FinishedSingleOp();
        }
        char msg[100];
        snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
        thread->stats.AddMessage(msg);
    }

    void DoDelete(ThreadState* thread, bool seq) {
        RandomGenerator gen;
        WriteBatch batch;
        Status s;
        for (int i = 0; i < num_; i += entries_per_batch_) {
            batch.Clear();
            for (int j = 0; j < entries_per_batch_; j++) {
                const int k = seq ? i + j : (thread->rand.Next() % FLAGS_num);
                char key[100];
                snprintf(key, sizeof(key), "%016d", k);
                batch.Delete(key);
                thread->stats.FinishedSingleOp();
            }
            s = db_->Write(write_options_, &batch);
            if (!s.ok()) {
                fprintf(stderr, "del error: %s\n", s.ToString().c_str());
                exit(1);
            }
        }
    }

    void DeleteSeq(ThreadState* thread) {
        DoDelete(thread, true);
    }

    void DeleteRandom(ThreadState* thread) {
        DoDelete(thread, false);
    }

    void ReadWhileWriting(ThreadState* thread) {
        if (thread->tid > 0) {
            ReadRandom(thread);
        } else {
            // Special thread that keeps writing until other threads are done.
            RandomGenerator gen;
            while (true) {
                {
                    MutexLock l(&thread->shared->mu);
                    if (thread->shared->num_done + 1 >= thread->shared->num_initialized) {
                        // Other threads have finished
                        break;
                    }
                }

                const int k = thread->rand.Next() % FLAGS_num;
                char key[100];
                snprintf(key, sizeof(key), "%016d", k);
                Status s = db_->Put(write_options_, key, gen.Generate(value_size_));
                if (!s.ok()) {
                    fprintf(stderr, "put error: %s\n", s.ToString().c_str());
                    exit(1);
                }
            }

            // Do not count any of the preceding work/delay in stats.
            thread->stats.Start();
        }
    }

    void Compact(ThreadState* thread) {
        db_->CompactRange(nullptr, nullptr);
    }

    void PrintStats(const char* key) {
        std::string stats;
        if (!db_->GetProperty(key, &stats)) {
            stats = "(failed)";
        }
        fprintf(stdout, "\n%s\n", stats.c_str());
    }

    static void WriteToFile(void* arg, const char* buf, int n) {
        reinterpret_cast<WritableFile*>(arg)->Append(Slice(buf, n));
    }

    void HeapProfile() {
        char fname[100];
        snprintf(fname, sizeof(fname), "%s/heap-%04d", FLAGS_db, ++heap_counter_);
        WritableFile* file;
        Status s = g_env->NewWritableFile(fname, &file);
        if (!s.ok()) {
            fprintf(stderr, "%s\n", s.ToString().c_str());
            return;
        }
        bool ok = port::GetHeapProfile(WriteToFile, file);
        delete file;
        if (!ok) {
            fprintf(stderr, "heap profiling not supported\n");
            g_env->DeleteFile(fname);
        }
    }
};

}  // namespace leveldb

int main(int argc, char** argv) {
    FLAGS_write_buffer_size = leveldb::Options().write_buffer_size;
    FLAGS_max_file_size = leveldb::Options().max_file_size;
    FLAGS_block_size = leveldb::Options().block_size;
    FLAGS_open_files = leveldb::Options().max_open_files;
    std::string default_db_path;

    for (int i = 1; i < argc; i++) {
        double d;
        int n;
        char junk;
        if (leveldb::Slice(argv[i]).starts_with("--benchmarks=")) {
            FLAGS_benchmarks = argv[i] + strlen("--benchmarks=");
        } else if (sscanf(argv[i], "--compression_ratio=%lf%c", &d, &junk) == 1) {
            FLAGS_compression_ratio = d;
        } else if (sscanf(argv[i], "--histogram=%d%c", &n, &junk) == 1 &&
                   (n == 0 || n == 1)) {
            FLAGS_histogram = n;
        } else if (sscanf(argv[i], "--use_existing_db=%d%c", &n, &junk) == 1 &&
                   (n == 0 || n == 1)) {
            FLAGS_use_existing_db = n;
        } else if (sscanf(argv[i], "--reuse_logs=%d%c", &n, &junk) == 1 &&
                   (n == 0 || n == 1)) {
            FLAGS_reuse_logs = n;
        } else if (sscanf(argv[i], "--num=%d%c", &n, &junk) == 1) {
            FLAGS_num = n;
        } else if (sscanf(argv[i], "--sensors=%d%c", &n, &junk) == 1) {
            FLAGS_sensors = n;
        } else if (sscanf(argv[i], "--reads=%d%c", &n, &junk) == 1) {
            FLAGS_reads = n;
        } else if (sscanf(argv[i], "--threads=%d%c", &n, &junk) == 1) {
            FLAGS_threads = n;
        } else if (sscanf(argv[i], "--value_size=%d%c", &n, &junk) == 1) {
            FLAGS_value_size = n;
        } else if (sscanf(argv[i], "--write_buffer_size=%d%c", &n, &junk) == 1) {
            FLAGS_write_buffer_size = n;
        } else if (sscanf(argv[i], "--max_file_size=%d%c", &n, &junk) == 1) {
            FLAGS_max_file_size = n;
        } else if (sscanf(argv[i], "--block_size=%d%c", &n, &junk) == 1) {
            FLAGS_block_size = n;
        } else if (sscanf(argv[i], "--cache_size=%d%c", &n, &junk) == 1) {
            FLAGS_cache_size = n;
        } else if (sscanf(argv[i], "--bloom_bits=%d%c", &n, &junk) == 1) {
            FLAGS_bloom_bits = n;
        } else if (sscanf(argv[i], "--open_files=%d%c", &n, &junk) == 1) {
            FLAGS_open_files = n;
        } else if (strncmp(argv[i], "--db=", 5) == 0) {
            FLAGS_db = argv[i] + 5;
        } else {
            fprintf(stderr, "Invalid flag '%s'\n", argv[i]);
            exit(1);
        }
    }

    leveldb::g_env = leveldb::Env::Default();
    leveldb::t_env = leveldb::Env::Default();

    // Choose a location for the test database if none given with --db=<path>
    if (FLAGS_db == nullptr) {
        leveldb::g_env->GetTestDirectory(&default_db_path);
        default_db_path += "/dbbench";
        FLAGS_db = default_db_path.c_str();
    }

    leveldb::Benchmark benchmark;
    benchmark.Run();
    return 0;
}
