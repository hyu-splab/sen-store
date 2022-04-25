// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DB_IMPL_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_H_

#include <atomic>
#include <unistd.h>
#include <deque>
#include <iostream>
#include <map>
#include <set>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "../util/mutexlock.h"
#include "db/IoI.h"
#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "filename.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/write_batch.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "version_edit.h"

using namespace std;
namespace leveldb {

class MemTable;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;

class DBImpl : public DB {
   public:
    DBImpl(const Options& options, const std::string& dbname);
    virtual ~DBImpl();

    // Implementations of the DB interface
    virtual Status Put(const WriteOptions&, const Slice& key, const Slice& value);
    virtual Status Delete(const WriteOptions&, const Slice& key);
    virtual Status RegisterSensors(int sensorCount);
	virtual Status RegisterSensors(std::string name);
    virtual Status RemoveNode(std::string nodePath);
    virtual Status addNode(std::string nodePath, std::string filePath);
    virtual Status Write(const WriteOptions& options, WriteBatch* updates);
    virtual Status Get(const ReadOptions& options,
                       const Slice& key,
                       std::string* value);
    virtual Iterator* NewIterator(const ReadOptions&);
    virtual Iterator* NewIteratorIoT(Slice sensorID, const ReadOptions&);
    // virtual Iterator* NewIteratorIoT(Slice sensorID, ReadOptions& options);
    virtual const Snapshot* GetSnapshot();
    virtual void ReleaseSnapshot(const Snapshot* snapshot);
    virtual bool GetProperty(const Slice& property, std::string* value);
    virtual void GetApproximateSizes(const Range* range, int n, uint64_t* sizes);
    Status WriteLevel0TableWithTable(TableBuilder* dbtable_, VersionEdit* edit, Version* base);
    virtual void CompactRange(const Slice* begin, const Slice* end);

    // Extra methods (for testing) that are not in the public DB interface

    // Compact any files in the named level that overlap [*begin,*end]
    void TEST_CompactRange(int level, const Slice* begin, const Slice* end);

    // Force current memtable contents to be compacted.
    Status TEST_CompactMemTable();

    // Return an internal iterator over the current state of the database.
    // The keys of this iterator are internal keys (see format.h).
    // The returned iterator should be deleted when no longer needed.
    Iterator* TEST_NewInternalIterator();

    // Return the maximum overlapping data (in bytes) at next level for any
    // file at a level >= 1.
    int64_t TEST_MaxNextLevelOverlappingBytes();

    // Record a sample of bytes read at the specified internal key.
    // Samples are taken approximately once every config::kReadBytesPeriod
    // bytes.
    void RecordReadSample(Slice key);

   private:
    friend class DB;
    struct CompactionState;
    struct Writer;

    Iterator* NewInternalIterator(const ReadOptions&,
                                  SequenceNumber* latest_snapshot,
                                  uint32_t* seed);

    Iterator* NewInternalIterator(Slice sensorID, const ReadOptions& options, SequenceNumber* latest_snapshot, uint32_t* seed);

    Status NewDB();

    // Recover the descriptor from persistent storage.  May do a significant
    // amount of work to recover recently logged updates.  Any changes to
    // be made to the descriptor are added to *edit.
    Status Recover(VersionEdit* edit, bool* save_manifest)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    void MaybeIgnoreError(Status* s) const;

    // Delete any unneeded files and stale in-memory entries.
    void DeleteObsoleteFiles() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    // Compact the in-memory write buffer to disk.  Switches to a new
    // log-file/memtable and writes a new descriptor iff successful.
    // Errors are recorded in bg_error_.
    void CompactMemTable() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    Status RecoverLogFile(uint64_t log_number, bool last_log, bool* save_manifest,
                          VersionEdit* edit, SequenceNumber* max_sequence)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);
    Status WriteTable(const WriteBatch* b, VersionEdit* edit, Version* base)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    Status MakeRoomForWrite(bool force /* compact even if there is room? */)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);
    WriteBatch* BuildBatchGroup(Writer** last_writer)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    void RecordBackgroundError(const Status& s);

    void MaybeScheduleCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
    static void BGWork(void* db);
    void BackgroundCall();
    void BackgroundCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
    void CleanupCompaction(CompactionState* compact)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);
    Status DoCompactionWork(CompactionState* compact)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    Status OpenCompactionOutputFile(CompactionState* compact);
    Status FinishCompactionOutputFile(CompactionState* compact, Iterator* input);
    Status InstallCompactionResults(CompactionState* compact)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    // Constant after construction
    Env* const env_;
    const InternalKeyComparator internal_comparator_;
    const InternalFilterPolicy internal_filter_policy_;
    const Options options_;  // options_.comparator == &internal_comparator_
    const bool owns_info_log_;
    const bool owns_cache_;
    const std::string dbname_;

    // table_cache_ provides its own synchronization
    TableCache* const table_cache_;

    // Lock over the persistent DB state.  Non-null iff successfully acquired.
    FileLock* db_lock_;

    // State below is protected by mutex_
    port::Mutex mutex_;
    unordered_map<string, port::Mutex*> mutexContainer;
    std::atomic<bool> shutting_down_;
    port::CondVar background_work_finished_signal_ GUARDED_BY(mutex_);
    TableBuilder* dbtable_;

    enum SaverState {
        kNotFound,
        kFound,
        kDeleted,
        kCorrupt,
    };
    struct Saver {
        SaverState state;
        const Comparator* ucmp;
        Slice user_key;
        std::string* value;
    };
    static void SaveValue(void* arg, const Slice& ikey, const Slice& v) {
        Saver* s = reinterpret_cast<Saver*>(arg);
        ParsedInternalKey parsed_key;
        if (!ParseInternalKey(ikey, &parsed_key)) {
            s->state = kCorrupt;
        } else {
            if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {
                s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
                if (s->state == kFound) {
                    s->value->assign(v.data(), v.size());
                }
            }
        }
    }

    static Iterator* GetFileIterator(void* arg, const ReadOptions& options, const Slice& file_value) {
        TableCache* cache = reinterpret_cast<TableCache*>(arg);
        if (file_value.size() != 16) {
            return NewErrorIterator(
                Status::Corruption("FileReader invoked with unexpected value"));
        } else {
            return cache->NewIterator(options, DecodeFixed64(file_value.data()), DecodeFixed64(file_value.data() + 8));
        }
    }

    class Handler : public WriteBatch::Handler {
       private:
        port::Mutex l_mutex;

		//IoI* sensor_index;

        std::vector<VersionSet*> vsh;
        std::vector<TableBuilder*> tbh;
        std::vector<FileMetaData*> mth;

        bool cond = false;
        typedef std::shared_mutex Lock;
        typedef std::unique_lock<Lock> WriteLock;
        typedef std::shared_lock<Lock> ReadLock;
        Lock myLock;
		IoI* senstore_index;

       public:
        Handler(){
			initSenstoreIndex("root");
		};
        ~Handler(){};
        Handler(Handler& h) {
        }
        // port::Mutex a_mutex;
        TableBuilder* dbtable_;
        SequenceNumber sequence_ = 0;
        Options options;
        FileMetaData meta_;
        const InternalKeyComparator* icmp_;
        std::string dbname_;
        VersionSet* currentVersionSet;

        // unordered_map<string, TableBuilder*> sensorContainer;
        // unordered_map<string, DBImpl*> sensorContainer;

        // port::Mutex handler_mutex_;
		void initSenstoreIndex(std::string name) {
			senstore_index = new IoI(name);
		}
        
        IoI* getSenstoreIndex() {
            return senstore_index;
        }

		void insertSenstoreIndex(std::string name) {
			options.comparator = icmp_;
			TableCache* tempCache = new TableCache(dbname_, options, options.max_open_files - 10);
			FileMetaData* tempMetaData = new FileMetaData();
			const Options* tempOption;
			tempOption = &options;
			VersionSet* vsTemp = new VersionSet(dbname_, tempOption, tempCache, icmp_);
			vsTemp->SetLastSequence(0);

			WritableFile* new_lfile;
			FileMetaData meta;
			tempMetaData->number = vsTemp->NewFileNumber();
			tempMetaData->file_size = 0;

			std::string sensorID = name;
			std::string fname = TableFileName(dbname_, sensorID, tempMetaData->number);
			vsTemp->sensorID = sensorID;

			if (options.env->NewWritableFile(fname, &new_lfile).ok()) {
				TableBuilder* tempDbtable = new TableBuilder(options, new_lfile);

				//tbh[i] = tempDbtable;
				//tbh[i]->sequence = 0;
				tempDbtable->sequence = 0;
				sequence_ = tempDbtable->sequence;
				senstore_index->addIndex(name, vsTemp, tempDbtable, tempMetaData); //add index
			} else {
				printf("Cannot create dbtable");
			}

		}

        void createSensorHandler(int sensorCount) {
            vsh.resize(sensorCount);
            tbh.resize(sensorCount);
            mth.resize(sensorCount);
            // create

            options.comparator = icmp_;

            for (int i = 0; i < sensorCount; i++) {
                TableCache* tempCache = new TableCache(dbname_, options, options.max_open_files - 10);
                FileMetaData* tempMetaData = new FileMetaData();
                const Options* tempOption;
                tempOption = &options;
                VersionSet* vsTemp = new VersionSet(dbname_, tempOption, tempCache, icmp_);
                vsTemp->SetLastSequence(0);

                WritableFile* new_lfile;
                FileMetaData meta;
                tempMetaData->number = vsTemp->NewFileNumber();
                tempMetaData->file_size = 0;

                std::string sensorID = std::to_string(i);
                std::string fname = TableFileName(dbname_, sensorID, tempMetaData->number);
                vsTemp->sensorID = sensorID;

                if (options.env->NewWritableFile(fname, &new_lfile).ok()) {
                    TableBuilder* tempDbtable = new TableBuilder(options, new_lfile);

                    tbh[i] = tempDbtable;
                    tbh[i]->sequence = 0;
                    sequence_ = tbh[i]->sequence;
                } else {
                    printf("Cannot create dbtable");
                }

                vsh[i] = vsTemp;
                mth[i] = tempMetaData;
            }
        }

        void AddIterators(const ReadOptions& options, std::vector<Iterator*>* iters) {
            Status* s;
            iters->push_back(dbtable_->NewBlockIterator(options));
            iters->push_back(dbtable_->NewIterator(options));
        }
        VersionSet* getVersionSet(Slice sensorID) {
            //int id = atoi(sensorID.ToString().c_str());
			//std::cout << "getVersionSet ID : " << sensorID.ToString();
			std::string path = sensorID.ToString();
			VersionSet* tempVsh;
			senstore_index->getVSH(path, &tempVsh);
			return tempVsh;
            //return vsh[id];
        }
        void AddIterators(Slice sensorID, const ReadOptions& options, std::vector<Iterator*>* iters) {
            Status* s;

            TableBuilder* l_dbtable;
            auto sensorIDstr = sensorID.ToString();
            //int id = atoi(sensorID.ToString().c_str());
            //VersionSet* vsTemp = vsh[id];
			VersionSet* vsTemp;
			TableBuilder* tbTemp;
			FileMetaData* mtTemp;

			senstore_index->getSDS(sensorIDstr, &vsTemp, &tbTemp, &mtTemp);
            //l_dbtable = tbh[id];
			l_dbtable = tbTemp;
            
            // Iterator* temp_iter = l_dbtable->NewIterator(options);         
            
            iters->push_back(l_dbtable->NewIterator(options));
        }

        bool Get(const ReadOptions& options, const LookupKey& key, std::string* value, Status* s) {
            Slice ikey = key.internal_key();
            Slice user_key = key.user_key();

            const Comparator* ucmp = icmp_->user_comparator();

            Saver saver;
            saver.state = kNotFound;
            saver.ucmp = ucmp;
            saver.user_key = user_key;
            saver.value = value;

            Slice a = meta_.smallest.user_key();
            Slice b = meta_.largest.user_key();
            bool c_1 = ucmp->Compare(user_key, a) >= 0;
            bool c_2 = ucmp->Compare(user_key, b) <= 0;

            if (c_1 && c_2) {
                *s = dbtable_->InternalGet(options, ikey, &saver, SaveValue);

                if (!s->ok()) {
                    return false;
                }
                switch (saver.state) {
                    case kNotFound:
                        return false;
                    case kFound:
                        return true;
                    case kDeleted:
                        return false;
                    case kCorrupt:
                        return false;
                }
            }

            return false;
        }

        virtual void Put(const Slice& key, const Slice& value) {
            if (sequence_ == 0) {
                meta_.file_size = 0;
                char* buf;
                size_t key_size = key.size();
                size_t val_size = value.size();
                Slice internal_key;
                size_t internal_key_size = key_size + 8;
                const size_t encoded_len =
                    VarintLength(internal_key_size) + internal_key_size;  // +
                VarintLength(val_size) + val_size;

                buf = new char[encoded_len];
                Slice internalKey = dbtable_->InternalKey(sequence_, kTypeValue, key, value, buf);
                meta_.smallest.DecodeFrom(internalKey);
                delete[] buf;
            }
            dbtable_->AddNew(sequence_, kTypeValue, key, value);
            // if (dbtable_->FileSize() >= leveldb::Options().max_file_size)
            {
                char* buf;
                size_t key_size = key.size();
                size_t val_size = value.size();
                Slice internal_key;
                size_t internal_key_size = key_size + 8;
                const size_t encoded_len =
                    VarintLength(internal_key_size) + internal_key_size;  // +
                VarintLength(val_size) + val_size;

                buf = new char[encoded_len];
                Slice internalKey = dbtable_->InternalKey(sequence_, kTypeValue, key, value, buf);
                meta_.largest.DecodeFrom(internalKey);
                delete[] buf;
            }
            sequence_++;
        }
        virtual void Put(const Slice& key, const Slice& sensorID, const Slice& value) {
            TableBuilder* l_dbtable;   // local dbtable_
            VersionSet* l_versionset;  // local versionset
            FileMetaData* l_meta;      // local metadata
            // port::Mutex l_mutex;

            /*int sensorNum = atoi(sensorID.ToString().c_str());
            string l_sensorID = std::to_string(sensorNum);

            l_dbtable = tbh[sensorNum];
            l_versionset = vsh[sensorNum];
            l_meta = mth[sensorNum];
            sequence_ = l_dbtable->sequence;*/

			std::string path = sensorID.ToString();
			std::string l_sensorID = path;
			senstore_index->getSDS(path, &l_versionset, &l_dbtable, &l_meta);
			
			sequence_ = l_dbtable->sequence;


            if (l_dbtable->sequence == 0)  // have to change sequence_ to dbtable' sequence_
            {
                l_meta->file_size = 0;
                char* buf;
                size_t key_size = key.size();
                size_t sensorID_size = sensorID.size();
                size_t val_size = value.size();
                Slice internal_key;
                size_t internal_key_size = key_size + 8;
                const size_t encoded_len =
                    VarintLength(internal_key_size) + internal_key_size;  // +
                VarintLength(val_size) + val_size;

                buf = new char[encoded_len];
                Slice internalKey = l_dbtable->InternalKey(l_dbtable->sequence, kTypeValue, key, value, buf);
                l_meta->smallest.DecodeFrom(internalKey);
                delete[] buf;
            }

            l_dbtable->AddNew(l_dbtable->sequence, kTypeValue, key, l_sensorID, value);
            char* buf;
            size_t key_size = key.size();
            size_t val_size = value.size();
            Slice internal_key;
            size_t internal_key_size = key_size + 8;
            const size_t encoded_len =
                VarintLength(internal_key_size) + internal_key_size;  // +
            VarintLength(val_size) + val_size;

            buf = new char[encoded_len];
            Slice internalKey = l_dbtable->InternalKey(l_dbtable->sequence, kTypeValue, key, value, buf);
            l_meta->largest.DecodeFrom(internalKey);
            delete[] buf;

            l_dbtable->sequence++;
            l_versionset->SetLastSequence(l_versionset->LastSequence() + 1);

            if (l_dbtable->FileSize() >= options.max_file_size * 2) {
                WritableFile* new_lfile;
                FileMetaData meta;
                meta.file_size = l_dbtable->FileSize();
                meta.smallest = l_meta->smallest;
                meta.largest = l_meta->largest;
                meta.number = l_versionset->NewFileNumber();

                Status status = l_dbtable->Finish();
                sequence_ = 0;
                l_dbtable->sequence = 0;

                int level = 0;
                if (status.ok() && meta.file_size > 0) {
                    const Slice min_user_key = meta.smallest.user_key();
                    const Slice max_user_key = meta.largest.user_key();

                    VersionEdit edit;
                    meta.file_size = l_dbtable->FileSize();
                    edit.AddFile(level, meta.number - 1, meta.file_size, meta.smallest, meta.largest);
                    // cout <<"sensor#" <<sensorID.ToString() << "level:" << level << "fnum:" << meta.number - 1 << "file_size:" << meta.file_size << "smallest:" << meta.smallest.user_key().ToString() << "largest:" << meta.largest.user_key().ToString() << "\n";
                    edit.SetPrevLogNumber(0);
                    port::Mutex mu;
                    // MutexLock l(&mu);
                    edit.SetLogNumber(1);  // Earlier logs no longer needed
                    // l_mutex.Lock();
                    status = l_versionset->LogAndApplyBySensor(&edit, l_sensorID, &mu);
                    // l_mutex.Unlock();
                }

                l_meta->file_size = 0;
                status = l_dbtable->CloseFile();
                if (!status.ok()) {
                    // std::cout << status.ToString();
                    std::cout << "status no ok closefile\n";
                    exit(0);
                }

                std::string fname = TableFileName(dbname_, l_sensorID, meta.number);
                // std::cout << "fname = " << fname << "\n";
                if (options.env->NewWritableFile(fname, &new_lfile).ok()) {
                    TableBuilder* tempDbtable = new TableBuilder(options, new_lfile);
                    //tbh[sensorNum] = tempDbtable;
					senstore_index->setTBH(path, tempDbtable);
					l_dbtable = tempDbtable;
                    //l_dbtable = tempDbtable;
                    l_dbtable->sequence = 0;
                    sequence_ = dbtable_->sequence;
                } else {
                    printf("error");
                }
            }
        }

        virtual void Delete(const Slice& key) {
            dbtable_->AddNew(sequence_, kTypeDeletion, key, Slice());
            sequence_++;
        }
    };

    uint64_t dbtable_number_ GUARDED_BY(mutex_);
    MemTable* mem_;
    Handler handler;
    MemTable* imm_ GUARDED_BY(mutex_);  // Memtable being compacted
    std::atomic<bool> has_imm_;         // So bg thread can detect non-null imm_
    WritableFile* logfile_;
    uint64_t logfile_number_ GUARDED_BY(mutex_);
    log::Writer* log_;
    uint32_t seed_ GUARDED_BY(mutex_);  // For sampling.

    // Queue of writers.
    std::deque<Writer*> writers_ GUARDED_BY(mutex_);
    WriteBatch* tmp_batch_ GUARDED_BY(mutex_);

    SnapshotList snapshots_ GUARDED_BY(mutex_);

    // Set of table files to protect from deletion because they are
    // part of ongoing compactions.
    std::set<uint64_t> pending_outputs_ GUARDED_BY(mutex_);

    // Has a background compaction been scheduled or is running?
    bool background_compaction_scheduled_ GUARDED_BY(mutex_);

    // Information for a manual compaction
    struct ManualCompaction {
        int level;
        bool done;
        const InternalKey* begin;  // null means beginning of key range
        const InternalKey* end;    // null means end of key range
        InternalKey tmp_storage;   // Used to keep track of compaction progress
    };
    ManualCompaction* manual_compaction_ GUARDED_BY(mutex_);

    VersionSet* const versions_;

    // Have we encountered a background error in paranoid mode?
    Status bg_error_ GUARDED_BY(mutex_);

    // Per level compaction stats.  stats_[level] stores the stats for
    // compactions that produced data for the specified "level".
    struct CompactionStats {
        int64_t micros;
        int64_t bytes_read;
        int64_t bytes_written;

        CompactionStats() : micros(0), bytes_read(0), bytes_written(0) {}

        void Add(const CompactionStats& c) {
            this->micros += c.micros;
            this->bytes_read += c.bytes_read;
            this->bytes_written += c.bytes_written;
        }
    };
    CompactionStats stats_[config::kNumLevels] GUARDED_BY(mutex_);

    // No copying allowed
    DBImpl(const DBImpl&);
    void operator=(const DBImpl&);

    const Comparator* user_comparator() const {
        return internal_comparator_.user_comparator();
    }
};

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
Options SanitizeOptions(const std::string& db,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DB_IMPL_H_
