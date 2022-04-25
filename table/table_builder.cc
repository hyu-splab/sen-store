// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <assert.h>
#include <unistd.h>
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/block.h"
#include "../include/leveldb/table.h"
#include "two_level_iterator.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "../db/dbformat.h"
#include <iostream>

namespace leveldb {

	struct TableBuilder::Rep {
		Options options;
		Options index_block_options;
		WritableFile* file;
		uint64_t offset;
		Status status;
		BlockBuilder data_block;
		BlockBuilder index_block;
		Block* index_block_for_read;
		std::string last_key;
		int64_t num_entries;
		bool closed;          // Either Finish() or Abandon() has been called.
		FilterBlockBuilder* filter_block;

		// We do not emit the index entry for a block until we have seen the
		// first key for the next data block.  This allows us to use shorter
		// keys in the index block.  For example, consider a block boundary
		// between the keys "the quick brown fox" and "the who".  We can use
		// "the r" as the key for the index block entry since it is >= all
		// entries in the first block and < all entries in subsequent
		// blocks.
		//
		// Invariant: r->pending_index_entry is true only if data_block is empty.
		bool pending_index_entry;
		BlockHandle pending_handle;  // Handle to add to index block

		std::string compressed_output;

		Rep(const Options& opt, WritableFile* f)
			: options(opt),
			index_block_options(opt),
			file(f),
			offset(0),
			data_block(&options),
			index_block(&index_block_options),
			num_entries(0),
			closed(false),
			filter_block(opt.filter_policy == nullptr ? nullptr
				: new FilterBlockBuilder(opt.filter_policy)),
			pending_index_entry(false) {
			index_block_options.block_restart_interval = 1;
		}
	};

	TableBuilder::TableBuilder(const Options& options, WritableFile* file)
		: rep_(new Rep(options, file)) {
		if (rep_->filter_block != nullptr) {
			rep_->filter_block->StartBlock(0);
		}
	}

	TableBuilder::~TableBuilder() {
		assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
		delete rep_->filter_block;
		delete rep_;
	}

	Status TableBuilder::ChangeOptions(const Options& options) {
		// Note: if more fields are added to Options, update
		// this function to catch changes that should not be allowed to
		// change in the middle of building a Table.
		if (options.comparator != rep_->options.comparator) {
			return Status::InvalidArgument("changing comparator while building table");
		}

		// Note that any live BlockBuilders point to rep_->options and therefore
		// will automatically pick up the updated options.
		rep_->options = options;
		rep_->index_block_options = options;
		rep_->index_block_options.block_restart_interval = 1;
		return Status::OK();
	}



	void TableBuilder::Add(const Slice& key, const Slice& value) {
		Rep* r = rep_;
		assert(!r->closed);
		if (!ok()) return;

		if (r->num_entries > 0) {
			assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
		}


		if (r->pending_index_entry) {
			assert(r->data_block.empty());
			r->options.comparator->FindShortestSeparator(&r->last_key, key);
			std::string handle_encoding;
			r->pending_handle.EncodeTo(&handle_encoding);
			r->index_block.Add(r->last_key, Slice(handle_encoding));
			r->pending_index_entry = false;
		}

		if (r->filter_block != nullptr) {
			r->filter_block->AddKey(key);
		}

		r->last_key.assign(key.data(), key.size());
		r->num_entries++;
		r->data_block.Add(key, value);

		const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
	
		if (estimated_block_size >= r->options.block_size) {
			Flush();
		}
	}

Slice TableBuilder::InternalKey(SequenceNumber s, ValueType type, const Slice& key, const Slice& value, char* buf){
	size_t key_size = key.size();
	size_t val_size = value.size();
	Slice internal_key;
	size_t internal_key_size = key_size + 8;
	const size_t encoded_len =
		VarintLength(internal_key_size) + internal_key_size;// +
	VarintLength(val_size) + val_size;

	//buf = new char[encoded_len];
	char* p = EncodeVarint32(buf, internal_key_size);
	memcpy(p, key.data(), key_size);
	p += key_size;
	EncodeFixed64(p, (s << 8) | type);
	p += 8;
	//	p = EncodeVarint32(p, val_size);
	//	memcpy(p, value.data(), val_size);
	//	assert(p + val_size == buf + encoded_len);

	uint32_t key_length;
	const char* key_ptr = GetVarint32Ptr(buf, buf + 5, &key_length);

	//internal_key = Slice(buf, internal_key_size);
	//internal_key = Slice(key_ptr, key_length - 8);
	internal_key = Slice(key_ptr, key_length);
	//free(buf);
	//delete[] buf;
	return internal_key;
}

Iterator* TableBuilder::BlockReader(void* arg, const ReadOptions& options, const Slice& index_value) {

	BlockHandle handle;
	Status s;
	BlockContents index_contents, data_contents;
	Block* index_block = nullptr;
	Block* data_block = nullptr;
	index_contents.data = Slice();
	index_contents.cachable = false;
	index_contents.heap_allocated = false;

	data_contents.data = Slice();
	data_contents.cachable = false;
	data_contents.heap_allocated = false;

	Slice input = index_value;
	Rep* rep_ = reinterpret_cast<Rep*>(arg);

	if (handle.DecodeFrom(&input).ok())
	{
		size_t n = static_cast<size_t>(handle.size());
		char* buf = new char[n + kBlockTrailerSize];
		Slice contents;
		//RandomAccessFile* file = nullptr;

		s = rep_->file->Read(handle.offset(), n + kBlockTrailerSize, &contents, buf);
		const char* data = contents.data();    // Pointer to where Read put the data
		if (options.verify_checksums) {
			const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
			const uint32_t actual = crc32c::Value(data, n + 1);
			if (actual != crc) {
				delete[] buf;
				s = Status::Corruption("block checksum mismatch");
				return NewErrorIterator(s);
			}
		}
		data_contents.data = Slice(data, n);
		data_block = new Block(data_contents);

		Iterator* block_iter = data_block->NewIterator(rep_->options.comparator);

		return block_iter;

	}
}

Iterator* TableBuilder::NewBlockIterator(const ReadOptions& options) const {
	
	BlockContents data_contents;
	Block* data_block = nullptr;

	//data_contents.data = rep_->data_block.FinishWithoutFlag();
	data_contents.data = rep_->data_block.MakeReadableBlock();
	data_block = new Block(data_contents);
	Iterator* block_iter = data_block->NewIterator(rep_->options.comparator);
	
	return block_iter;
}

Iterator* TableBuilder::NewIterator(const ReadOptions& options) const {

	Status s;
	BlockContents index_contents, data_contents;
	Block* index_block = nullptr;

	index_contents.data = Slice();
	index_contents.cachable = false;
	index_contents.heap_allocated = false;

	Slice temp_1 = rep_->index_block.MakeReadableBlock();

	index_contents.data = temp_1;
	index_block = new Block(index_contents);

	return NewTwoLevelIterator(
		index_block->NewIterator(rep_->options.comparator),
		&TableBuilder::BlockReader, rep_, options);
}

Status TableBuilder::InternalGet(const ReadOptions& options, const Slice& k,
	void* arg,
	void (*saver)(void*, const Slice&, const Slice&)) {

	Status s;
	BlockContents index_contents, data_contents;
	Block* index_block = nullptr;
	Block* data_block = nullptr;
	index_contents.data = Slice();
	index_contents.cachable = false;
	index_contents.heap_allocated = false;

	data_contents.data = Slice();
	data_contents.cachable = false;
	data_contents.heap_allocated = false;


	//index_contents.data = rep_->index_block.FinishWithoutFlag();
	index_contents.data = rep_->index_block.MakeReadableBlock();
	index_block = new Block(index_contents);

	Iterator* iiter = index_block->NewIterator(rep_->options.comparator);
	iiter->Seek(k);
	if (iiter->Valid()) {
		Slice handle_value = iiter->value();
		BlockHandle handle;
		if (handle.DecodeFrom(&handle_value).ok())
		{
			size_t n = static_cast<size_t>(handle.size());
			char* buf = new char[n + kBlockTrailerSize];
			Slice contents;
			//RandomAccessFile* file = nullptr;

			s = rep_->file->Read(handle.offset(), n + kBlockTrailerSize, &contents, buf);
			const char* data = contents.data();    // Pointer to where Read put the data
			if (options.verify_checksums) {
				const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
				const uint32_t actual = crc32c::Value(data, n + 1);
				if (actual != crc) {
					delete[] buf;
					s = Status::Corruption("block checksum mismatch");
					return s;
				}
			}
			data_contents.data = Slice(data, n);
			data_block = new Block(data_contents);

			Iterator* block_iter = data_block->NewIterator(rep_->options.comparator);
			block_iter->Seek(k);
			if (block_iter->Valid()) {
				(*saver)(arg, block_iter->key(), block_iter->value());
			}

			s = block_iter->status();
			delete block_iter;

			if (s.ok()) {
				s = iiter->status();
			}
			delete iiter;
		}
	}
	else {
		//data_contents.data = rep_->data_block.FinishWithoutFlag();
		data_contents.data = rep_->data_block.MakeReadableBlock();
		data_block = new Block(data_contents);
		Iterator* block_iter = data_block->NewIterator(rep_->options.comparator);
		block_iter->Seek(k);
		if (block_iter->Valid()) {
			(*saver)(arg, block_iter->key(), block_iter->value());
		}

		s = block_iter->status();
		delete block_iter;


	}


	return s;
}



void TableBuilder::AddNew(SequenceNumber s, ValueType type, const Slice& key, const Slice& value) {
	Rep* r = rep_;
	assert(!r->closed);
	if (!ok()) return;
	size_t key_size = key.size();
	size_t val_size = value.size();
	size_t internal_key_size = key_size + 8;
	Slice internal_key;
	const size_t encoded_len =
		VarintLength(internal_key_size) + internal_key_size;// +
		VarintLength(val_size) + val_size;

	char* buf = new char[encoded_len];
	char* p = EncodeVarint32(buf, internal_key_size);
	memcpy(p, key.data(), key_size);
	p += key_size;
	EncodeFixed64(p, (s << 8) | type);
	p += 8;

//	p = EncodeVarint32(p, val_size);
//	memcpy(p, value.data(), val_size);
//	assert(p + val_size == buf + encoded_len);

	uint32_t key_length;
	const char* key_ptr = GetVarint32Ptr(buf, buf + 5, &key_length);

	//internal_key = Slice(buf, internal_key_size);
	//internal_key = Slice(key_ptr, key_length - 8);
	internal_key = Slice(key_ptr, key_length);
	//uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);


	if (r->pending_index_entry) { //After Writing Data Block
		assert(r->data_block.empty());
		r->options.comparator->FindShortestSeparator(&r->last_key, internal_key);
		std::string handle_encoding;
		r->pending_handle.EncodeTo(&handle_encoding);
		r->index_block.Add(r->last_key, Slice(handle_encoding));
		r->pending_index_entry = false;
	}

	if (r->filter_block != nullptr) {
		r->filter_block->AddKey(internal_key);
	}

	r->last_key.assign(internal_key.data(), internal_key.size());
	r->num_entries++;
	r->data_block.Add(internal_key, value);

	const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
	if (estimated_block_size >= r->options.block_size) {
		Flush();
	}

	free(buf);
}

void TableBuilder::AddNew(SequenceNumber s, ValueType type, const Slice& key, const Slice& sensorID, const Slice& value) {
	Rep* r = rep_;
	assert(!r->closed);
	if (!ok()) return;
	size_t key_size = key.size();
	size_t val_size = value.size();
	size_t internal_key_size = key_size + 8;
	Slice internal_key;
	const size_t encoded_len =
		VarintLength(internal_key_size) + internal_key_size;// + VarintLength(val_size) + val_size;


	char* buf = new char[encoded_len];
	char* p = EncodeVarint32(buf, internal_key_size);
	memcpy(p, key.data(), key_size);
	p += key_size;
	EncodeFixed64(p, (s << 8) | type);
	p += 8;
	//	p = EncodeVarint32(p, val_size);
	//	memcpy(p, value.data(), val_size);
	//	assert(p + val_size == buf + encoded_len);

	uint32_t key_length;
	const char* key_ptr = GetVarint32Ptr(buf, buf + 5, &key_length);

	//internal_key = Slice(buf, internal_key_size);
	//internal_key = Slice(key_ptr, key_length - 8);
	
	
	internal_key = Slice(key_ptr, key_length);
	
	//internal_key = Slice(key_ptr, key_length-8);


	
	//uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);


	// See Flush function below. It is required for adding Index Entry
	/*if (r->pending_index_entry) {
		assert(r->data_block.empty());
		//r->options.comparator->FindShortestSeparator(&r->last_key, internal_key);
		std::string handle_encoding;
		r->pending_handle.EncodeTo(&handle_encoding);
		r->index_block.Add(r->last_key, Slice(handle_encoding));
		r->pending_index_entry = false;
	} */ 

	if (r->filter_block != nullptr) {
		r->filter_block->AddKey(internal_key);
	}
	

	r->last_key.assign(internal_key.data(), internal_key.size());
	//std::cout << "AFTER LAST_KEY ASSIGN:" << r->last_key << std::endl; //it's normal

	r->num_entries++;
	r->data_block.Add(internal_key, value);

	const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
	//if (true) {
	if (estimated_block_size >= r->options.block_size) {
		Flush();
	}

	delete[] buf;
}

void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  WriteBlock(&r->data_block, &r->pending_handle);
  if (ok()) {
    r->pending_index_entry = true;
    r->status = r->file->Flush();
    //WAF
    if (r->pending_index_entry) {
      //r->options.comparator->FindShortSuccessor(&r->last_key); //what is this? user key vs internal key ?
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      //std::cout << "Adding Index Block:" << r->last_key << ", Size" << r->last_key.size() << "\n";
      r->pending_index_entry = false;
    }
  }
  if (r->filter_block != nullptr) {
    r->filter_block->StartBlock(r->offset);
  }
}

void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  Slice raw = block->Finish();

  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();
  block->Reset();
}

void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type,
                                 BlockHandle* handle) {
  Rep* r = rep_;
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer+1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

Status TableBuilder::status() const {
  return rep_->status;
}
Status TableBuilder::CloseFile() {
	Rep* r = rep_;

	r->status = r->file->Sync();
	if(!r->status.ok())
		return r->status;
	r->status = r->file->Close();
	delete r->file;
	r->file = nullptr;
	return r->status;
}

Status TableBuilder::Finish() {
  Rep* r = rep_;
  //comments for WAF test
  Flush();
  assert(!r->closed);
  r->closed = true;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter block
  if (ok() && r->filter_block != nullptr) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }

  // Write metaindex block
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != nullptr) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  // Write index block
  if (ok()) {
    if (r->pending_index_entry) {
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // Write footer
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const {
  return rep_->num_entries;
}

uint64_t TableBuilder::FileSize() const {
  return rep_->offset;
}

}  // namespace leveldb
