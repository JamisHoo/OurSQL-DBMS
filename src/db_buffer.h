/******************************************************************************
 *  Copyright (c) 2014 Jinming Hu 
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: Database
 *  Filename: db_buffer.h 
 *  Version: 1.0
 *  Author: Jinming Hu
 *  E-mail: hjm211324@gmail.com
 *  Date: Oct. 31, 2014
 *  Time: 15:04:35
 *  Description: Fixed sized data buffer.
                 Read pages from disk, write pages back to disk.
 *****************************************************************************/
#ifndef DB_BUFFER_H_
#define DB_BUFFER_H_

#include <cassert>
#include <cstring>
#include <list>
#include <map>
#include <unordered_map>
#include "db_common.h"
#include "db_file.h"

class Database::DBBuffer {
private:
#ifdef DEBUG
public:
#endif
    class LRU {
    public:
        LRU(const uint64 maxsize): _max_size(maxsize), _unused_buffer(0) {
            assert(maxsize);
        }

        // visit pageID, return bufferID
        // if a dirty LRU page is moved out of buffer, 
        // callback(pageid, bufferid) will be called
        template <class CALLBACK>
        uint64 find(const uint64 pageID, CALLBACK callback, bool* pagemiss = nullptr) {
            auto ite = _map.find(pageID);
            // page miss
            if (ite == _map.end()) {
                if (pagemiss) *pagemiss = 1;
                uint64 bufferID;

                // buffer not yet full
                if (_unused_buffer < _max_size) {
                    bufferID = _unused_buffer++;
                } else {
                // buffer is already full
                // move the LRU page out of buffer
                    uint64 oldPageID = std::get<0>(*_stack.begin());
                    bufferID = std::get<1>(*_stack.begin());

                    // page is dirty
                    if (std::get<2>(*_stack.begin()))
                        callback(oldPageID, bufferID);
                    
                    // remove from stack and hash map
                    assert(_map.erase(oldPageID) == 1);

                    _stack.pop_front();
                }
                _map[pageID] = _stack.insert(_stack.end(), 
                                             std::make_tuple(pageID, bufferID, 0));
                return bufferID;
            }
            // else, page hit
            uint64 bufferID = std::get<1>(*(ite->second));
            // move to list end
            auto node = *(ite->second);
            _stack.erase(ite->second);
            // ite->second = _stack.insert(_stack.end(), node);
            _map[pageID] = _stack.insert(_stack.end(), node);
            return bufferID;
        }

        // mark page as dirty
        void markDirty(const uint64 pageID) {
            auto ite = _map.find(pageID);
            assert(ite != _map.end());
            std::get<2>(*(ite->second)) = 1;
        }
        
        // traverse all dirty pages
        // dirty flag will be cleaned after callback returns
        // callback(pageID, bufferID)
        template <class CALLBACK>
        void traverseDirtyBuffer(CALLBACK callback) {
            for (auto& node: _map)
                if (std::get<2>(*node.second) == 1) {
                    callback(std::get<0>(*node.second), std::get<1>(*node.second));
                    std::get<2>(*node.second) = 0;
                }
        }

#ifdef DEBUG
    public:
        void display(std::ostream& out) {
            for (auto& i: _stack)
                out << std::get<0>(i) << ' ' 
                    << std::get<1>(i) << ' ' 
                    << std::get<2>(i) << std::endl;
        }
#endif

    private: 
        // <page ID, buffer page ID, dirty>
        // 1 means dirty
        // simulate a non-duplicate stack with a list
        // _stack.front() is least recently used, _stack.back() is most recently used
        std::list< std::tuple<uint64, uint64, bool> > _stack;
        // <page ID, _stack iterator>
        std::unordered_map<uint64, std::list< std::tuple<uint64, uint64, bool> >::iterator> _map;
        // std::map<uint64, std::list< std::tuple<uint64, uint64, bool> >::iterator> _map;
        // buffer pages with ID >= _unused_buffer have not been used
        // when _unused_buffer == _max_size; all buffer pages have been used
        uint64 _unused_buffer;
        // max size of buffer, range of bufferID is [0, _max_size)
        uint64 _max_size;
        
    };


public:
    DBBuffer(const std::string& filename, const uint64 buffer_size): 
        _file(filename), _buffer_size(buffer_size),
        _buffer(new char[buffer_size]), _lru(nullptr) { }

    ~DBBuffer() {
        if (isopen()) close();
        delete[] _buffer;
    }

    // directly call corresponding functions in DBFile
    bool isopen() const { return _file.isopen(); }
    bool accessible() const { return _file.accessible(); }
    uint64 pageSize() const { return _file.pageSize(); }
    // both create() and remove() write through to disk
    bool create(const uint64 page_size, const char* buffer) {
        return _file.create(page_size, buffer);
    }
    bool remove() { return _file.remove(); }

    uint64 open() { 
        uint64 page_size = _file.open();
        // if open failed
        if (!page_size) return page_size;

        // if buffer size < page size
        // buffer is disabled
        if (_buffer_size >= page_size)
            _lru = new LRU(_buffer_size / page_size);
        else {
            _lru = nullptr;
            std::cout << "Buffer is disbaled" << std::endl;
        }

        _num_pages = _file.numPages();

        return page_size; 
    }

    // write back all dirty data before closing the file.
    bool close() {
        // if buffer is enabled
        // write back all dirty data
        if (_lru)
            _lru->traverseDirtyBuffer(
                [this](const uint64 pageid, const uint64 bufferpageid) {
                    _file.writePage(pageid, _buffer + bufferpageid * pageSize());
                });

        delete _lru;
        _lru = nullptr;
        _num_pages = 0;
        _file.close();
    }
    
    // write to buffer rather than disk
    void writePage(const uint64 pageid, const char* data) {
        // if buffer is disabled
        if (!_lru) return _file.writePage(pageid, data);

        // get buffer page ID of this page
        uint64 bufferID = _lru->find(pageid, 
            [this](const uint64 oldpageid, const uint64 oldbufferid) {
                _file.writePage(oldpageid, _buffer + oldbufferid * pageSize()); 
            });

        
        // write to buffer
        memcpy(_buffer + bufferID * pageSize(), 
               data,
               pageSize());

        // mark this page as dirty
        _lru->markDirty(pageid);

        // when expanding this file
        if (pageid >= _num_pages) 
            _num_pages = pageid + 1;
        
    }
    
    // check whether already in buffer.
    // if so, return data in buffer
    // else, read to buffer and return
    void readPage(const uint64 pageid, char* data) {
        // if buffer is disabled
        if (!_lru) return _file.readPage(pageid, data);

        // if this flag is set to 1, 
        // the lru only assign a buffer page ID for this page,
        // u've got to read it from file to this buffer page
        bool page_miss = 0;

        // get buffer page ID of this page
        uint64 bufferID = _lru->find(pageid,
            [this](const uint64 oldpageid, const uint64 oldbufferid) {
                _file.writePage(oldpageid, _buffer + oldbufferid * pageSize()); 
            }, &page_miss);

        // if page miss, read in
        if (page_miss) {
            // first time read a page which is in the buffer but not yet written to disk
            if (pageid >= _file.numPages() && pageid < _num_pages)
                memset(data, 0x00, _file.pageSize());

            _file.readPage(pageid, _buffer + bufferID * pageSize());
        }

        // copy to buffer
        memcpy(data,
               _buffer + bufferID * pageSize(),
               pageSize());
    }

    // if this function is called before new pages is written back to disk,
    // it will returns a wrong number
    // there's a delay if using buffer
    uint64 numPages() const {
        if (!_lru) return _file.numPages();
        return _num_pages;
    }

private:
    // forbid copying
    DBBuffer(const DBBuffer&) = delete;
    DBBuffer(DBBuffer&&) = delete;
    DBBuffer& operator=(const DBBuffer&) & = delete;
    DBBuffer& operator=(DBBuffer&&) & = delete;

private:
    // cache for _file._num_pages
    uint64 _num_pages;
    // disk manipulator
    DBFile _file;
    // LUR manager
    LRU* _lru;
    // pointer to buffer
    char* _buffer;
    // size of buffer
    uint64 _buffer_size;
    

};


#endif /* DB_BUFFER_H_ */
