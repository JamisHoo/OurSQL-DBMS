/******************************************************
 *  Copyright (c) 2014 Terranlee
 *  Author: Terranlee
 *  E_mail: ltrthu@163.com
 *  
 *  FileName: db_btree.h
 *  Date:  Oct. 27, 2014 
 *  Time:  19:27:34
 *  *************************************************** */

/****************************************
 *  Structure of index file:
 *      Page0:  _num_pages,  _page_size,  _data_length
 *      Page1:  root node
 *      Page2:  BTreeNode
 *      ...
 *
 *  Structure of each node
 *      Header: _position,  _size,  _leaf
 *      Data:   _data
 *******************************************************/

#ifndef DB_INDEXMANAGER_H_
#define DB_INDEXMANAGER_H_

#include <vector>
#include <iostream>
#include <fstream>
#include "db_common.h"

template<class Comparator>
class Database::DBIndexManager {

    struct BTreeNode {

        BTreeNode() : _position(0), _size(0), _leaf(1) 
        { _data = nullptr; }

        // position in index file
        uint64 _position;

        // number of Entry used in _entries
        uint64 _size;

        // is leaf node or not
        uint64 _leaf;

        // all the entries stored by _data
        char* _data;

        // copy one node to another, used when sloving the split of _root
        void copyKey(BTreeNode& newNode) {

        }

        // split one node into two
        void splitKey(BTreeNode& newNode) {
    
        }
    
        void deleteKey(uint64 pos) {
    
        }
    
        void insertKey(char* newKey, uint64 pos) {
    
        }
    
        uint64 searchKey(char* target, DBFields::Comparator* cmp) {
                
        }
    };


public:

    DBIndexManager(const std::string& file): _file(file), _page_size(0), _num_pages(0), _data_length(0)
    { _node_tracker = nullptr; }

    ~DBIndexManager() {
        if(_num_pages != 0)
            this->close();
        _fs.close();
    }

    // close the btree index and write back the buffer
    // return 0 if close successfully, return 1 when error
    bool close() {
        if(!isopen()) return 1;

        finalize();

        _fs.close();
        if(isopen()) return 1;
        _num_pages = 0;
        return 0;
    }

    // open index file, return _num_pages if open successfully
    // return 0 when error
    uint64 open() {
        // fail if file not accessiable
        if(!accessible()) return 0;
        if(isopen()) return _num_pages;

        _fs.open(_file, std::fstream::in| std::fstream::out| std::fstream::binary);
        if(!isopen()) return 0;

        // open successfully, load data from the first page
        _fs.seekg(0);
        char buffer[sizeof(uint64) * 3];
        _fs.read(buffer, sizeof(uint64) * 3);
        _num_pages = *(pointer_convert<uint64*>(buffer));
        _write_to = _num_pages;
        _page_size = *(pointer_convert<uint64*>(buffer + sizeof(uint64)));
        _data_length = *(pointer_convert<uint64*>(buffer + sizeof(uint64)*2));

        initialize();

        return _num_pages;
    }

    // create index file and init Page0
    // return 0 if create successfully, return 1 when error
    bool create(const uint64 page_size, const uint64 data_length) {
        // fail if file exists
        if(accessible()) return 1;
        _fs.open(_file, std::fstream::out| std::fstream::binary);
        if(!isopen()) return 1;

        _page_size = page_size;
        _data_length = data_length;

        // write page0 after creating
        char buffer[_page_size];
        uint64 numPages = 2;
        memset(buffer, 0xdd, _page_size);
        memcpy(buffer, &numPages, sizeof(numPages));
        memcpy(buffer + sizeof(numPages), &_page_size, sizeof(_page_size));
        memcpy(buffer + sizeof(numPages)*2, &_data_length, sizeof(_data_length));
        wrietPage(0, buffer);

        // write page1(root) after create
        BTreeNode root;
        root._data = new char[_page_size];
        root._position = 1;
        writeNode(1, &root);
        delete[] root._data;

        _fs.close();
    }

    // initialize during open index
    void initialize() {
        // initialize buffer and root
        for(int i=0; i<BUFFER_SIZE; i++) {
            _buffer[i]._node._data = new char[_page_size];
            _buffer[i].dirty = false;
        }
        _root._data = new char[_page_size];
        readNode(1, &_root);
    }

    // finalize during close index
    void finalize() {
        closeBuffer();
        writeNode(1, &_root);
        delete[] _root._data;
    }


    // buffer operation
    void closeBuffer() {
        for(int i=0; i<BUFFER_SIZE; i++){
            if(_buffer[i].dirty == true)
                writeNode(_buffer[i]._node._position, &(_buffer[i]._node));
        }
        for(int i=0; i<BUFFER_SIZE; i++) {
            delete[] _buffer[i]._node._data;
            _buffer[i].dirty = false;
        }
    }

    // some private functions to support btree
    void solveNodeSplit() {}
    void solveRootSplit() {}

    // open an existing index, load index file to _container
    // open at most one index at the same time

    // remove an index file
    bool removeIndex() { }

    // remove record from an open index...
    bool removeRecord() { }
    
    // find...
    RID findRecord() { }
    std::vector<RID> findRecords() { }

    void setComparatorType(const uint64 type) {
        _comparator.type = type;
    }

    // read and write a node to index
    void writeNode(uint64 position, BTreeNode* src) {
        memcpy(src->data, src->_position, sizeof(uint64)*3);
        _fs.seekp(_page_size * position);
        _fs.write(src->data, _page_size);
        writeNumPages(position);
    }

    void readNode(uint64 position, BTreeNode* dst) {
        _fs.seekg(_page_size * position);
        _fs.read(dst->_data, _page_size);
        memcpy(dst->_position, dst->data, sizeof(uint64)*3);
    }

    // read and write page using a char buffer, use this in open/create
    void writePage(uint64 position, char* buffer) {
        _fs.seekp(_page_size * position);
        _fs.write(buffer, _page_size);
        writeNumPages(position);
    }

    // change _num_pages of index file in page0
    void writeNumPages(uint64 position) {
        if(position >= _num_pages) {
            _num_pages = position + 1;
            char buffer[sizeof(_num_pages)];
            memcpy(buffer, &_num_pages, sizeof(_num_pages));
            _fs.seekp(0);
            _fs.write(buffer, sizeof(_num_pages));
        }
    }
    
    // check whether the file is accessible
    bool accessible() const {
        std::ifstream fin(_file);
        return fin.good();
    }
    // returns 1 if file is opened, 0 otherwise
    bool isopen() const { return _fs.is_open(); }



    static constexpr uint64 MAX_LEVEL = 8;
    static constexpr uint64 BUFFER_SIZE = 16;

    // track different level of BTreeNode during search
    // record the position and offset of every node from root to the target
    struct Level {
        uint64 block;
        uint64 offset;
    };
    Level _level[MAX_LEVEL];
    uint64 _lev_track;

    // each buffer node contains a header and data area
    struct BufferNode {
        BTreeNode _node;
        bool _dirty;
    };
    BufferNode _buffer[BUFFER_SIZE];

    BTreeNode _root;
    BTreeNode* _node_tracker;

    uint64 _num_pages;
    uint64 _write_to;
    uint64 _page_size;
    uint64 _data_length;

    DBFields::Comparator _comparator;

    std::string _file;
    std::fstream _fs;
};

#endif /* DB_INDEXMANAGER_H_ */
