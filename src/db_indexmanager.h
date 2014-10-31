/******************************************************************************
 *  Copyright (c) 2014 Jinming Hu 
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: Database
 *  Filename: db_indexmanager.h 
 *  Version: 1.0
 *  Author: Jinming Hu
 *  E-mail: hjm211324@gmail.com
 *  Date: Oct. 29, 2014
 *  Time: 19:27:26
 *  Description: Index manager, just a trial
 *****************************************************************************/
#ifndef DB_INDEXMANAGER_H_
#define DB_INDEXMANAGER_H_

#include <vector>
#include <iostream>
#include <fstream>
#include "db_common.h"

using RecPos = unsigned long long;

struct Database::BTreeNode {
    static constexpr uint64 INDEX_NODE_SIZE = 2 * 1024;
    static constexpr uint64 DATA_FULL_LENGTH = INDEX_NODE_SIZE - sizeof(uint64) * 2 - sizeof(bool);

    static uint64 maxNode;

    // all the entries stored by _data
    char _data[DATA_FULL_LENGTH];

    // position in index file
    RecPos _position;

    // number of Entry used in _entries
    uint64 _size;

    // is leaf node or not
    bool _leaf;

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

    uint64 searchKey(char* target, Comparator* cmp) {
        uint64 location;
        return location;
    }
};

template<class Comparator>
class Database::DBIndexManager {
public:

    DBIndexManager(const uint64 data_length): _data_length(data_length) { }

    ~DBIndexManager() {

    }

    // create an empty index file.
    bool createIndex() { }

    // open an existing index, load index file to _container
    // open at most one index at the same time
    bool openIndex() { }

    bool closeIndex() { }
    
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

    // some private functions to support btree
    void solveNodeSplit() {}
    void solveRootSplit() {}

    static constexpr uint64 MAX_LEVEL = 8;
    static constexpr uint64 BUFFER_SIZE = 16;

    // track different level of BTreeNode during search
    // record the position and offset of every node from root to the target
    struct {
        RecPos block;
        uint64 offset;
    } _level [MAX_LEVEL];
    uint64 _levTrack;

    // buffer of BTreeNode
    struct {
        BTreeNode node;
        bool dirty;
    } _buffer [BUFFER_SIZE];
    BTreeNode* _nodeTrack;

    uint64 _data_length;

    // total number of pages in this index file
    uint64 _num_pages;

    Comparator _comparator;
    
    BTreeNode _root;
};

#endif /* DB_INDEXMANAGER_H_ */
