/******************************************************************************
 *  Copyright (c) 2014 Jinming Hu 
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: 
 *  Filename: db_indexmgr_test.cc 
 *  Version: 1.0
 *  Author: Jinming Hu
 *  E-mail: hjm211324@gmail.com
 *  Date: Oct. 24, 2014
 *  Time: 09:44:22
 *  Description: 
 *****************************************************************************/
#include <iostream>
#include <cassert>
#include "../src/db_tablemanager.h"
#include "../src/db_fields.h"
#include "../src/db_indexmanager.h"

class test{
public:
    static int counter;
};


int main() {
    using namespace std;
    using namespace Database;

    DBTableManager table;

    DBFields fields;
    
    static constexpr uint64 TYPE_INT8    = 0;
    static constexpr uint64 TYPE_UINT8   = 1;
    static constexpr uint64 TYPE_INT16   = 2;
    static constexpr uint64 TYPE_UINT16  = 3;
    static constexpr uint64 TYPE_INT32   = 4;
    static constexpr uint64 TYPE_UINT32  = 5;
    static constexpr uint64 TYPE_INT64   = 6;
    static constexpr uint64 TYPE_UINT64  = 7;
    static constexpr uint64 TYPE_BOOL    = 8;
    static constexpr uint64 TYPE_CHAR    = 9;
    static constexpr uint64 TYPE_UCHAR   = 10;
    static constexpr uint64 TYPE_FLOAT   = 11;
    static constexpr uint64 TYPE_DOUBLE  = 12;
    
/*
    // init BTreeNode and Compare
    DBFields::Comparator cmp;
    cmp.type == TYPE_INT64;
    BTreeNode node;
    BTreeNode::_data_length = sizeof(uint64) + BTreeNode::REC_SIZE;
    BTreeNode::_max_node = BTreeNode::INDEX_NODE_SIZE / BTreeNode::_data_length;

    // init one node
    uint64 arr[] = {1234,1,2345,2,3456,3};
    memcpy(node._data, arr, sizeof(uint64) * 6);
    node._size = 3;

    // try search
    uint64 key = 1234;
    char* target = pointer_convert<char*>(&key);
    node.searchKey(target, &cmp);
    */
}
