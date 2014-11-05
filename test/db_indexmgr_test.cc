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
#include <cstdlib>
#include "../src/db_tablemanager.h"
#include "../src/db_fields.h"
#include "../src/db_indexmanager.h"

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

    srand(21514);
// test of BTreeNode
/*
    uint64 constexpr _page_size = 4*1024;
    DBIndexManager<DBFields::Comparator>::BTreeNode node;
    
    node._leaf = 1;
    node._size = 4;
    node._position = 1;
    node._data = new char[_page_size];
    DBIndexManager<DBFields::Comparator>::BTreeNode::EntrySize = sizeof(uint64) + sizeof(uint64);
    DBIndexManager<DBFields::Comparator>::BTreeNode::MaxSons = (_page_size - sizeof(uint64)*3) / DBIndexManager<DBFields::Comparator>::BTreeNode::EntrySize;
    DBIndexManager<DBFields::Comparator>::BTreeNode::DataLength = sizeof(uint64);

    //cout<<DBIndexManager<DBFields::Comparator>::BTreeNode::EntrySize<<endl;
    //cout<<DBIndexManager<DBFields::Comparator>::BTreeNode::MaxSons<<endl;
    uint64 arr[] = {1234,1,3456,2,43676,3,34734,4};

    char* pointer = node._data + sizeof(uint64) * 3;
    memcpy(pointer, arr, sizeof(uint64)*8);

    char buffer[sizeof(uint64) + sizeof(uint64)];
    uint64 key = 9999;
    uint64 value = 8888;
    memcpy(buffer, &key, sizeof(uint64));
    memcpy(buffer + sizeof(uint64), &value, sizeof(uint64));
    node.insertKey(buffer, 3);
    node.insertKey(buffer, 3);
    node.deleteKey(6);

    node.display();

    DBFields::Comparator comp;
    comp.type = TYPE_INT64;
    char* pkey = pointer_convert<char*>(&key);
    uint64 next;
    cout<<node.searchKey(pkey, &comp, &next)<<endl;
    uint64 key1 = 123463;
    char* pkey1 = pointer_convert<char*>(&pkey1);
    cout<<node.searchKey(pkey1, &comp, &next)<<endl;
    cout<<endl;

    uint64 writeTest = 10000;
    pkey = pointer_convert<char*>(&writeTest);
    node.writeKey(pkey, 0);

    node.display();

    DBIndexManager<DBFields::Comparator>::BTreeNode newNode;
    newNode._data = new char[_page_size];

    node.splitKey(&newNode);
    
    node.display()    ;
    newNode.display();

    node.copyKey(&newNode);
    newNode.display();

    cout<<newNode.getPosition(0)<<endl;
    cout<<newNode.getPosition(newNode._size-1)<<endl;
*/


//test of index manager
    
    constexpr uint64 page_size = 128;
    constexpr uint64 data_length = 8;

    DBIndexManager<DBFields::Comparator> manager("index.idx");
    manager.setComparatorType(TYPE_INT64);

    if(manager.open() == 0) {
        manager.create(page_size, data_length);
        manager.open();
    }

/*
    for(int i=0; i<100; i++){
        uint64 key = rand() % 123456;
        RID pos(key, key*10);
        manager.insertRecord(pointer_convert<char*>(&key), pos, 0);
        cout<<key<<endl;
    }  
*/


    for(int i=0; i<19; i++){
        uint64 key;
        cin>>key;
        char* keyp = pointer_convert<char*>(&key);
        manager.removeRecord(keyp);
    }


/*
    manager._root._size = 3;
    uint64 arr[] = {1234,1,2345,2,3456,3};
    char* dst = manager._root._data + sizeof(uint64)*3;
    memcpy(dst, arr, sizeof(uint64) * 8);


    manager._root.display();

    uint64 key = 23456;

    RID rid = manager.searchRecord(pointer_convert<char*>(&key));
    cout<<rid.pageID<<" "<<rid.slotID<<endl;
*/

    //manager.close();
    return 0;
}
