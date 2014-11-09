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
    
    constexpr uint64 page_size = 256;
    constexpr uint64 data_length = 8;

    DBIndexManager<DBFields::Comparator> manager("index.idx");

    if(manager.open() == 0) {
        manager.create(page_size, data_length, TYPE_INT64);
        manager.open();
    }

/*
    char buffer[100];
    for(int i=0; i<1000; i++){
        memset(buffer, 0, 100);
        for(int j=0; j<50; j++)
            buffer[j] = 'a' + rand() % 26;
        manager.insertRecord(buffer, RID(rand(), rand()), 0);
    }
*/

/*
    srand(1234);
    for(int i=0; i<10000; i++){
        uint64 key = rand() % 1000;
        RID pos(rand(), rand());
        manager.insertRecord(pointer_convert<char*>(&key), pos, 0);
    }
*/

    /*
    srand(1234);
    for(int i=0; i<50000; i++){
        uint64 key = rand();
        RID pos(rand(), rand());
        char* pointer = pointer_convert<char*>(&key);
        manager.removeRecords(pointer);
    }
    */

    uint64 upper = 123;
    uint64 lower = 119;
    char* pointeru = pointer_convert<char*>(&upper);
    char* pointerl = pointer_convert<char*>(&lower);
    std::vector<RID> rid = manager.rangeQuery(pointerl, pointeru);
    for (std::vector<RID>::iterator i = rid.begin(); i != rid.end(); ++i)
    {
        cout<<i->pageID<<" "<<i->slotID<<endl;
    }
    //manager.traverseRecords(1);
    

    /*
    int k;
    for(int i=0; i<10000; i++)
        cin>>k;

    for(int i=0; i<89500; i++){
        uint64 key;
        cin>>key;
        char* pointer = pointer_convert<char*>(&key);
        manager.removeRecord(pointer);
    }
    */

    
/*    
    char del[] = "bgfpjicbpakbddmfytnquauxkdtskyulfzbqhdtyfebljnsjgh";
    char buffer[100];
    memset(buffer, 0, 100);
    sprintf(buffer, del, 50);
    cout<<buffer<<endl;
    bool answer = manager.removeRecords(buffer);
    cout<<answer<<endl;
*/

/*
    for(int i=0; i<5000; i++){
        uint64 key;
        cin>>key;
    }

    for(int i=0; i<4900; i++){
        uint64 key;
        cin>>key;
        //cout<<key<<endl;
        char* keyp = pointer_convert<char*>(&key);
        manager.removeRecord(keyp);
    }
*/

/*
    uint64 del = 5;
    char* dpointer = pointer_convert<char*>(&del);
    manager.removeRecords(dpointer);
*/

/*
    uint64 lower = 0;
    uint64 upper = 3;
    uint64 notfound = 143564;
    char* lpointer = pointer_convert<char*>(&lower);
    char* upointer = pointer_convert<char*>(&upper);
    char* npointer = pointer_convert<char*>(&notfound);

    vector<RID> ridVector = manager.searchRecords(lpointer);
    for(int i=0; i<ridVector.size(); i++)
        cout<<ridVector[i].pageID<<" "<<ridVector[i].slotID<<endl;

    cout<<endl;
    vector<RID> range = manager.rangeQuery(lpointer, upointer);
    for(int i=0; i<range.size(); i++)
        cout<<range[i].pageID<<" "<<range[i].slotID<<endl;

    cout<<endl;
    std::vector<RID> noOne = manager.rangeQuery(upointer, lpointer);
    cout<<noOne.size()<<endl;

    noOne = manager.searchRecords(npointer);
    cout<<noOne.size()<<endl;
*/

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
