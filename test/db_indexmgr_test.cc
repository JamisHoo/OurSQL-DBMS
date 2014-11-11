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

using namespace std;
using namespace Database;

void show(const char* key, const RID rid){
    uint64 pos = *(pointer_convert<const uint64*>(key));
    cout<<pos<<" "<<rid.pageID<<" "<<rid.slotID<<endl;
}

int main() {

    DBTableManager table;

    DBFields fields;
    
    static constexpr uint64 TYPE_INT64   = 6;


    constexpr uint64 page_size = 128;
    constexpr uint64 data_length = 8;

    DBIndexManager<DBFields::Comparator> manager("index.idx");

    if(manager.open() == 0) {
        manager.create(page_size, data_length, TYPE_INT64);
        manager.open();
    }

/*
    for(int i=100; i<200; i++){
        uint64 key = i;
        RID rid(i*10,i*10);
        manager.insertRecord(pointer_convert<char*>(&key), rid, 0);
    }
    
    for(int i=180; i<220; i++){
        uint64 key = i;
        RID rid(i*10,i*10);
        manager.insertRecord(pointer_convert<char*>(&key), rid, 0);
    }
*/
    uint64 lower = 178;
    uint64 upper = 199;

    vector<RID> ridVector = manager.rangeQuery(pointer_convert<char*>(&lower), pointer_convert<char*>(&upper));
    for (std::vector<RID>::iterator i = ridVector.begin(); i != ridVector.end(); ++i)
    {
        cout<<i->pageID<<" "<<i->slotID<<endl;
    }

    
/*
    for(int i=0; i<20; i++){
        uint64 key = 20;
        RID rid(i*10,i*10);
        manager.insertRecord(pointer_convert<char*>(&key), rid, 0);
    }
*/
/*
    for(int i=1; i<=20; i++){
        uint64 key = i;
        RID rid1(i * 10, i * 10);
        RID rid2(i * 100, i * 100);
        manager.insertRecord(pointer_convert<char*>(&key), rid1, 0);
        manager.insertRecord(pointer_convert<char*>(&key), rid2, 0);
    }
    */

/*
    for(int i=0; i<3; i++){
        uint64 key = i;
        manager.removeRecords(pointer_convert<char*>(&key));
        cout<<manager.getNumRecords()<<endl;
        cout<<endl;
    }

    for(int i=5; i<10; i++){
        uint64 key = i;
        manager.removeRecord(pointer_convert<char*>(&key));
        cout<<manager.getNumRecords()<<endl;
        cout<<endl;
    }

    for(int i=1234; i<1236; i++){
        uint64 key = i;
        manager.removeRecord(pointer_convert<char*>(&key));
        cout<<manager.getNumRecords()<<endl;
        cout<<endl;
    }

    for(int i=10; i<12; i++){
        uint64 key = i;
        RID rid(10 * i, 10 * i);
        manager.removeRecord(pointer_convert<char*>(&key), rid);
        cout<<manager.getNumRecords()<<endl;
        cout<<endl;
    }

    for(int i=12; i<15; i++){
        uint64 key = i;
        RID rid(100 * i, 100 * i);
        manager.removeRecord(pointer_convert<char*>(&key), rid);
        cout<<manager.getNumRecords()<<endl;
        cout<<endl;
    }

    for(int i=15; i<17; i++){
        uint64 key = i;
        RID rid(1000 * i, 1000 * i);
        manager.removeRecord(pointer_convert<char*>(&key), rid);
        cout<<manager.getNumRecords()<<endl;
        cout<<endl;
    }

    for(int i=18; i<20; i++){
        uint64 key = i * 100;
        RID rid(100 * i, 100 * i);
        manager.removeRecord(pointer_convert<char*>(&key), rid);
        cout<<manager.getNumRecords()<<endl;
        cout<<endl;
    }
*/

    /*
    for(int i=0; i<10; i++){
        uint64 key = 10;
        RID rid(i*10, i*10);
        manager.removeRecord(pointer_convert<char*>(&key), rid);
        cout<<manager.getNumRecords()<<endl;
    }
    cout<<endl;
    for(int i=30; i<40; i++){
        uint64 key = 10;
        RID rid(i*10, i*10);
        manager.removeRecord(pointer_convert<char*>(&key), rid);
        cout<<manager.getNumRecords()<<endl;
    }
    cout<<endl;
    */
    /*
    uint64 key = 10;
    manager.removeRecord(pointer_convert<char*>(&key));
    cout<<manager.getNumRecords()<<endl;

    key = 20;
    manager.removeRecords(pointer_convert<char*>(&key));
    cout<<manager.getNumRecords()<<endl;
*/

    /*
    bool flags[5] = { 0, 0, 0, 0, 0 };
    for (int i = 0; i < 100000000; ++i) {
        cout << i << endl;
        uint64 rnd = rand() % 5;
        // auto rid = manager.searchRecord(pointer_convert<char*>(&rnd));
        bool rtv;
        // switch (rid == RID(0, 0)) {
        switch (flags[rnd] == 0)  {
            case true:
                assert(flags[rnd] == 0);
                flags[rnd] = 1;
                rtv = manager.insertRecord(pointer_convert<char*>(&rnd), { 100, 100 }, 1);
                assert(rtv);
                break;
            case false:
                assert(flags[rnd] == 1);
                flags[rnd] = 0;
                rtv = manager.removeRecord(pointer_convert<char*>(&rnd));
                assert(rtv);
        }
    }
    */

    manager.close();

    return 0;
}
