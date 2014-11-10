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
    
    static constexpr uint64 TYPE_INT64   = 6;

    
    constexpr uint64 page_size = 4096;
    constexpr uint64 data_length = 8;

    DBIndexManager<DBFields::Comparator> manager("index.idx");

    if(manager.open() == 0) {
        manager.create(page_size, data_length, TYPE_INT64);
        manager.open();
    }
    
    bool flags[5] = { 0, 0, 0, 0, 0 };
    for (int i = 0; i < 100000000; ++i) {
        cout << i << endl;
        uint64 rnd = rand() % 5;
        auto rid = manager.searchRecord(pointer_convert<char*>(&rnd));
        bool rtv;
        switch (rid == RID(0, 0)) {
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
        



    manager.close();

    return 0;
}
