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
#include <vector>
#include <iterator>
#include <algorithm>
#include <cassert>
#include <cstdlib>
#include "../src/db_tablemanager.h"
#include "../src/db_fields.h"
#include "../src/db_indexmanager.h"

using namespace std;
using namespace Database;

struct RID_Comp {
    bool operator()(const RID& rid1, const RID& rid2) const {
        return rid1.pageID == rid2.pageID? rid1.slotID < rid2.slotID: rid1.pageID < rid2.pageID;
    }
};

int main() {

    DBTableManager table;

    DBFields fields;
    
    static constexpr uint64 TYPE_INT64   = 6;

    constexpr uint64 page_size = 128;
    constexpr uint64 data_length = 8;

    DBIndexManager<DBFields::Comparator> manager("index.idx");

    if(manager.open() == 0) {
        manager.create(page_size, data_length + 1, TYPE_INT64);
        manager.open();
    }

    int pageid = 100;
    int slotid = 100;

    char* buff1 = new char[data_length + 1];
    char* buff2 = new char[data_length + 1];
    buff1[0] = '\xff';
    buff2[0] = '\xff';

    for (int i = 0; i < 300000; ++i) {
        int64_t tmp = rand() % 5 - 10;
        memcpy(buff1 + 1, &tmp, data_length);
        int rtv = manager.insertRecord(buff1, { pageid, slotid++ }, 0);
        assert(rtv == 1);
    }

    remove("index.idx");
}
