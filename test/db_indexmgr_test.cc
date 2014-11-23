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
        manager.create(page_size, data_length, TYPE_INT64);
        manager.open();
    }

    int pageid = 100;
    int slotid = 0;

    // crash when ceiling of i is relatively large
    for (int i = 0; i < 30000; ++i) {
        int64_t tmp = rand() % 5 - 10;
        int rtv = manager.insertRecord(pointer_convert<char*>(&tmp), { pageid, slotid++ }, 0);
        assert(rtv == 1);
        if (slotid == 40) 
            ++pageid, slotid = 0;
    }

    remove("index.idx");
}

