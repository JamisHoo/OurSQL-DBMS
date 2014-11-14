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
    static constexpr uint64 TYPE_BOOL    = 8;
    static constexpr uint64 TYPE_CHAR    = 9;
    static constexpr uint64 TYPE_UCHAR   = 10;

    constexpr uint64 page_size = 128;
    constexpr uint64 data_length = 4;

    DBIndexManager<DBFields::Comparator> manager("index.idx");

    if(manager.open() == 0) {
        manager.create(page_size, data_length, TYPE_CHAR);
        manager.open();
    }


    char a[5] = "abcd";
    char b[5] = "bbcd";
    char c[5] = "cbcd";
    char d[5] = "ddcd";

    manager.insertRecord(a, { 0, 0 }, 0);
    manager.insertRecord(b, { 0, 1 }, 0);
    manager.insertRecord(c, { 0, 2 }, 0);
    manager.insertRecord(d, { 0, 3 }, 0);

    auto rids = manager.rangeQuery(a, c);
    cout << rids.size() << ": "; for (auto rid: rids) cout << rid << ' '; cout << endl;
    
    rids = manager.rangeQuery(c, a);
    cout << rids.size() << ": "; for (auto rid: rids) cout << rid << ' '; cout << endl;

    rids = manager.rangeQuery(b, d);
    cout << rids.size() << ": "; for (RID rid: rids) cout << rid << ' '; cout << endl;

    rids = manager.rangeQuery(a, d);
    cout << rids.size() << ": "; for (auto rid: rids) cout << rid << ' '; cout << endl;

    rids = manager.rangeQuery(d, a);
    cout << rids.size() << ": "; for (auto rid: rids) cout << rid << ' '; cout << endl;

    remove("index.idx");
}
