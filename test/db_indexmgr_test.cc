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
    static constexpr uint64 TYPE_INT32   = 4;

    constexpr uint64 page_size = 128;
    constexpr uint64 data_length = 8;

    DBIndexManager<DBFields::Comparator> manager("index.idx");

    if(manager.open() == 0) {
        manager.create(page_size, data_length, TYPE_INT64);
        manager.open();
    }


    uint64 t = 0, f = 1;
    
    std::vector<RID> t_vec;
    std::vector<RID> f_vec;

    uint64 pageid = 1;
    uint64 slotid = 1;
    
    // randomly insert 0 and 1
    for (int i = 0; i < 20; ++i) {
        if (rand() & 1) {
            manager.insertRecord(pointer_convert<char*>(&t), RID( pageid, slotid) , 0);
            t_vec.push_back({ pageid, slotid });
        } else {
            manager.insertRecord(pointer_convert<char*>(&f), RID( pageid, slotid ), 0);
            f_vec.push_back({ pageid, slotid });
        }

        ++slotid;
        if (slotid == 38) 
            slotid = 1, ++pageid;
    }

    sort(t_vec.begin(), t_vec.end(), RID_Comp());
    sort(f_vec.begin(), f_vec.end(), RID_Comp());

    cout << "True vector: " << endl;
    copy(t_vec.begin(), t_vec.end(), ostream_iterator<RID>(cout, ", "));
    cout << endl << "False vector: " << endl;
    copy(f_vec.begin(), f_vec.end(), ostream_iterator<RID>(cout, ", "));
    cout << endl;
  

//    manager.show();


    // search 0 or 1
    for (int i = 0; i < 200000; ++i) {
        int rnd = rand() & 1;
        auto rids = manager.searchRecords(pointer_convert<char*>(rnd? &t: &f));
        sort(rids.begin(), rids.end(), RID_Comp());

        if (rids != (rnd? t_vec: f_vec)) {
            cout << "rnd: " << rnd << endl;
            cout << "i: " << i << endl;
            cout << "RIDs: " << endl;
            copy(rids.begin(), rids.end(), ostream_iterator<RID>(cout, ", "));
            cout << endl;
            assert(0);
        }
    }



}
