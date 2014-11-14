/******************************************************************************
 *  Copyright (c) 2014 Jinming Hu 
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: 
 *  Filename: random_test.cc 
 *  Version: 1.0
 *  Author: Jinming Hu
 *  E-mail: hjm211324@gmail.com
 *  Date: Nov. 06, 2014
 *  Time: 20:26:27
 *  Description: 
 *****************************************************************************/
#include <iostream>
#include <algorithm>
#include <map>
#include <vector>
#include <cassert>
#include <cstdlib>
#include "../src/db_tablemanager.h"
#include "../src/db_fields.h"

using namespace std;
using namespace Database;

uint64_t randuint64() {
    uint64_t rnd = (uint64_t(rand()) << 62) | (uint64_t(rand()) << 31) | uint64_t(rand());
    return rnd;
}

std::string randstring(const int len) {
    string str(len, 0);
    generate(str.begin(), str.end(), [](){return rand();});
    return str;
}

bool randbool() {
    return rand() & 1;
}

struct Record {
    uint64_t id;
    char name[100];
    bool clever;
};

struct Comp {
    bool operator()(const RID& r1, const RID& r2) const {
        return r1.pageID < r2.pageID || (r1.pageID == r2.pageID && r1.slotID < r2.slotID);
    }
};

// insert a random record
void insert(DBTableManager& table, std::map<RID, Record, Comp>& reference) {
    Record record;
    record.id = randuint64();
    memcpy(record.name, randstring(100).data(), 100);
    record.clever = randbool();
    auto rid = table.insertRecord({ &record.id, record.name, &record.clever });
    if (!rid) return;
    assert(reference.find(rid) == reference.end());
    reference[rid] = record;
}

// modify a random record
void modify(DBTableManager& table, std::map<RID, Record, Comp>& reference) {
    if (reference.size() == 0) return;
    // fast generate a random rid
    // this tends to select the first slot in the first page when there's few pages
    auto ite = reference.end();
    while (ite == reference.end()) 
        ite = reference.lower_bound(RID(rand() % (prev(reference.end())->first.pageID + 1),
                                        rand() % (prev(reference.end())->first.slotID + 1)));
    
    switch (rand() % 3) {
        case 0:
            uint64_t rnd = randuint64();
            int rtv = table.modifyRecord(ite->first, 0, &rnd);
            if (rtv == 0) ite->second.id = rnd;
            break;
        case 1:
            memcpy(ite->second.name, randstring(100).data(), 100);
            rtv = table.modifyRecord(ite->first, 1, ite->second.name);
            assert(rtv == 0);
            break;
        case 2:
            ite->second.clever = randbool();
            rtv = table.modifyRecord(ite->first, 2, &(ite->second.clever));
            assert(rtv == 0);
    }
}

void remove(DBTableManager& table, std::map<RID, Record, Comp>& reference) {
    if (reference.size() == 0) return;
    auto ite = reference.end();
    while (ite == reference.end()) 
        ite = reference.lower_bound(RID(rand() % (prev(reference.end())->first.pageID + 1),
                                        rand() % (prev(reference.end())->first.slotID + 1)));

    int rtv = table.removeRecord(ite->first);
    assert(!rtv);
    reference.erase(ite);
}
    

// compare all records
void compare(DBTableManager& table, std::map<RID, Record, Comp>& reference) {

    int num = 0;
    auto callback = [&num, &reference](const char* record, const RID rid) {
        auto ite = reference.find(rid);
        assert(ite != reference.end());
        assert(!memcmp(record, &(ite->second.id), 8));
        assert(!memcmp(record + 8, ite->second.name, 100));
        assert(!memcmp(record + 108, &(ite->second.clever), 1));
        ++num;
    };

    table.traverseRecords(callback);

    assert(num == reference.size());

    cout << "Compared all " << num << " elements..." << endl;
}

int main() {
    int seed = time(0);
    srand(0);
    cout << "Seed: " << seed << endl;

    DBTableManager table;
    map<RID, Record, Comp> reference;

    // remove file if exists
    std::remove("student.tb");
    std::remove("student_Student_ID.idx");

    /*
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
    */

    DBFields fields;
    // test create
    fields.insert(DBFields::TYPE_UINT64, 8, 1, 1, "Student_ID");
    fields.insert(DBFields::TYPE_CHAR, 100, 0, 0, "Student_name");
    fields.insert(DBFields::TYPE_BOOL, 1, 0, 0, "Clever_or_Foolish");

    int rtv;
    // create table
    // It's a litter faster with a large page size if there's lots of data
    // If there's little data, small page size will be extremely faster
    rtv = table.create("student", fields, 1024 * 4);
    assert(rtv == 0);

    // open table
    rtv = table.open("student");
    assert(rtv == 0);

    // insert some records
    for (int i = 0; i < 40000; ++i)
        insert(table, reference);

    std::cout << "Insert Finished" << endl;
       
    // random insert/modify/remove record
    for (int i = 0; i < 4000; ++i) {
        switch (rand() % 3) {
            case 0:
                insert(table, reference);
                break;
            case 1: 
                break;
                modify(table, reference);
                break;
            case 2:
                remove(table, reference);
        }
    }

    table.createIndex(1, "fuck");
    table.createIndex(2, "fucktheshit");

    table.checkIndex();
    compare(table, reference);

    // close table
    rtv = table.close();
    assert(rtv == 0);

    rtv = table.open("student");
    assert(rtv == 0);

    std::vector<uint64_t> vec;
    for (const auto& r: reference) 
        vec.push_back(r.second.id);
    sort(vec.begin(), vec.end());

    // test findRecords()
    for (int i = 0; i < 4000; ++i) 
        switch (rand() % 3) {
            case 0: {
                uint64_t pos = randuint64() % vec.size();
                auto rids = table.findRecords(0, pointer_convert<const char*>(vec.data() + pos));
                assert(rids.size() == 1);
                assert(reference[rids[0]].id == vec[pos]);
                break;
            }
            case 1: {
                uint64_t pos = randuint64();
                if (binary_search(vec.begin(), vec.end(), pos)) continue;
                auto rids = table.findRecords(0, pointer_convert<const char*>(&pos));
                assert(rids.size() == 0);
                break;
            }
            case 2: {
                uint64_t pos1 = randuint64() % vec.size();
                uint64_t pos2 = randuint64() % vec.size();
                auto rids = table.findRecords(0, pointer_convert<const char*>(vec.data() + pos1), pointer_convert<const char*>(vec.data() + pos2));
                assert(rids.size() == (pos1 > pos2? 0ull: pos2 - pos1));

                for (auto rid: rids)
                    assert(reference[rid].id >= vec[pos1] && 
                           reference[rid].id < vec[pos2]);
            }
        }
    

    // remove table
    rtv = table.remove();
    assert(rtv == 0);
}
