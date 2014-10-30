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

int main() {
    using namespace std;
    using namespace Database;

    DBTableManager table;

    DBFields fields;
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


    // test create
    fields.insert(DBFields::TYPE_UINT64, 8, 1, "Student ID");
    fields.insert(DBFields::TYPE_CHAR, 100, 0, "Student name");
    fields.insert(DBFields::TYPE_BOOL, 1, 0, "Clever or Foolish");

    int rtv;

    // create
    rtv = table.create("student", fields, 4096);
    assert(rtv == 0);
    
    // open
    rtv = table.open("student");
    assert(rtv == 0);

    unsigned long long student_id = 0x2040;
    char student_name[1000] = "John Idiot";
    bool clever = 1;

    // insert
    for (int i = 0; i < 38; ++i) {
        student_id += 1;
        clever ^= 1;
        rtv = table.insertRecord({ &student_id, student_name, &clever });
        assert(rtv == 0);
    }



    // test insert
    // insert action will be moved into DBTableManager in later version

    // input a field id, create index in which field
    // 0 -- ID, 1 -- name, 2 -- c or f
    int whichField;
    cin >> whichField;
    assert(whichField >= 0 && whichField < 3);

    DBIndexManager<DBFields::Comparator>* index;

    index = new DBIndexManager<DBFields::Comparator>(fields.field_length()[whichField]);

    auto insertToIndex = [&index, &whichField](const char* record, const RID rid) {
        int offset;
        if (whichField == 0) offset = 0;
        if (whichField == 1) offset = 8;
        if (whichField == 2) offset = 108;
        index->insertRecord(record + offset, rid);
    };
    
    index->setComparatorType(fields.field_type()[whichField]);
    table.traverseRecords(insertToIndex);

    cout << "index size: " << index->_dataSet.size() << endl << endl;

    // output index
    ofstream fout("index", fstream::out | fstream::binary);
    index->display(fout);



    rtv = table.close();
    assert(rtv == 0);

    // test remove
    // rtv = table.remove("student");
    // cout << rtv << endl;

}
