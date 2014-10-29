/******************************************************************************
 *  Copyright (c) 2014 Jinming Hu 
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: 
 *  Filename: db_tablemgr_test.cc 
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
    rtv = table.create("student", fields, 4096);
    assert(rtv == 0);


    
    rtv = table.open("student");
    assert(rtv == 0);

    cout << "Table name: " << table._table_name << endl;
    cout << "Number of fields: " << table._num_fields << endl;
    cout << "Pages in each map page: " << table._pages_each_map_page << endl;
    cout << "Record length: " << table._record_length << endl;
    cout << "Number of records in each page: " << table._num_records_each_page << endl;
    cout << "Last empty slots map page: " << table._last_empty_slots_map_page << endl;
    cout << "Last record page: " << table._last_record_page << endl;

    cout << "Empty slots: " << table._empty_slots_map.size() << endl;
    for (int i = 0; i < table._empty_slots_map.size(); ++i) {
        if (table._empty_slots_map[i])
            cout << "    " << i << endl;
    }

    cout << "DBField info: " << endl;
    DBFields& fields2 = table._fields;
    cout << "Num fields: " << fields2.size() << endl;
    cout << "Total length: " << fields2.recordLength() << endl;
    
    for (int i = 0; i < fields2.size(); ++i)
        cout << fields2.field_id()[i] << ' ' << fields2.field_type()[i] << ' ' 
             << fields2.field_length()[i] << ' ' << fields2.primary_key()[i] << ' '
             << fields2.field_name()[i] << endl;


    
    unsigned long long student_id = 0x2001;
    char student_name[1000] = "John Idiot";
    bool clever = 1;
    
    /*
    // insert until the first map page is full
    for (int i = 0; i < 32573 * 37; ++i) {
        student_id += 1;
        clever ^= 1;
        rtv = table.insertRecord({ &student_id, student_name, &clever });
        assert(rtv == 0);
    }
    */
    
    



    // close table
    rtv = table.close();
    assert(rtv == 0);

    rtv = table.open("student");
    assert(rtv == 0);

    for (int i = 0; i < 38; ++i) {
        student_id += 1;
        clever ^= 1;
        rtv = table.insertRecord({ &student_id, student_name, &clever });
        assert(rtv == 0);
    }


    /*
    // test traverse function
    ofstream fout("traverse_result", fstream::out | fstream::binary);
    auto writeTofile = [&fout, &table](const char* record, const RID rid) {
        fout.write(record, table._record_length);
    };
    
    table.traverseRecords(writeTofile);
    */


    rtv = table.close();
    assert(rtv == 0);

    // test remove
    // rtv = table.remove("student");
    // cout << rtv << endl;


}
