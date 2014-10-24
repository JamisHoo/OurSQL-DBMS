/******************************************************************************
 *  Copyright (c) 2014 Jinming Hu 
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: 
 *  Filename: db_fields_test.cc 
 *  Version: 1.0
 *  Author: Jinming Hu
 *  E-mail: hjm211324@gmail.com
 *  Date: Oct. 23, 2014
 *  Time: 21:29:34
 *  Description: 
 *****************************************************************************/
#include <iostream>
#include <fstream>
#include "../src/db_fields.h"
#include "../src/db_tablemanager.h"

int main() {
    using namespace std;
    using namespace Database;

    DBFields fields;
    
    // type, length, primary_key, name
    char name_tmp[231];
    for (int i = 0; i < 231; ++i)
        name_tmp[i] = 'a';

    fields.insert(0, 1, 1, name_tmp);
    fields.insert(11, 4, 0, "flaot_test");
    fields.insert(8, 1, 0, "bool_test");
    fields.insert(9, 100, 0, "char[100]_test");

    cout << "Num fields: " << fields.size() << endl;
    cout << "Total length: " << fields.recordLength() << endl;
    
    for (int i = 0; i < fields.size(); ++i)
        cout << fields.field_id()[i] << ' ' << fields.field_type()[i] << ' ' 
             << fields.field_length()[i] << ' ' << fields.primary_key()[i] << ' '
             << fields.field_name()[i] << endl;

    char buff1[256] = { 0 };
    char buff2[256] = { 0 };
    char buff3[256] = { 0 };
    char buff4[256] = { 0 };

    fields.generateFieldDescription(0, buff1);
    fields.generateFieldDescription(1, buff2);
    fields.generateFieldDescription(2, buff3);
    fields.generateFieldDescription(3, buff4);
    
    ofstream fout("tmp");
    fout.write(buff1, 256);
    fout.write(buff2, 256);
    fout.write(buff3, 256);
    fout.write(buff4, 256);
    fout.close();

    memset(buff1, 0, 256);
    memset(buff2, 0, 256);
    memset(buff3, 0, 256);
    memset(buff4, 0, 256);
    ifstream fin("tmp");
    fin.read(buff1, 256);
    fin.read(buff2, 256);
    fin.read(buff3, 256);
    fin.read(buff4, 256);
    fin.close();

    fout.open("tmp2");
    fout.write(buff1, 256);
    fout.write(buff2, 256);
    fout.write(buff3, 256);
    fout.write(buff4, 256);
    fout.close();

    DBFields fields2;
    fields2.insert(buff1, 256);
    fields2.insert(buff2, 256);
    fields2.insert(buff3, 256);
    fields2.insert(buff4, 256);

    cout << "Num fields: " << fields2.size() << endl;
    cout << "Total length: " << fields2.recordLength() << endl;
    
    for (int i = 0; i < fields2.size(); ++i)
        cout << fields2.field_id()[i] << ' ' << fields2.field_type()[i] << ' ' 
             << fields2.field_length()[i] << ' ' << fields2.primary_key()[i] << ' '
             << fields2.field_name()[i] << endl;



}
