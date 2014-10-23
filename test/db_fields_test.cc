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
    fields.insert(0, 1, 1, "int8_test");
    fields.insert(11, 4, 0, "flaot_test");
    fields.insert(8, 1, 0, "bool_test");
    fields.insert(9, 100, 0, "char[100]_test");

    cout << "Num fields: " << fields.size() << endl;
    cout << "Total length: " << fields.recordLength() << endl;
    
    for (int i = 0; i < fields.size(); ++i)
        cout << fields.field_id()[i] << ' ' << fields.field_type()[i] << ' ' 
             << fields.field_length()[i] << ' ' << fields.primary_key()[i] << ' '
             << fields.field_name()[i] << endl;

    char buff[256] = { 0 };

    fields.generateFieldDescription(3, buff);
    
    ofstream fout("tmp");
    fout.write(buff, 256);


}
