/******************************************************************************
 *  Copyright (c) 2014 Jamis Hoo 
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: 
 *  Filename: sqlfile_test.cc 
 *  Version: 1.0
 *  Author: Jamis Hoo
 *  E-mail: hjm211324@gmail.com
 *  Date: Dec. 09, 2014
 *  Time: 21:43:46
 *  Description: 
 *****************************************************************************/
#include <iostream>
#include "../src/db_query.h"

inline std::string trim(std::string str) {
    str.erase(0, str.find_first_not_of(" \n\t\r"));       //prefixing spaces
    str.erase(str.find_last_not_of(" \n\t\r") + 1);         //surfixing spaces
    return str;
}

int main() {
    using namespace std;
    using namespace Database;
    
    DBQuery query;

    
    string str;
    string tmp;
    int count = 0;
    while (getline(cin, tmp)) {
        if (trim(tmp).front() == '#') continue;
        str += tmp + ' ';
        if (trim(tmp).back() == ';') {
            query.execute(str);
            str.clear();
            ++count;
            if (count == -1) break;
        }
    }
    
} 
