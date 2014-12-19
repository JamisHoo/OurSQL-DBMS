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

inline std::string trim(std::string str)
{
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
    
/*
cout << query.execute("CREATE DATABASE orderDB;");
cout << query.execute("USE orderDB;");
cout << query.execute("CREATE TABLE book ( id int(10) NOT NULL, title varchar(100) NOT NULL, authors varchar(200), publisher_id int(10) NOT NULL, copies int(10), PRIMARY KEY  (id));");
    
cout << query.execute("INSERT INTO book VALUES (200011,'Not Without My Daughter','David Adams Richards',101177,567);");
cout << query.execute("INSERT INTO book VALUES (1000, 'fuck \\\" shit', 213, 213);");

    std::cout << std::endl;
    */
} 
