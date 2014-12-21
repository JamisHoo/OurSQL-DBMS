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
#include <regex>
#include "../src/db_query.h"
#include "../src/db_interface.h"

int main() {
    using namespace Database;
    
    DBQuery query;
    DBInterface ui;
    
    std::string str;
    while (std::getline(std::cin, str)) {
        ui.feed(str);
        while (ui.ready()) 
            query.execute(ui.get());
    }
    
    
} 
