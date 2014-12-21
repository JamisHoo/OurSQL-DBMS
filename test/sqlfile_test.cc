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
#include <fstream>
#include <regex>
#include "../src/db_query.h"
#include "../src/db_interface.h"

int main(int argc, char** argv) {
    using namespace Database;

    // argc == 1, stdin
    // argc == 2, read from file
    if (argc > 2) return 1;
    
    DBQuery query;
    DBInterface ui;

    std::ifstream fin;
    // open file
    if (argc == 2)
        fin.open(argv[1]);
    
    // if stdin mode, output prompt
    if (argc == 1) std::clog << "yoursql> " << std::flush;

    std::string str;
    while (std::getline(argc == 1? std::cin: fin, str)) {
        // read a line
        ui.feed(str + '\n');

        // fetch commands and execute
        while (ui.ready()) query.execute(ui.get());

        // if stdin mode, output prompt
        if (argc == 1) {
            if (ui.emptyBuff()) std::clog << "yoursql> " << std::flush;
            else std::clog << "       > " << std::flush;
        }
    }
    if (argc == 1) std::clog << std::endl;
    
    
} 
