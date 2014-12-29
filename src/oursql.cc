/******************************************************************************
 *  Copyright (c) 2014 Jamis Hoo 
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: Database
 *  Filename: oursql.cc 
 *  Version: 1.0
 *  Author: Jamis Hoo
 *  E-mail: hjm211324@gmail.com
 *  Date: Dec. 09, 2014
 *  Time: 21:43:46
 *  Description: main function of total project.
 *               read in sql statements and pass them to query parser.
 *****************************************************************************/
#include <iostream>
#include <fstream>
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
    else {
        std::clog << "Welcome to OurSQL(Version 1.0) monitor. " << std::endl;
        std::clog << std::endl;
        std::clog << "Commands end with ;. Press Ctrl + D to exit. " << std::endl;
        std::clog << std::endl;
        std::clog << "Copyright (c) 2014 Jamis Hoo, Terran Lee. " << std::endl;
        std::clog << std::endl;
        std::clog << "Distributed under the MIT license. " << std::endl;
        std::clog << std::endl;
    }
    
    std::string str;
    while (true) {
        // if stdin mode, output prompt
        if (argc == 1) 
            std::clog << (ui.emptyBuff()? "oursql> ": "      > ") << std::flush;

        // read a line
        if (!std::getline(argc == 1? std::cin: fin, str)) break;

        ui.feed(str + '\n');

        // fetch commands and execute
        while (ui.ready()) query.execute(ui.get());
    }
    if (argc == 1) std::clog << "Bye! " << std::endl;
    
    return 0; 
} 
