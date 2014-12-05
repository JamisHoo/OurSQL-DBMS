/******************************************************************************
 *  Copyright (c) 2014 Jinming Hu 
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: 
 *  Filename: db_query_test.cc 
 *  Version: 1.0
 *  Author: Jinming Hu
 *  E-mail: hjm211324@gmail.com
 *  Date: Dec. 05, 2014
 *  Time: 20:43:41
 *  Description: 
 *****************************************************************************/
#include <iostream>
#include "../src/db_query.h"

int main() {
    using namespace std;
    using namespace Database;
    
    DBQuery query;
    query.execute("create database fuck;");
    query.execute("create databasefuck;");
    query.execute("create database\tfuck;");
    query.execute("crEAte datABASe __fuck;");
    query.execute("create database\t\t\tfu__c123k;");
    query.execute("create database 12fuck__;");
    query.execute("createdatabase fuck;");
  
    
    /*
    using namespace std;
    using namespace Database::QueryProcess;
    using namespace boost::spirit;

    std::string str;
    while (getline(std::cin, str)) {
        if (str.empty() || str[0] == 'q' || str[0] == 'Q')
            break;
        
        auto ite_begin(std::begin(str));
        auto ite_end(std::end(str));
        parser<decltype(ite_begin), qi::space_type> p;
        CreateDBStatement query;

        
        bool ok = qi::phrase_parse(ite_begin, ite_end, p, qi::space, query); 
        if (ok) {
            std::cout << "-------------------------\n";
            std::cout << "Stmt: " << str << std::endl;
            std::cout << "Parsing succeeded\n";
            std::cout << "Get: [" << query.db_name << "]\n";
            std::cout << "\n-------------------------\n";
        } else {
            std::cout << "-------------------------\n";
            std::cout << "Stmt: " << str << std::endl;
            std::cout << "Parsing failed\n";
            std::cout << "-------------------------\n";
        }
    }

    std::cout << "Bye... :-) \n\n";
    return 0;
    */

}

