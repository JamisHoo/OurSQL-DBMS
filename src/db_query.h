/******************************************************************************
 *  Copyright (c) 2014 Jinming Hu 
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: Database
 *  Filename: db_query.h 
 *  Version: 1.0
 *  Author: Jinming Hu
 *  E-mail: hjm211324@gmail.com
 *  Date: Dec. 05, 2014
 *  Time: 21:01:37
 *  Description: deal with a query. 
                 Call anaylyser to analyse, then compile and execute.
 *****************************************************************************/
#ifndef DB_QUERY_H_
#define DB_QUERY_H_

#include "db_query_analyser.h"

class Database::DBQuery {
    bool parseAsCreateDBStatement(const std::string& str) {
        using namespace QueryProcess;

        CreateDBStatementParser<std::string::const_iterator, qi::space_type> p;
        CreateDBStatement query;
        
        bool ok = qi::phrase_parse(str.begin(), str.end(), p, qi::space, query); 
        if (ok) {
            std::cout << "-------------------------\n";
            std::cout << "Stmt: " << str << std::endl;
            std::cout << "Parsing succeeded\n";
            std::cout << "Get: [" << query.db_name << "]\n";
            std::cout << "-------------------------\n";
            return 0;
        } else {
            std::cout << "-------------------------\n";
            std::cout << "Stmt: " << str << std::endl;
            std::cout << "Parsing failed\n";
            std::cout << "-------------------------\n";
            return 1;
        }
    }

public:
    bool execute(const std::string& str) {
        if (parseAsCreateDBStatement(str))
            return 1;
    }

};


#endif /* DB_QUERY_H_ */
