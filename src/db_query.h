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

#include <boost/filesystem.hpp>
#include "db_query_analyser.h"

class Database::DBQuery {
public:
    bool execute(const std::string& str) {
#ifdef DEBUG
        std::cout << "----------------------------\n";
        std::cout << "Stmt: " << str << std::endl;
#endif
        // try to parse with different patterns
        for (int i = 0; i < 2; ++i) {
            int rtv = (this->*parseFunctions[i])(str);

            switch (rtv) {
                // parse and execute sucessful
                case 0: 
                    return 0;
                // parse failed
                case 1:
                    continue;
                // parse sucessful but execute failed
                default:
                    return 1;
            }
        }
#ifdef DEBUG
        std::cout << "Parsing failed. " << std::endl;
#endif
        return 1;
    }

private:
    // parse as statement "CREATE DATABASE <database name>"
    // returns 0 if parse and execute successful
    // returns 1 if parse failed
    // returns other values if parse successful but execute failed
    int parseAsCreateDBStatement(const std::string& str) {
        using namespace QueryProcess;
        using namespace boost::filesystem;

        CreateDBStatementParser<std::string::const_iterator, qi::space_type> parser;
        CreateDBStatement query;
        
        bool ok = qi::phrase_parse(str.begin(), str.end(), parser, qi::space, query); 
        if (ok) {
#ifdef DEBUG
            std::cout << "Parsing succeeded. " << "Get: create database [" << query.db_name << "]\n";
#endif
            // check whether existing file or directory with the same name
            if (exists(query.db_name)) return 2;
            // create directory
            if (!create_directory(query.db_name)) return 3;
            return 0;
        }
        return 1;
    }
    
    // parse as statement "DROP DATABASE <database name>
    // returns 0 if parse and execute successful
    // returns 1 if parse failed
    // returns other values if parse successful but execute failed
    int parseAsDropDBStatement(const std::string& str) {
        using namespace QueryProcess;
        using namespace boost::filesystem;

        DropDBStatementParser<std::string::const_iterator, qi::space_type> parser;
        DropDBStatement query;
        
        bool ok = qi::phrase_parse(str.begin(), str.end(), parser, qi::space, query);
        if (ok) {
#ifdef DEBUG
            std::cout << "Parsing succeeded " << "Get: drop database [" << query.db_name << "]\n";
#endif
            // check whether database exists
            if (!exists(query.db_name) || !is_directory(query.db_name)) return 2;
            // remove directory
            if (!remove_all(query.db_name)) return 3;
            return 0;
        }
        return 1;
    }

    typedef int (DBQuery::*ParseFunctions)(const std::string&);
    ParseFunctions parseFunctions[2] = {
        &DBQuery::parseAsCreateDBStatement,
        &DBQuery::parseAsDropDBStatement
    };

};

#endif /* DB_QUERY_H_ */
