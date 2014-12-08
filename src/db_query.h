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
        for (int i = 0; i < kParseFunctions; ++i) {
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
    // returns 0 if parse and execute succeed
    // returns 1 if parse failed
    // returns other values if parse succeed but execute failed
    int parseAsCreateDBStatement(const std::string& str) {
        QueryProcess::CreateDBStatement query;
        
        bool ok = boost::spirit::qi::phrase_parse(str.begin(), 
                                                  str.end(), 
                                                  createDBStatementParser, 
                                                  boost::spirit::qi::space, 
                                                  query); 
        if (ok) {
#ifdef DEBUG
            std::cout << "Parsing succeeded. " << "Get: create database [" << query.db_name << "]\n";
#endif
            // check whether existing file or directory with the same name
            if (boost::filesystem::exists(query.db_name)) return 2;
            // create directory
            if (!boost::filesystem::create_directory(query.db_name)) return 3;
            return 0;
        }
        return 1;
    }
    
    // parse as statement "DROP DATABASE <database name>"
    // returns 0 if parse and execute succeed
    // returns 1 if parse failed
    // returns other values if parse succeed but execute failed
    int parseAsDropDBStatement(const std::string& str) {
        QueryProcess::DropDBStatement query;
        
        bool ok = boost::spirit::qi::phrase_parse(str.begin(), 
                                                  str.end(), 
                                                  dropDBStatementParser, 
                                                  boost::spirit::qi::space, 
                                                  query);
        if (ok) {
#ifdef DEBUG
            std::cout << "Parsing succeeded " << "Get: drop database [" << query.db_name << "]\n";
#endif
            // check whether database exists
            if (!boost::filesystem::exists(query.db_name) || 
                !boost::filesystem::is_directory(query.db_name)) return 2;
            // remove directory
            if (!boost::filesystem::remove_all(query.db_name)) return 3;
            // if a databse in use is dropped, close it.
            if (db_inuse == query.db_name)
                db_inuse = "";
            return 0;
        }
        return 1;
    }

    // parse as statement "USE <database name>"
    // returns 0 if parse and execute succeed
    // returns 1 if parse failed
    // returns other values if parse succeed but execute failed
    int parseAsUseDBStatement(const std::string& str) {
        QueryProcess::UseDBStatement query;

        bool ok = boost::spirit::qi::phrase_parse(str.begin(), 
                                                  str.end(), 
                                                  useDBStatementParser, 
                                                  boost::spirit::qi::space, 
                                                  query);

        if (ok) {
#ifdef DEBUG
            std::cout << "parsing succeed " << "Get: use [" << query.db_name << "]\n";
#endif
            // check whether database exists
            if (!boost::filesystem::exists(query.db_name) || 
                !boost::filesystem::is_directory(query.db_name)) return 2;
            db_inuse = query.db_name;

            return 0;
        }
        return 1;
    }

    // parse as statement "SHOW DATABASES"
    // returns 0 if parse and execute succeed
    // returns 1 if parse failed
    // returns other values if parse succeed but execute failed
    // this function should always execute succeed if parse succeed 
    int parseAsShowDBStatement(const std::string& str) {
        bool ok = boost::spirit::qi::phrase_parse(str.begin(), 
                                                  str.end(), 
                                                  showDBStatementParser, 
                                                  boost::spirit::qi::space
                                                  );
        if (ok) {
#ifdef DEBUG
            std::cout << "Parsing succeed " << "Get: show databases" << std::endl;
#endif
            for (auto f = boost::filesystem::directory_iterator("."); 
                 f != boost::filesystem::directory_iterator(); ++f) 
                if (boost::filesystem::is_directory(f->path()))
                    std::cout << f->path().filename().string() << std::endl;

            return 0;
        }
        return 1;
    }

    // parse as statement "CREATE TABLE <table name> (+<field name>);"
    // returns 0 if parse and execute succeed
    // returns 1 if parse failed
    // returns other values if parse succeed but execute failed
    int parseAsCreateTableStatement(const std::string& str) {
        QueryProcess::CreateTableStatement query;

        bool ok = boost::spirit::qi::phrase_parse(str.begin(),
                                                  str.end(),
                                                  createTableStatementParser,
                                                  boost::spirit::qi::space,
                                                  query
                                                 );
        
        if (ok) {
#ifdef DEBUG
            std::cout << "Parsing succeed Get: create table [" << query.table_name << "] (\n";
            for (const auto& x: query.field_descs) {
                std::cout << x.field_name << ' ' << 
                             x.field_type << ' ';
                
                assert(x.field_length.size() < 2);

                if (x.field_length.size())
                    std::cout << x.field_length[0];
                else 
                    std::cout << "(no length)";
                if (x.field_type_unsigned)
                    std::cout << " unsigned";

                std::cout << ' ' << (x.field_not_null? "not null": "") << std::endl;
            }
            std::cout << "primary key [" << query.primary_key_name << "]" << std::endl;
            std::cout << ");" << std::endl;
#endif

            return 0;
        }
        return 1;
    }
  

private:
    // member function pointers to parser action
    typedef int (DBQuery::*ParseFunctions)(const std::string&);
    constexpr static int kParseFunctions = 5;
    ParseFunctions parseFunctions[kParseFunctions] = {
        &DBQuery::parseAsCreateDBStatement,
        &DBQuery::parseAsDropDBStatement,
        &DBQuery::parseAsUseDBStatement,
        &DBQuery::parseAsShowDBStatement,
        &DBQuery::parseAsCreateTableStatement
    };

    // parsers
    QueryProcess::CreateDBStatementParser createDBStatementParser;
    QueryProcess::DropDBStatementParser dropDBStatementParser;
    QueryProcess::UseDBStatementParser useDBStatementParser;
    QueryProcess::ShowDBStatementParser showDBStatementParser;
    QueryProcess::CreateTableStatementParser createTableStatementParser;
#ifdef DEBUG
public:
#endif
    // database currently using
    std::string db_inuse;

};

#endif /* DB_QUERY_H_ */
