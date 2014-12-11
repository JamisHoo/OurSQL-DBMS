/******************************************************************************
 *  Copyright (c) 2014 Terran Lee 
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: Database
 *  Filename: db_query.h 
 *  Version: 1.0
 *  Author: Terran Lee
 *  E-mail: hjm211324@gmail.com
 *  Date: Dec. 05, 2014
 *  Time: 21:01:37
 *  Description: deal with a query. 
                 Call anaylyser to analyse, then compile and execute.
 *****************************************************************************/
#ifndef DB_QUERY_H_
#define DB_QUERY_H_

#include <cstdlib>
#include <set>
#include <map>
#include <unordered_map>
#include <tuple>
#include <boost/filesystem.hpp>
#include "db_query_analyser.h"
#include "db_tablemanager.h"
#include "db_fields.h"

class Database::DBQuery {
public:
    DBQuery() { }

    ~DBQuery() {
        closeDBInUse();
    }

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
#ifdef DEBUG
                    std::cout << "Error code: " << rtv << std::endl;
#endif
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
            std::cout << "Get: create database [" << query.db_name << "]\n";
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
            std::cout << "Get: drop database [" << query.db_name << "]\n";
#endif
            // check whether database exists
            if (!boost::filesystem::exists(query.db_name) || 
                !boost::filesystem::is_directory(query.db_name)) return 2;

            // if this database is opened, close it first
            if (db_inuse == query.db_name)
                closeDBInUse();
            // remove directory
            if (!boost::filesystem::remove_all(query.db_name)) return 3;
            // if a databse in use is dropped, close it.
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
            std::cout << "Get: use [" << query.db_name << "]\n";
#endif
            // check whether database exists
            if (!boost::filesystem::exists(query.db_name) || 
                !boost::filesystem::is_directory(query.db_name)) return 2;
            // if a database has already been opened, close it first
            if (db_inuse.length())
                closeDBInUse();

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
            std::cout << "Get: show databases" << std::endl;
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
            std::cout << "Get: create table [" << query.table_name << "] (\n";
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
                if (x.field_type_signed)
                    std::cout << " signed";

                std::cout << ' ' << (x.field_not_null? "not null": "") << std::endl;
            }
            std::cout << "primary key [" << query.primary_key_name << "]" << std::endl;
            std::cout << ");" << std::endl;
#endif
            std::set<std::string> field_names;
            bool primary_key_exist = 0;
            // field name, field type, field length, is primary key, indexed, not null
            std::vector< std::tuple<std::string, uint64, uint64, bool, bool, bool> > field_descs;
            for (const auto& field: query.field_descs) {
                if (field.field_name.length() > DBFields::FIELD_NAME_LENGTH) 
                    return 2;
                // insert failed, there're duplicate field names
                if (field_names.insert(field.field_name).second == 0)
                    return 3;
                // check primary key name validity
                if (field.field_name == query.primary_key_name) 
                    primary_key_exist = 4;

                auto type_desc = DBFields::datatype_map.find(
                    std::make_tuple(field.field_type, 
                                    field.field_type_unsigned,
                                    field.field_type_signed));

#ifdef DEBUG
                static_assert(std::is_same<decltype(type_desc->second), 
                                           std::tuple<uint64, uint64, bool> >::value, 
                              "not same type");
#endif

                // unsupported type.
                // this is usually because of redundant "unsigned" and "signed"
                if (type_desc == DBFields::datatype_map.end()) 
                    return 5;

                // field length should be explicited provided, but not provided
                if (std::get<2>(type_desc->second) == 1 && field.field_length.size() == 0)
                    return 6;

                field_descs.push_back(std::make_tuple(
                    // field name
                    field.field_name, 
                    // field type
                    std::get<0>(type_desc->second),
                    // is field length must be provided(or use default)?
                    std::get<2>(type_desc->second)? 
                        // provided 
                        field.field_length[0]: 
                        // default
                        std::get<1>(type_desc->second),
                    // is primary key
                    (field.field_name == query.primary_key_name),
                    // indexed, primary key is indexed by default
                    (field.field_name == query.primary_key_name),
                    // not null, primary key is not null by default
                    (field.field_name == query.primary_key_name || 
                     field.field_not_null)));
            }
            
            // invalid primary key name
            if (query.primary_key_name.length() != 0 && primary_key_exist == 0) 
                return 7;
#ifdef DEBUG
            std::cout << "+++++++++++++++++" << std::endl;
            for (const auto& desc: field_descs) {
                std::cout << std::get<0>(desc) << ' ' <<
                             std::get<1>(desc) << ' ' << 
                             std::get<2>(desc) << ' ' <<
                             std::get<3>(desc) << ' ' <<
                             std::get<4>(desc) << ' ' << 
                             std::get<5>(desc) << std::endl;
            }
            std::cout << "+++++++++++++++++" << std::endl;
#endif 
            DBFields dbfields;
            for (const auto& desc: field_descs) 
                dbfields.insert(std::get<1>(desc),
                                std::get<2>(desc),
                                std::get<3>(desc),
                                std::get<4>(desc),
                                std::get<5>(desc),
                                std::get<0>(desc));


            dbfields.addPrimaryKey();
            
            // an oepn database is required.
            if (db_inuse.length() == 0) return 8;

            DBTableManager table_manager;
            // create table
            bool create_rtv = table_manager.create(db_inuse + '/' + query.table_name, dbfields, 
                                                   DBTableManager::DEFAULT_PAGE_SIZE);
            if (create_rtv) return 9;
            
            return 0;
        }
        return 1;
    }

    // parse as statement "SHOW TABLES"
    // returns 0 if parse and execute succeed
    // returns 1 if parse failed
    // returns other values if parse succeed but execute failed
    int parseAsShowTablesStatement(const std::string& str) {
        bool ok = boost::spirit::qi::phrase_parse(str.begin(), 
                                                  str.end(), 
                                                  showTablesStatementParser, 
                                                  boost::spirit::qi::space
                                                  );
        if (ok) {
#ifdef DEBUG
            std::cout << "Get: show tables" << std::endl;
#endif
            // no database is opened
            if (db_inuse.length() == 0) return 2;

            for (auto f = boost::filesystem::directory_iterator(db_inuse); 
                 f != boost::filesystem::directory_iterator(); ++f) 
                if (boost::filesystem::is_regular_file(f->path()) &&
                    boost::filesystem::extension(f->path()) == DBTableManager::TABLE_SUFFIX)
                    std::cout << f->path().stem().filename().string() << std::endl;

            return 0;
        }
        return 1;
    }

    // parse as statement "DROP TABLE <table name>"
    // returns 0 if parse and execute succeed
    // returns 1 if parse failed
    // returns other values if parse succeed but execute failed
    int parseAsDropTableStatement(const std::string& str) {
        QueryProcess::DropTableStatement query;
        
        bool ok = boost::spirit::qi::phrase_parse(str.begin(), 
                                                  str.end(), 
                                                  dropTableStatementParser, 
                                                  boost::spirit::qi::space, 
                                                  query);
        if (ok) {
#ifdef DEBUG
            std::cout << "Get: drop table [" << query.table_name << "]\n";
#endif
            if (db_inuse.length() == 0) return 2;
            // check whether table exists
            if (!boost::filesystem::exists(db_inuse + '/' + query.table_name + DBTableManager::TABLE_SUFFIX) || 
                !boost::filesystem::is_regular_file(db_inuse + '/' + query.table_name + DBTableManager::TABLE_SUFFIX)) 
                return 3;

            // remove table file and related index file
            DBTableManager* table_manager = openTable(query.table_name);
            if (!table_manager) return 4;

            bool rtv = table_manager->remove();
            if (rtv) return 5;
            closeTable(query.table_name);

            return 0;
        }
        return 1;
    }

    // parse as statement "DESC[RIBE] TABLE <table name>"
    // returns 0 if parse and execute  succeed
    // returns 1 if parse failed
    // returns other values if parse succeed but execute failed.
    int parseAsDescTableStatement(const std::string& str) {
        QueryProcess::DescTableStatement query;
        bool ok = boost::spirit::qi::phrase_parse(str.begin(), 
                                                  str.end(),
                                                  descTableStatementParser,
                                                  boost::spirit::qi::space,
                                                  query);
        if (ok) {
#ifdef DEBUG
            std::cout << "Get: desc table [" << query.table_name << "]\n";
#endif
            if (db_inuse.length() == 0) return 2;
            // check whether table exists
            if (!boost::filesystem::exists(db_inuse + '/' + query.table_name + DBTableManager::TABLE_SUFFIX) || 
                !boost::filesystem::is_regular_file(db_inuse + '/' + query.table_name + DBTableManager::TABLE_SUFFIX)) 
                return 3;
            // open table
            DBTableManager* table_manager = openTable(query.table_name);
            // bool rtv = table_manager.open(db_inuse + '/' + query.table_name);
            // open failed
            // if (rtv) return 4;
            if (!table_manager) return 4;
            
            const DBFields& fields_desc = table_manager->fieldsDesc();
            
            // TODO : use speciala outputer
            std::cout << "name, type, primary, not null, index" << std::endl;
            for (int i = 0; i < fields_desc.size(); ++i) {
                // empty field name means this is a auto created primary key field
                if (fields_desc.field_name()[i].length() == 0) continue; 
                std::cout << fields_desc.field_name()[i] << ' ' <<
                             DBFields::datatype_name_map.at(fields_desc.field_type()[i]);
                
                if (fields_desc.field_type()[i] == DBFields::TYPE_CHAR || fields_desc.field_type()[i] == DBFields::TYPE_UCHAR)
                    std::cout << "(" << fields_desc.field_length()[i] - 1 << ')';
                std::cout << ' ' <<
                             (fields_desc.field_id()[i] == fields_desc.primary_key_field_id()) << ' ' <<
                             fields_desc.notnull()[i] << ' ' << 
                             fields_desc.indexed()[i] << std::endl;
            }
            
            return 0;
        }
        return 1;
    }

    // parse as statement "CREATE INDEX ON <table name> (<field name>)"
    // returns 0 if parse and execute  succeed
    // returns 1 if parse failed
    // returns other values if parse succeed but execute failed.
    int parseAsCreateIndexStatement(const std::string& str) {
        QueryProcess::CreateIndexStatement query;
        bool ok = boost::spirit::qi::phrase_parse(str.begin(), 
                                                  str.end(),
                                                  createIndexStatementParser,
                                                  boost::spirit::qi::space,
                                                  query);
        if (ok) {
#ifdef DEBUG
            std::cout << "Get: create index on " << query.table_name << "(" << query.field_name << ")" << std::endl;
#endif
            if (db_inuse.length() == 0) return 2;

            DBTableManager* table_manager = openTable(query.table_name);
            // bool rtv = table_manager.open(db_inuse + '/' + query.table_name);
            // open failed
            // if (rtv) return 3;
            if (!table_manager) return 3;

            auto index_field_ite = std::find(
                table_manager->fieldsDesc().field_name().begin(),
                table_manager->fieldsDesc().field_name().end(),
                query.field_name);

            // invalid table name
            if (index_field_ite == table_manager->fieldsDesc().field_name().end())
                return 4;

            bool rtv = table_manager->createIndex(
                index_field_ite - table_manager->fieldsDesc().field_name().begin(),
                "Index name not supported yet");
            
            // create failed
            if (rtv) return 5;
            return 0;
        }
        return 1;
    }

    // parse as statement "DROP INDEX ON <table name> (<field name>)"
    // returns 0 if parse and execute  succeed
    // returns 1 if parse failed
    // returns other values if parse succeed but execute failed.
    int parseAsDropIndexStatement(const std::string& str) {
        QueryProcess::DropIndexStatement query;
        bool ok = boost::spirit::qi::phrase_parse(str.begin(), 
                                                  str.end(),
                                                  dropIndexStatementParser,
                                                  boost::spirit::qi::space,
                                                  query);
        if (ok) {
#ifdef DEBUG
            std::cout << "Get: drop index on " << query.table_name << "(" << query.field_name << ")" << std::endl;
#endif
            if (db_inuse.length() == 0) return 2;

            DBTableManager* table_manager = openTable(query.table_name);
            // bool rtv = table_manager.open(db_inuse + '/' + query.table_name);
            // open failed
            // if (rtv) return 3;
            if (!table_manager) return 3;

            auto index_field_ite = std::find(
                table_manager->fieldsDesc().field_name().begin(),
                table_manager->fieldsDesc().field_name().end(),
                query.field_name);

            // invalid table name
            if (index_field_ite == table_manager->fieldsDesc().field_name().end())
                return 4;

            bool rtv = table_manager->removeIndex(
                index_field_ite - table_manager->fieldsDesc().field_name().begin());
            
            // remove failed
            if (rtv) return 5;
            return 0;
        }
        return 1;
    }

    // parse as statement "INSERT INTO <table name> VALUES (...) (...) ...;"
    // returns 0 if parse and execute  succeed
    // returns 1 if parse failed
    // returns other values if parse succeed but execute failed.
    int parseAsInsertRecordStatement(const std::string& str) {
        QueryProcess::InsertRecordStatement query;
        bool ok = boost::spirit::qi::phrase_parse(str.begin(), 
                                                  str.end(),
                                                  insertRecordStatementParser,
                                                  boost::spirit::qi::space,
                                                  query);
        if (ok) {
#ifdef DEBUG
            std::cout << "Get: Insert into [" << query.table_name << "] values\n";
            
            for (const auto& value: query.values) {
                std::cout << "\"" << value << "\"" << ' ';
            }
            std::cout << std::endl;
#endif
            if (db_inuse.length() == 0) return 2;

            DBTableManager* table_manager = openTable(query.table_name);
            // int rtv = table_manager.open(db_inuse + '/' + query.table_name);
            // open failed
            // if (rtv) return 3;
            if (!table_manager) return 3;

            DBFields fields_desc = table_manager->fieldsDesc();
            // remove auto-created primary key
            fields_desc.removePrimaryKey();

            std::unique_ptr<char[]> buffer(new char[fields_desc.recordLength()]);
            std::vector<void*> args;

            DBFields::LiteralParser literalParser;

            for (int i = 0; i < query.values.size(); ++i) {
                bool isnull = 0;
                int rtv = literalParser(query.values[i],
                                        fields_desc.field_type()[i],
                                        fields_desc.field_length()[i],
                                        buffer.get() + fields_desc.offset()[i],
                                        isnull);
                // parse failed
                if (rtv == 1) return 4;
                // out of range
                if (rtv == 2) return 5;
                args.push_back(isnull? nullptr: buffer.get() + fields_desc.offset()[i]);
            }
            auto rid = table_manager->insertRecord(args);
            // insert failed
            if (!rid) return 6;
            return 0;
        }
        return 1;

    }

private: 
    DBTableManager* openTable(const std::string& table_name) {
        auto ptr = tables_inuse.find(table_name);
        if (ptr != tables_inuse.end()) return ptr->second;
        DBTableManager* table_manager(new DBTableManager);
        int rtv = table_manager->open(db_inuse + '/' + table_name);
        if (rtv) return nullptr;
        tables_inuse.insert({table_name, table_manager});
        return table_manager;
    }

    void closeTable(const std::string& table_name) {
        auto ptr = tables_inuse.find(table_name);
        assert(ptr!= tables_inuse.end());
        delete ptr->second;
        tables_inuse.erase(ptr);
    }

    void closeDBInUse() {
        for (auto& ptr: tables_inuse) 
            delete ptr.second;
        tables_inuse.clear();
        db_inuse.clear();
    }

private:
    // member function pointers to parser action
    typedef int (DBQuery::*ParseFunctions)(const std::string&);
    constexpr static int kParseFunctions = 11;
    ParseFunctions parseFunctions[kParseFunctions] = {
        &DBQuery::parseAsCreateDBStatement,
        &DBQuery::parseAsDropDBStatement,
        &DBQuery::parseAsUseDBStatement,
        &DBQuery::parseAsShowDBStatement,
        &DBQuery::parseAsCreateTableStatement,
        &DBQuery::parseAsShowTablesStatement,
        &DBQuery::parseAsDropTableStatement,
        &DBQuery::parseAsDescTableStatement,
        &DBQuery::parseAsCreateIndexStatement,
        &DBQuery::parseAsDropIndexStatement,
        &DBQuery::parseAsInsertRecordStatement
    };

    // parsers
    QueryProcess::CreateDBStatementParser createDBStatementParser;
    QueryProcess::DropDBStatementParser dropDBStatementParser;
    QueryProcess::UseDBStatementParser useDBStatementParser;
    QueryProcess::ShowDBStatementParser showDBStatementParser;
    QueryProcess::CreateTableStatementParser createTableStatementParser;
    QueryProcess::ShowTablesStatementParser showTablesStatementParser;
    QueryProcess::DropTableStatementParser dropTableStatementParser;
    QueryProcess::DescTableStatementParser descTableStatementParser;
    QueryProcess::CreateIndexStatementParser createIndexStatementParser;
    QueryProcess::DropIndexStatementParser dropIndexStatementParser;
    QueryProcess::InsertRecordStatementParser insertRecordStatementParser;
#ifdef DEBUG
public:
#endif
    // database currently using
    std::string db_inuse;

    // tables currently opened
    std::unordered_map<std::string, DBTableManager*> tables_inuse;

};

#endif /* DB_QUERY_H_ */
