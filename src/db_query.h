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
#include <regex>
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

    bool execute(const std::string& str, std::ostream& out = std::cout, std::ostream& err = std::cerr) {
#ifdef DEBUG
        std::cout << "----------------------------\n";
        std::cout << "Stmt: " << str << std::endl;
#endif
        try {
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
                        std::cout << "Error code: " << rtv << std::endl;
                        return 1;
                }
            }
            // parse failed
            throw ParseFailed();

        } catch(const ParseFailed&) {
            err << "Error: ";
            err << "Parse failed. " << std::endl;  
        } catch (const PathExists& error) {
            err << "Error: ";
            err << "File or directory \"" << error.name << "\" already exists. " 
                << std::endl;
        } catch (const CreateDBFailed& error) {
            err << "Error: ";
            err << "Error when creating Database \"" << error.name << "\"." 
                << std::endl;
        } catch (const DBNotExists& error) {
            err << "Error: ";
            err << "Database \"" << error.name << "\" not exists. " << std::endl;
        } catch (const RemoveFailed& error) {
            err << "Error: ";
            err << "Error when removing \"" << error.name << "\"." << std::endl;
        } catch (const FieldNameTooLong& error) {
            err << "Error: ";
            err << "Field name \"" << error.name << "\" of length " 
                << error.length << " exceeds length limitation " << 
                error.max_length << "." << std::endl;
        } catch (const DuplicateFieldName& error) {
            err << "Error: ";
            if (error.name.length()) 
                err << "Duplicate field name \"" << error.name << "\"." << std::endl;
            else 
                err << "Duplicate field name." << std::endl;
        } catch (const UnsupportedType& error) {
            err << "Error: ";
            err << "Unsupported type \"" << (error.is_unsigned? "unsigned ": "") 
                << (error.is_signed? "signed ": "") 
                << error.type_name << "\"." << std::endl;
        } catch (const FieldLengthRequired& error) {
            err << "Error: ";
            err << "Field \"" << error.field_name << "\" of type \"" 
                << error.field_type << "\" required an explicit length." 
                << std::endl;
        } catch (const InvalidPrimaryKey& error) {
            err << "Error: ";
            err << "Invalid primary key \"" << error.name << "\"." << std::endl;
        } catch (const DBNotOpened&) {
            err << "Error: ";
            err << "No database is opened. " << std::endl;
        } catch (const CreateTableFailed& error) {
            err << "Error: ";
            err << "Error when creating table \"" << error.name
                << "\"." << std::endl;
        } catch (const OpenTableFailed& error) {
            err << "Error: ";
            err << "Error when opening table \"" << error.name << "\". "
                << error.getInfo() << std::endl;
        } catch (const RemoveTableFailed& error) {
            err << "Error: ";
            err << "Error when removing table \"" << error.name << "\"."
                << std::endl;
        } catch (const InvalidFieldName& error) {
            err << "Error: ";
            err << "Invalid field name \"" << error.name << "\"." << std::endl;
        } catch (const CreateIndexFailed& error) {
            err << "Error: ";
            err << "Error when creating index on field \"" << error.field_name 
                << "\" of table \"" << error.table_name << "\"." << std::endl;
        } catch (const RemoveIndexFailed& error) {
            err << "Error: ";
            err << "Error when removing index on field \"" << error.field_name 
                << "\" of table \"" << error.table_name << "\"." << std::endl;
        } catch (const LiteralParseFailed& error) {
            err << "Error: ";
            err << "Error when parsing literal \"" << error.literal << "\"."
                << std::endl;
        } catch (const LiteralOutofrange& error) {
            err << "Error: ";
            err << "Literal \"" << error.literal << "\" out of range." 
                << std::endl;
        } catch (const InsertRecordFailed& error) {
            err << "Error: ";
            err << "Error when inserting tuple (";
            for (int i = 0; i < error.values.size(); ++i) {
                if (i) err << ", ";
                err << error.values[i];
            }
            err << ") into table \"" << error.table_name << "\". ";
            if (error.getInfo().length())
                err << error.getInfo();
            err << std::endl;
        } catch (const InvalidWhereClause& error) {
            err << "Error: ";
            err << "Invalid where clause. ";
            if (error.getInfo().length())
                err << "Error near \"" << error.getInfo() << "\". ";
            err << std::endl;
        }


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
            if (boost::filesystem::exists(query.db_name)) 
                throw PathExists(query.db_name);
            // create directory
            if (!boost::filesystem::create_directory(query.db_name)) 
                throw CreateDBFailed(query.db_name);
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
                !boost::filesystem::is_directory(query.db_name))
                throw DBNotExists(query.db_name);

            // if this database is opened, close it first
            if (db_inuse == query.db_name)
                closeDBInUse();
            // remove directory
            try {
                if (!boost::filesystem::remove_all(query.db_name)) 
                    throw RemoveFailed(query.db_name);
            } catch (...) {
                throw RemoveFailed(query.db_name);
            }
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
                !boost::filesystem::is_directory(query.db_name)) 
                throw DBNotExists(query.db_name);
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
                    throw FieldNameTooLong(field.field_name, field.field_name.length(), DBFields::FIELD_NAME_LENGTH);
                // insert failed, there're duplicate field names
                if (field_names.insert(field.field_name).second == 0)
                    throw DuplicateFieldName(field.field_name);
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
                    throw UnsupportedType(field.field_type, 
                                          field.field_type_unsigned, 
                                          field.field_type_signed);

                // field length should be explicited provided, but not provided
                if (std::get<2>(type_desc->second) == 1 && field.field_length.size() == 0)
                    throw FieldLengthRequired(field.field_name, field.field_type);

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
                throw InvalidPrimaryKey(query.primary_key_name);
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
            
            // an open database is required.
            if (db_inuse.length() == 0) throw DBNotOpened();

            DBTableManager table_manager;
            // create table
            bool create_rtv = table_manager.create(db_inuse + '/' + query.table_name, dbfields, 
                                                   DBTableManager::DEFAULT_PAGE_SIZE);
            if (create_rtv) throw CreateTableFailed(query.table_name);
            
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
            if (db_inuse.length() == 0) throw DBNotOpened();

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
            if (db_inuse.length() == 0) throw DBNotOpened();

            // remove table file and related index file
            DBTableManager* table_manager = openTable(query.table_name);
            if (!table_manager) throw OpenTableFailed(query.table_name);

            bool rtv = table_manager->remove();
            if (rtv) throw RemoveTableFailed(query.table_name);
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
            if (db_inuse.length() == 0) throw DBNotOpened();
            
            // open table
            DBTableManager* table_manager = openTable(query.table_name);
            // open failed
            if (!table_manager) throw OpenTableFailed(query.table_name);
            
            const DBFields& fields_desc = table_manager->fieldsDesc();
            
            // TODO : use special outputer
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
            if (db_inuse.length() == 0) throw DBNotOpened();

            DBTableManager* table_manager = openTable(query.table_name);
            // open failed
            if (!table_manager) throw OpenTableFailed(query.table_name);

            auto index_field_ite = std::find(
                table_manager->fieldsDesc().field_name().begin(),
                table_manager->fieldsDesc().field_name().end(),
                query.field_name);

            // invalid field name
            if (index_field_ite == table_manager->fieldsDesc().field_name().end())
                throw InvalidFieldName(query.field_name);

            bool rtv = table_manager->createIndex(
                index_field_ite - table_manager->fieldsDesc().field_name().begin(),
                "Index name not supported yet");
            
            // create failed
            if (rtv) throw CreateIndexFailed(query.table_name, query.field_name);
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
            if (db_inuse.length() == 0) throw DBNotOpened();

            DBTableManager* table_manager = openTable(query.table_name);
            // open failed
            if (!table_manager) throw OpenTableFailed(query.table_name);

            auto index_field_ite = std::find(
                table_manager->fieldsDesc().field_name().begin(),
                table_manager->fieldsDesc().field_name().end(),
                query.field_name);

            // invalid field name
            if (index_field_ite == table_manager->fieldsDesc().field_name().end())
                throw InvalidFieldName(query.field_name);

            bool rtv = table_manager->removeIndex(
                index_field_ite - table_manager->fieldsDesc().field_name().begin());
            
            // remove failed
            if (rtv) throw RemoveIndexFailed(query.table_name, query.field_name);
            return 0;
        }
        return 1;
    }

    // parse as statement "INSERT INTO <table name> VALUES (...);"
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
            // assert database is opened 
            if (db_inuse.length() == 0) throw DBNotOpened();
            
            // open table
            DBTableManager* table_manager = openTable(query.table_name);
            // open failed
            if (!table_manager) throw OpenTableFailed(query.table_name);
            
            // get fileds description
            const DBFields& fields_desc = table_manager->fieldsDesc();
            
            std::unique_ptr<char[]> buffer(new char[fields_desc.recordLength()]);

            insertRecord(query.table_name, table_manager, query.values, buffer.get());
            
            return 0;
        }
        return 1;

    }

    // parse as statement "SELECT <field name> [, <field name>]* FROM <table name> [WHERE <simple condition>];"
    //                  or "SELECT * FROM <table name> [WHERE <simple condition>];"
    // returns 0 if parse and execute  succeed
    // returns 1 if parse failed
    // returns other values if parse succeed but execute failed.
    int parseAsSimpleSelectStatement(const std::string& str) {
        QueryProcess::SimpleSelectStatement query;
        bool ok = boost::spirit::qi::phrase_parse(str.begin(), 
                                                  str.end(),
                                                  simpleSelectStatementParser,
                                                  boost::spirit::qi::space,
                                                  query);
        if (ok) {
#ifdef DEBUG
            std::cout << "Get: select ";
            for (const auto& f: query.field_names)
                std::cout << '['<< f << "] ";
            std::cout << "from [" << query.table_name << "] where " << query.condition.left_expr << ' ' << query.condition.op << ' ' << query.condition.right_expr << std::endl;
#endif

            if (db_inuse.length() == 0) throw DBNotOpened();

            DBTableManager* table_manager = openTable(query.table_name);
            // open failed
            if (!table_manager) throw OpenTableFailed(query.table_name);

            const DBFields& fields_desc = table_manager->fieldsDesc();
            // check field names
            std::vector<uint64> display_field_ids;
            std::set<uint64> check_duplicate_field_id;
            for (const auto& field_name: query.field_names) {
                if (field_name == "*") { 
                    for (const auto& field_id: fields_desc.field_id()) 
                        if (fields_desc.field_name()[field_id].length()) {
                            display_field_ids.push_back(field_id);
                            check_duplicate_field_id.insert(field_id);
                        }
                } else {
                    auto ite = std::find(fields_desc.field_name().begin(),
                                         fields_desc.field_name().end(),
                                         field_name);
                    // invalid field name
                    if (ite == fields_desc.field_name().end()) 
                        throw InvalidFieldName(field_name);
                    uint64 field_id = ite - fields_desc.field_name().begin();
                    display_field_ids.push_back(field_id);
                    check_duplicate_field_id.insert(field_id);
                }
            }
            // duplicate field_name
            if (display_field_ids.size() != check_duplicate_field_id.size()) 
                throw DuplicateFieldName("");


            // check where clause
            int constant_condition = 2;
            std::tuple<int, uint64, std::string, std::string> cond;
            // no where clause, equvalent to conditon is always true
            if (query.condition.left_expr.length() +
                query.condition.right_expr.length() +
                query.condition.op.length() == 0) {
                constant_condition = 1;
                std::cout << "Always true" << std::endl;
            } else {
            // else, parse where clause
                cond = parseSimpleCondition(query.condition, fields_desc);
                // condition parse failed
                if (std::get<0>(cond) >= 3) throw InvalidWhereClause();
                if (std::get<0>(cond) == 0) {
                    constant_condition = 0;
                    std::cout << "Always false" << std::endl;
                }
                if (std::get<0>(cond) == 1) {
                    constant_condition = 1;
                    std::cout << "Always true" << std::endl;
                }
                if (std::get<0>(cond) == 2) {
                    std::cout << fields_desc.field_name()[std::get<1>(cond)] << std::get<2>(cond);
                    for (int i = 0; i < std::get<3>(cond).length(); ++i)
                        printf("%02x ", int(std::get<3>(cond)[i]) & 0xff);
                    std::cout << std::endl;
                }
            }

            auto rids = selectRID(table_manager, std::get<1>(cond), 
                                  fields_desc.field_length()[std::get<1>(cond)],
                                  fields_desc.field_type()[std::get<1>(cond)],
                                  std::get<2>(cond), std::get<3>(cond),
                                  constant_condition);


            outputRID(table_manager, fields_desc, display_field_ids, rids);

            return 0;
        }
        return 1;
    }
     

private: 


    void insertRecord(const std::string& table_name, DBTableManager* table_manager, 
                      const std::vector<std::string>& values, char* buffer) {
        const DBFields& fields_desc = table_manager->fieldsDesc();

        // buffer is asserted to be cleared by caller
        memset(buffer, 0x00, fields_desc.recordLength());

        // args with null flags
        std::vector<void*> args;

        // check fields size
        uint64 expected_size = fields_desc.size() - 
            (fields_desc.field_name()[fields_desc.primary_key_field_id()].length() == 0? 1: 0);
        if (values.size() != expected_size)
            throw WrongTupleSize(table_name, values, values.size(), expected_size);

        for (int i = 0; i < values.size(); ++i) {
            std::cout << values[i] << std::endl;
            int rtv = literalParser(values[i],
                                    fields_desc.field_type()[i],
                                    fields_desc.field_length()[i],
                                    buffer + fields_desc.offset()[i]
                                   );
            // parse failed
            if (rtv == 1) throw LiteralParseFailed(values[i]);
            // out of range
            if (rtv == 2) throw LiteralOutofrange(values[i]);
            args.push_back(buffer + fields_desc.offset()[i]);
        }

        // auto created primary key
        if (fields_desc.field_name()[fields_desc.primary_key_field_id()].length() == 0) {
            // null flag
            (buffer + fields_desc.offset()[fields_desc.primary_key_field_id()])[0] = '\xff';
            uint64 unique_number = uniqueNumber();
            memcpy(buffer + fields_desc.offset()[fields_desc.primary_key_field_id()] + 1, 
                   &unique_number, 
                   fields_desc.field_length()[fields_desc.primary_key_field_id()] - 1);
            assert(fields_desc.field_length()[fields_desc.primary_key_field_id()] - 1 == sizeof(uint64));
            args.push_back(buffer + fields_desc.offset()[fields_desc.primary_key_field_id()]);
        }

        auto rid = table_manager->insertRecord(args);
        // insert failed
        if (rid == RID(0, 1)) 
            throw WrongTupleSize(table_name, values, args.size(), fields_desc.size());
        else if (rid == RID(0, 3)) 
            throw NotNullExpected(table_name, values);
        else if (rid == RID(0, 4))
            throw DuplicatePrimaryKey(table_name, values);
        else if (!rid)
            throw InsertRecordFailed(table_name, values);
    }

    // output a certain record
    void outputRID(const DBTableManager* table_manager,
                   const DBFields& fields_desc, 
                   const std::vector<uint64> display_field_ids,
                   const std::vector<RID> rids) {
        std::unique_ptr<char[]> buff(new char[fields_desc.recordLength()]);

        std::string output_buff;
        for (const auto rid: rids) {
            table_manager->selectRecord(rid, buff.get());
            for (const auto id: display_field_ids) {
                literalParser(buff.get() + fields_desc.offset()[id],
                              fields_desc.field_type()[id],
                              fields_desc.field_length()[id],
                              output_buff);
                std::cout << output_buff << ' ';
            }
            std::cout << std::endl;
        }
    }

    // select rids meeting conditons
    // const_condition == 2 -> no constant condition
    // const_condition == 1 -> constant condition true
    // const_condition == 0 -> constant condition false
    std::vector<RID> selectRID(const DBTableManager* table_manager,
                               const uint64 field_id, const uint64 field_length,
                               const uint64 field_type, const std::string& op, 
                               const std::string& right_value, 
                               const int const_condition) {
        std::vector<RID> rids;
        // TODO: optimise: directly output
        // return all rids
        if (const_condition == 1) 
            return table_manager->findRecords(field_id, [](const char*) { return 1; });
        // return empty rids 
        if (const_condition == 0)
            return rids;

        // TODO: use index
        
        DBFields::Comparator comp;
        comp.type = field_type;
        // if there isn't index on field_id
        
        std::string null_value = std::string(field_length, '\x00');
        rids = table_manager->findRecords(field_id, 
            [&right_value, &field_length, &op, &comp, &null_value](const char* data)->bool {
                int comp_result;
                if (op == "=") 
                    return comp(data, right_value.data(), field_length) == 0 &&
                           (comp(right_value.data(), null_value.data(), field_length) == 0 ||
                           comp(data, null_value.data(), field_length) != 0);
                if (op == ">")
                    return comp(data, right_value.data(), field_length) >= 1 &&
                           (comp(right_value.data(), null_value.data(), field_length) == 0 ||
                           comp(data, null_value.data(), field_length) != 0);
                if (op == "<") 
                    return comp(data, right_value.data(), field_length) <= -1 && 
                           (comp(right_value.data(), null_value.data(), field_length) == 0 ||
                           comp(data, null_value.data(), field_length) != 0);
                if (op == ">=")
                    return comp(data, right_value.data(), field_length) >= 0 &&
                           (comp(right_value.data(), null_value.data(), field_length) == 0 ||
                           comp(data, null_value.data(), field_length) != 0);
                if (op == "<=")
                    return comp(data, right_value.data(), field_length) <= 0 &&
                           (comp(right_value.data(), null_value.data(), field_length) == 0 ||
                           comp(data, null_value.data(), field_length) != 0);
                if (op == "!=") 
                    return comp(data, right_value.data(), field_length) != 0 &&
                           (comp(right_value.data(), null_value.data(), field_length) == 0 ||
                           comp(data, null_value.data(), field_length) != 0);
                assert(0);
                return 0;
        });

        return rids;
    }

    // parse simple condition
    // returns 0 if condition is always false
    // returns 1 if condition is always true
    // else
    // returns 2 if parse succeed
    //     uint64 is left field id, string is op, string is right value
    // returns >= 3 if parse failed
    std::tuple<int, uint64, std::string, std::string> parseSimpleCondition(
        QueryProcess::SimpleCondition& condition, const DBFields& fields_desc) {
        // TODO: conditon is now very strict
        // left_expr must be field name
        // right_expr and op mustn't be empty
        // right_expr mustn't be field name

        // convert "not(\blank)*null" to "not null", "null" to "null", "is" to "is"
        if (std::regex_match(condition.left_expr, std::regex("((not)(\\s+))?(null)", std::regex_constants::icase)))
            condition.left_expr = condition.left_expr.length() != 4? "not null": "null";
        if (std::regex_match(condition.right_expr, std::regex("((not)(\\s+))?(null)", std::regex_constants::icase)))
            condition.right_expr = condition.right_expr.length() != 4? "not null": "null";
        if (std::regex_match(condition.op, std::regex("(is)", std::regex_constants::icase)))
            condition.op = "is";

        // check field name in left expr
        auto ite = std::find(fields_desc.field_name().begin(),
                             fields_desc.field_name().end(),
                             condition.left_expr);
        // invalid field name
        if (ite == fields_desc.field_name().end()) 
            throw InvalidExpr_WhereClause(condition.left_expr);
        uint64 left_field_id = ite - fields_desc.field_name().begin();

        std::string right_value(fields_desc.field_length()[left_field_id], '\x00');
        // check right value 
        // something to do with null/not null/is
        if (condition.op != "is" && condition.right_expr == "null") 
            return { 0, left_field_id, condition.op, right_value };
        if (condition.op != "is" && condition.right_expr == "not null") 
            return { 0, left_field_id, condition.op, right_value };
        if (condition.op == "is") {
            if (condition.right_expr == "null") {
                return { 2, left_field_id, "=", right_value };
            } else if (condition.right_expr == "not null") {
                return { 2, left_field_id, "<", right_value };
            } else 
                throw InvalidExpr_WhereClause(condition.op);
        }

        // parse right value
        std::unique_ptr<char[]> buff(new char[fields_desc.field_length()[left_field_id]]);
        memset(buff.get(), 0x00, fields_desc.field_length()[left_field_id]);
        if (literalParser(condition.right_expr, fields_desc.field_type()[left_field_id],
                          fields_desc.field_length()[left_field_id], buff.get()))
            throw InvalidExpr_WhereClause(condition.right_expr);
        right_value.assign(buff.get(), fields_desc.field_length()[left_field_id]);
         
        return { 2, left_field_id, condition.op, right_value };
    }

    DBTableManager* openTable(const std::string& table_name) {
        auto ptr = tables_inuse.find(table_name);
        if (ptr != tables_inuse.end()) return ptr->second;

        // check file exists or not
        if (!boost::filesystem::exists(db_inuse + '/' + table_name + DBTableManager::TABLE_SUFFIX) || 
            !boost::filesystem::is_regular_file(db_inuse + '/' + table_name + DBTableManager::TABLE_SUFFIX)) 
            throw TableNotExists(table_name);
            // return nullptr;

        // open table
        DBTableManager* table_manager(new DBTableManager);
        // if failed
        int rtv = table_manager->open(db_inuse + '/' + table_name);
        if (rtv) {
            delete table_manager;
            return nullptr;
        }
        // insert to open table map
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
    constexpr static int kParseFunctions = 12;
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
        &DBQuery::parseAsInsertRecordStatement,
        &DBQuery::parseAsSimpleSelectStatement
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
    QueryProcess::SimpleSelectStatementParser simpleSelectStatementParser;
#ifdef DEBUG
public:
#endif
    // database currently using
    std::string db_inuse;

    // tables currently opened
    std::unordered_map<std::string, DBTableManager*> tables_inuse;

    // literal parser
    DBFields::LiteralParser literalParser;

    // error classes:
    struct ParseFailed { };
    struct PathExists {
        std::string name;
        PathExists(const std::string& n): name(n) { }
    };
    struct DBNotExists {
        std::string name;
        DBNotExists(const std::string& n): name(n) { }
    };
    struct CreateDBFailed { 
        std::string name; 
        CreateDBFailed(const std::string& n): name(n) { }
    };
    struct RemoveFailed {
        std::string name;
        RemoveFailed(const std::string& n): name(n) { }
    };
    struct FieldNameTooLong {
        std::string name;
        uint64 length, max_length;
        FieldNameTooLong(const std::string& n, const uint64 l, const uint64 maxl): name(n), length(l), max_length(maxl) { }
    };
    struct DuplicateFieldName {
        std::string name;
        DuplicateFieldName(const std::string& n): name(n) { }
    };
    struct UnsupportedType {
        std::string type_name;
        bool is_unsigned;
        bool is_signed;
        UnsupportedType(const std::string& t, const bool isu, const bool iss): type_name(t), is_unsigned(isu), is_signed(iss) { }
    };
    struct FieldLengthRequired {
        std::string field_name;
        std::string field_type;
        FieldLengthRequired(const std::string& n, const std::string& t): field_name(n), field_type(t) { };
    };
    struct InvalidPrimaryKey {
        std::string name;
        InvalidPrimaryKey(const std::string& n): name(n) { }
    };
    struct DBNotOpened { };
    struct CreateTableFailed {
        std::string name;
        CreateTableFailed(const std::string& n): name(n) { }
    };
    struct OpenTableFailed {
        std::string name;
        OpenTableFailed(const std::string& n): name(n) { }
        virtual std::string getInfo() const { return ""; }
    };
    struct TableNotExists: OpenTableFailed {
        TableNotExists(const std::string& n): OpenTableFailed(n) { }
        virtual std::string getInfo() const { return "Table not exists. "; }
    };
    struct RemoveTableFailed {
        std::string name;
        RemoveTableFailed(const std::string& n): name(n) { }
    };
    struct InvalidFieldName {
        std::string name;
        InvalidFieldName(const std::string& n): name(n) { }
    };
    struct CreateIndexFailed {
        std::string table_name;
        std::string field_name;
        CreateIndexFailed(const std::string& tn, const std::string& fn): table_name(tn), field_name(fn) { }
    };
    struct RemoveIndexFailed {
        std::string table_name;
        std::string field_name;
        RemoveIndexFailed(const std::string& tn, const std::string& fn): table_name(tn), field_name(fn) { }
    };
    struct LiteralParseFailed {
        std::string literal;
        LiteralParseFailed(const std::string& l): literal(l) { }
    };
    struct LiteralOutofrange {
        std::string literal;
        LiteralOutofrange(const std::string& l): literal(l) { }
    };
    struct InsertRecordFailed {
        std::string table_name;
        std::vector<std::string> values;
        InsertRecordFailed(const std::string& tn, const std::vector<std::string>& vs): table_name(tn), values(vs) { }
        virtual std::string getInfo() const { return ""; }
    };
    struct WrongTupleSize: InsertRecordFailed {
        uint64 wrong_size;
        uint64 right_size;
        WrongTupleSize(const std::string& tn, const std::vector<std::string>& vs, const uint64 ws, const uint64 rs): 
            InsertRecordFailed(tn, vs), wrong_size(ws), right_size(rs) { }
        virtual std::string getInfo() const { 
            return "Expected a tuple with size equal to " + 
                   std::to_string(right_size) + ", " + 
                   std::to_string(wrong_size) + " provided. "; 
        }
    };
    struct NotNullExpected: InsertRecordFailed {
        NotNullExpected(const std::string& tn, const std::vector<std::string>& vs):
            InsertRecordFailed(tn, vs) { }
        virtual std::string getInfo() const {
            return "Not null expected. ";
        }
    };
    struct DuplicatePrimaryKey: InsertRecordFailed {
        DuplicatePrimaryKey(const std::string& tn, const std::vector<std::string>& vs):
            InsertRecordFailed(tn, vs) { }
        virtual std::string getInfo() const {
            return "Duplicate primary key. ";
        }
    };
    struct InvalidWhereClause {
        virtual std::string getInfo() const { return ""; }
    };
    struct InvalidExpr_WhereClause: InvalidWhereClause {
        std::string expr;
        virtual std::string getInfo() const { return expr; }
        InvalidExpr_WhereClause(const std::string& l): expr(l) { }
    };


};

#endif /* DB_QUERY_H_ */
