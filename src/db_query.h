/******************************************************************************
 *  Copyright (c) 2014 Terran Lee 
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: Database
 *  Filename: db_query.h 
 *  Version: 1.0
 *  Author: Terran Lee
 *  E-mail: ltrthu@163.com
 *  Date: Dec. 05, 2014
 *  Time: 21:01:37
 *  Description: deal with a query. 
                 Call anaylyser to analyse, then compile and execute.
 *****************************************************************************/
#ifndef DB_QUERY_H_
#define DB_QUERY_H_

#include <cstdio>
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
#include "db_error.h"
#include "db_outputer.h"

class Database::DBQuery {
public:
    static constexpr char* CHECK_CONSTRAINT_SUFFIX = (char*)".chk";
    static constexpr char* REFERENCED_CONSTRAINT_SUFFIX = (char*)".refed";
    static constexpr char* REFERENCING_CONSTRAINT_SUFFIX = (char*)".refing";


    DBQuery(std::ostream& o = std::cout, std::ostream& e = std::cerr): 
        out(o), err(e) {
        temp_dir = uniquePath();
        boost::filesystem::create_directories(temp_dir);
    }

    ~DBQuery() {
        boost::filesystem::remove_all(temp_dir);
        closeDBInUse();
    }

    bool execute(const std::string& str) {
#ifdef DEBUG
        err << "----------------------------\n";
        err << "Stmt: " << (str.length() > 400? str.substr(0, 400): str) << std::endl;
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
                        // all execution error will throw exceptions
                        // this will never happed
                        assert(0);
                        return 1;
                }
            }
            // parse failed
            throw DBError::ParseFailed();
        } catch (const DBError::Error& error) {
            err << error.getInfo() << std::endl;
        }
        return 1;
    }

private:
    struct Condition {
        // 0 - constant false
        // 1 - constant true
        // 2 - right value is literal
        // 3 - right value is variant
        int type;
        uint64 left_id;
        uint64 right_id;
        std::string op;
        std::string right_literal;
        Condition(const int t, const uint64 li, const uint64 ri, const std::string& o, const std::string& rl):
            type(t), left_id(li), right_id(ri), op(o), right_literal(rl) { }
        Condition() { }
    };
    struct ComplexCondition {
        // left table name
        std::string left_name;
        // left field id
        uint64 left_id;
        std::string op;
        std::string right_name;
        uint64 right_id;
        ComplexCondition(const std::string& ln, const uint64 li, const std::string& o, const std::string& rn, const uint64 ri):
            left_name(ln), left_id(li), op(o), right_name(rn), right_id(ri) { }
        ComplexCondition reverse() const {
            std::string new_op;
            if (op == "<") new_op = ">";
            else if (op == "<=") new_op = ">=";
            else if (op == ">") new_op = "<";
            else if (op == ">=") new_op = "<=";
            else new_op = op;
            return { right_name, right_id, new_op, left_name, left_id };
        }
    };

    // intermediate result
    struct IntermediateTable {
        DBQuery& parent;
        DBTableManager* table_manager;
        int table_manager_number;
        IntermediateTable(DBQuery& p): parent(p), table_manager(nullptr), table_manager_number(-1) { }
        ~IntermediateTable() { parent.removeTempTable(table_manager_number); }
    };

    // parse as statement "CREATE DATABASE <database name>"
    // returns 0 if parse and execute succeed
    // returns 1 if parse failed
    int parseAsCreateDBStatement(const std::string& str) {
        QueryProcess::CreateDBStatement query;
        
        bool ok = boost::spirit::qi::phrase_parse(str.begin(), 
                                                  str.end(), 
                                                  createDBStatementParser, 
                                                  boost::spirit::qi::space, 
                                                  query); 
        if (ok) {
            // check whether existing file or directory with the same name
            if (boost::filesystem::exists(query.db_name)) 
                throw DBError::PathExisted(query.db_name);
            // create directory
            if (!boost::filesystem::create_directory(query.db_name)) 
                throw DBError::CreateDBFailed(query.db_name);
            return 0;
        }
        return 1;
    }
    
    // parse as statement "DROP DATABASE <database name>"
    // returns 0 if parse and execute succeed
    // returns 1 if parse failed
    int parseAsDropDBStatement(const std::string& str) {
        QueryProcess::DropDBStatement query;
        
        bool ok = boost::spirit::qi::phrase_parse(str.begin(), 
                                                  str.end(), 
                                                  dropDBStatementParser, 
                                                  boost::spirit::qi::space, 
                                                  query);
        if (ok) {
            // check whether database exists
            if (!boost::filesystem::exists(query.db_name) || 
                !boost::filesystem::is_directory(query.db_name))
                throw DBError::DBNotExists<DBError::DropDBFailed>(query.db_name);

            // if this database is opened, close it first
            if (db_inuse == query.db_name)
                closeDBInUse();
            // remove directory
            try {
                if (!boost::filesystem::remove_all(query.db_name)) 
                    throw DBError::RemoveDBFailed(query.db_name);
            } catch (...) {
                throw DBError::RemoveDBFailed(query.db_name);
            }
            return 0;
        }
        return 1;
    }

    // parse as statement "USE <database name>"
    // returns 0 if parse and execute succeed
    // returns 1 if parse failed
    int parseAsUseDBStatement(const std::string& str) {
        QueryProcess::UseDBStatement query;

        bool ok = boost::spirit::qi::phrase_parse(str.begin(), 
                                                  str.end(), 
                                                  useDBStatementParser, 
                                                  boost::spirit::qi::space, 
                                                  query);

        if (ok) {
            // check whether database exists
            if (!boost::filesystem::exists(query.db_name) || 
                !boost::filesystem::is_directory(query.db_name)) 
                throw DBError::DBNotExists<DBError::UseDBFailed>(query.db_name);
            // if a database has already been opened, close it first
            if (db_inuse.length())
                closeDBInUse();

            db_inuse = query.db_name;

            // if existing foreign key reference configuration files
            bool refed_exists = boost::filesystem::exists(db_inuse + '/' + db_inuse + REFERENCED_CONSTRAINT_SUFFIX) &&
                                boost::filesystem::is_regular_file(db_inuse + '/' + db_inuse + REFERENCED_CONSTRAINT_SUFFIX);
            bool refing_exists = boost::filesystem::exists(db_inuse + '/' + db_inuse + REFERENCING_CONSTRAINT_SUFFIX) &&
                                 boost::filesystem::is_regular_file(db_inuse + '/' + db_inuse + REFERENCING_CONSTRAINT_SUFFIX);
            assert(refed_exists == refing_exists);
            // load them
            loadForeignKeyConstraints(referenced_tables, db_inuse + '/' + db_inuse + REFERENCED_CONSTRAINT_SUFFIX);
            loadForeignKeyConstraints(referencing_tables, db_inuse + '/' + db_inuse + REFERENCING_CONSTRAINT_SUFFIX);

            return 0;
        }
        return 1;
    }

    // parse as statement "SHOW DATABASES"
    // returns 0 if parse and execute succeed
    // returns 1 if parse failed
    // this function should always execute succeed if parse succeed 
    int parseAsShowDBStatement(const std::string& str) {
        bool ok = boost::spirit::qi::phrase_parse(str.begin(), 
                                                  str.end(), 
                                                  showDBStatementParser, 
                                                  boost::spirit::qi::space
                                                  );
        if (ok) {
            for (auto f = boost::filesystem::directory_iterator("."); 
                 f != boost::filesystem::directory_iterator(); ++f) 
                if (boost::filesystem::is_directory(f->path()))
                    out << f->path().filename().string() << std::endl;

            return 0;
        }
        return 1;
    }

    // parse as statement "CREATE TABLE <table name> (+<field name>);"
    // returns 0 if parse and execute succeed
    // returns 1 if parse failed
    int parseAsCreateTableStatement(const std::string& str) {
        QueryProcess::CreateTableStatement query;

        bool ok = boost::spirit::qi::phrase_parse(str.begin(),
                                                  str.end(),
                                                  createTableStatementParser,
                                                  boost::spirit::qi::space,
                                                  query
                                                 );
        
        if (ok) {
            // check field descriptions and primary key constraint
            std::set<std::string> field_names;
            bool primary_key_exist = 0;
            // field name, field type, field length, is primary key, indexed, not null
            std::vector< std::tuple<std::string, uint64, uint64, bool, bool, bool> > field_descs;
            for (const auto& field: query.field_descs) {
                if (field.field_name.length() > DBFields::FIELD_NAME_LENGTH) 
                    throw DBError::FieldNameTooLong(field.field_name, DBFields::FIELD_NAME_LENGTH, query.table_name);
                // insert failed, there're duplicate field names
                if (field_names.insert(field.field_name).second == 0)
                    throw DBError::DuplicateFieldName<DBError::CreateTableFailed>(field.field_name, query.table_name);
                // check primary key name validity
                if (field.field_name == query.primary_key_name) 
                    primary_key_exist = 1;

                auto type_desc = DBFields::datatype_map.find(
                    std::make_tuple(field.field_type, 
                                    field.field_type_unsigned,
                                    field.field_type_signed));

                // unsupported type.
                // this is usually because of redundant "unsigned" and "signed"
                if (type_desc == DBFields::datatype_map.end()) 
                    throw DBError::UnsupportedType(field.field_type, 
                                                   field.field_type_unsigned, 
                                                   field.field_type_signed,
                                                   query.table_name);

                // field length should be explicited provided, but not provided
                if (std::get<2>(type_desc->second) == 1 && field.field_length.size() == 0)
                    throw DBError::FieldLengthRequired(field.field_name, field.field_type, query.table_name);

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
                throw DBError::InvalidPrimaryKey<DBError::CreateTableFailed>(query.primary_key_name, query.table_name);

            // construct DBFields
            DBFields dbfields;
            for (const auto& desc: field_descs) 
                dbfields.insert(std::get<1>(desc), std::get<2>(desc),
                                std::get<3>(desc), std::get<4>(desc),
                                std::get<5>(desc), std::get<0>(desc));
            dbfields.addPrimaryKey();

            // check "check constraint"
            std::vector<Condition> conditions;
            for (const auto& cond: query.check) {
                // parse condition
                auto parsed_cond = parseSimpleCondition<DBError::CreateTableFailed>(cond, dbfields, query.table_name);
                // if this condition is always false
                if (parsed_cond.type == 0) {
                    // ignore all other conditions
                    conditions.clear();
                    conditions.push_back(parsed_cond);
                    break;
                // ignore constant true condition
                // record other conditions
                } else if (parsed_cond.type != 1) 
                    conditions.push_back(parseSimpleCondition<DBError::CreateTableFailed>(cond, dbfields, query.table_name));
            }

            std::map< std::tuple< std::string, uint64>, std::tuple<std::string, uint64> > foreign_keys;
            // check foreign key constraints
            for (const auto& fk: query.foreignkeys) {
                // get referencing field id
                auto ite = std::find(dbfields.field_name().begin(), dbfields.field_name().end(), fk.field_name);
                if (ite == dbfields.field_name().end()) 
                    throw DBError::InvalidFieldName< DBError::ForeignKeyFailed<DBError::CreateTableFailed> >(fk.field_name, query.table_name);
                uint64 field_id = ite - dbfields.field_name().begin();
                // open referenced table
                DBTableManager* referenced_table = openTable(fk.foreign_table_name);
                if (!referenced_table) 
                    throw DBError::OpenTableFailed< DBError::ForeignKeyFailed<DBError::CreateTableFailed> >(fk.foreign_table_name, query.table_name);
                // get referenced field id
                auto ite1 = std::find(referenced_table->fieldsDesc().field_name().begin(), 
                                      referenced_table->fieldsDesc().field_name().end(), 
                                      fk.foreign_field_name);
                if (ite1 == referenced_table->fieldsDesc().field_name().end())
                    throw DBError::InvalidFieldName< DBError::ForeignKeyFailed<DBError::CreateTableFailed> >(fk.foreign_field_name, query.table_name);
                uint64 field_id1 = ite1 - referenced_table->fieldsDesc().field_name().begin();

                // check referenced field is primary field
                if (field_id1 != referenced_table->fieldsDesc().primary_key_field_id())
                    throw DBError::PrimaryKeyRequired(fk.foreign_table_name, fk.foreign_field_name, query.table_name);

                // check type match
                if (dbfields.field_type()[field_id] != referenced_table->fieldsDesc().field_type()[field_id1])
                    throw DBError::TypesDismatch(fk.field_name, fk.foreign_table_name, fk.foreign_field_name, query.table_name);
                
                // check length match
                if (dbfields.field_length()[field_id] != referenced_table->fieldsDesc().field_length()[field_id1])
                    throw DBError::LengthsDismatch(fk.field_name, fk.foreign_table_name, fk.foreign_field_name, query.table_name);

                // check duplicate referencing field ids
                if (foreign_keys.insert(
                    std::make_pair(std::make_tuple(query.table_name,
                                                   ite - dbfields.field_name().begin()),
                                   std::make_tuple(fk.foreign_table_name,
                                                   ite1 - referenced_table->fieldsDesc().field_name().begin()))).second == 0)
                    throw DBError::DuplicateFieldName< DBError::ForeignKeyFailed<DBError::CreateTableFailed> >(fk.field_name, query.table_name);
            }
            assert(foreign_keys.size() == query.foreignkeys.size());
            
            // an open database is required.
            if (db_inuse.length() == 0) 
                throw DBError::DBNotOpened<DBError::CreateTableFailed>(query.table_name);

            DBTableManager table_manager;
            // create table
            bool create_rtv = table_manager.create(db_inuse + '/' + query.table_name, dbfields, 
                                                   DBTableManager::DEFAULT_PAGE_SIZE);
            if (create_rtv) 
                throw DBError::CreateTableFailed(query.table_name);
            
            // save check conditions
            if (conditions.size())
                saveConditions(conditions, 
                               db_inuse + '/' + query.table_name + CHECK_CONSTRAINT_SUFFIX);

            // save foreign key constraints
            if (foreign_keys.size()) {
                for (auto f: foreign_keys) {
                    referencing_tables.emplace(std::get<0>(f.first), std::make_tuple(std::get<1>(f.first), 
                                                                                    std::get<0>(f.second), 
                                                                                    std::get<1>(f.second)));
                    referenced_tables.emplace(std::get<0>(f.second), std::make_tuple(std::get<1>(f.second),
                                                                                      std::get<0>(f.first),
                                                                                      std::get<1>(f.first)));
                }
                saveForeignKeyConstraints(referenced_tables, db_inuse + '/' + db_inuse + REFERENCED_CONSTRAINT_SUFFIX);
                saveForeignKeyConstraints(referencing_tables, db_inuse + '/' + db_inuse + REFERENCING_CONSTRAINT_SUFFIX);
            }
            
            return 0;
        }
        return 1;
    }

    // parse as statement "SHOW TABLES"
    // returns 0 if parse and execute succeed
    // returns 1 if parse failed
    int parseAsShowTablesStatement(const std::string& str) {
        bool ok = boost::spirit::qi::phrase_parse(str.begin(), 
                                                  str.end(), 
                                                  showTablesStatementParser, 
                                                  boost::spirit::qi::space
                                                  );
        if (ok) {
            // no database is opened
            if (db_inuse.length() == 0) 
                throw DBError::DBNotOpened<DBError::ShowTablesFailed>();

            for (auto f = boost::filesystem::directory_iterator(db_inuse); 
                 f != boost::filesystem::directory_iterator(); ++f) 
                if (boost::filesystem::is_regular_file(f->path()) &&
                    boost::filesystem::extension(f->path()) == DBTableManager::TABLE_SUFFIX)
                    out << f->path().stem().filename().string() << std::endl;

            return 0;
        }
        return 1;
    }

    // parse as statement "DROP TABLE <table name>"
    // returns 0 if parse and execute succeed
    // returns 1 if parse failed
    int parseAsDropTableStatement(const std::string& str) {
        QueryProcess::DropTableStatement query;
        
        bool ok = boost::spirit::qi::phrase_parse(str.begin(), 
                                                  str.end(), 
                                                  dropTableStatementParser, 
                                                  boost::spirit::qi::space, 
                                                  query);
        if (ok) {
            if (db_inuse.length() == 0) 
                throw DBError::DBNotOpened<DBError::DropTableFailed>(query.table_name);
            
            auto eqr = referenced_tables.equal_range(query.table_name);
            if (eqr.first != eqr.second) 
                throw DBError::TableReferenced(query.table_name, std::get<1>(eqr.first->second));

            // remove table file and related index file
            DBTableManager* table_manager = openTable(query.table_name);
            if (!table_manager) 
                throw DBError::OpenTableFailed<DBError::DropTableFailed>(query.table_name, query.table_name);

            bool rtv = table_manager->remove();
            if (rtv) throw DBError::RemoveTableFailed(query.table_name);
            closeTable(query.table_name);

            // remove check constraint file if exists
            std::remove((db_inuse + '/' + query.table_name + CHECK_CONSTRAINT_SUFFIX).c_str());

            // remove foreign constraints
            eqr = referencing_tables.equal_range(query.table_name);
            for (auto ite = eqr.first; ite != eqr.second;) {
                auto eqr2 = referenced_tables.equal_range(std::get<1>(ite->second));
                for (auto ite2 = eqr2.first; ite2 != eqr2.second;) 
                    if (std::get<1>(ite2->second) == query.table_name) 
                        referenced_tables.erase(ite2++);
                    else ++ite2;
                referencing_tables.erase(ite++);
            }
            if (eqr.first != eqr.second) {
                saveForeignKeyConstraints(referenced_tables, db_inuse + '/' + db_inuse + REFERENCED_CONSTRAINT_SUFFIX);
                saveForeignKeyConstraints(referencing_tables, db_inuse + '/' + db_inuse + REFERENCING_CONSTRAINT_SUFFIX);
            }
            
            return 0;
        }
        return 1;
    }

    // parse as statement "DESC[RIBE] TABLE <table name>"
    // returns 0 if parse and execute  succeed
    // returns 1 if parse failed
    int parseAsDescTableStatement(const std::string& str) {
        QueryProcess::DescTableStatement query;
        bool ok = boost::spirit::qi::phrase_parse(str.begin(), 
                                                  str.end(),
                                                  descTableStatementParser,
                                                  boost::spirit::qi::space,
                                                  query);
        if (ok) {
            if (db_inuse.length() == 0)
                throw DBError::DBNotOpened<DBError::Error>();
            
            // open table
            DBTableManager* table_manager = openTable(query.table_name);
            // open failed
            if (!table_manager) 
                throw DBError::OpenTableFailed<DBError::Error>(query.table_name);
            
            const DBFields& fields_desc = table_manager->fieldsDesc();
            
            AlignedOutputer outputer(out);
            outputer << "Name" << "Type" << "Primary Key" << "Not Null" << "Index" << AlignedOutputer::endl;
            for (std::size_t i = 0; i < fields_desc.size(); ++i) {
                // empty field name means this is a auto created primary key field
                if (fields_desc.field_name()[i].length() == 0) continue; 
                outputer << fields_desc.field_name()[i]
                         << (DBFields::datatype_name_map.at(fields_desc.field_type()[i]) +
                            (fields_desc.field_type()[i] == DBFields::TYPE_CHAR || 
                             fields_desc.field_type()[i] == DBFields::TYPE_UCHAR? 
                                "(" + std::to_string(fields_desc.field_length()[i] - 1) + ")": ""));
                
                outputer << (fields_desc.field_id()[i] == fields_desc.primary_key_field_id()) 
                         << fields_desc.notnull()[i] 
                         << fields_desc.indexed()[i]
                         << AlignedOutputer::endl;
            }
            
            return 0;
        }
        return 1;
    }

    // parse as statement "CREATE INDEX ON <table name> (<field name>)"
    // returns 0 if parse and execute  succeed
    // returns 1 if parse failed
    int parseAsCreateIndexStatement(const std::string& str) {
        QueryProcess::CreateIndexStatement query;
        bool ok = boost::spirit::qi::phrase_parse(str.begin(), 
                                                  str.end(),
                                                  createIndexStatementParser,
                                                  boost::spirit::qi::space,
                                                  query);
        if (ok) {
            if (db_inuse.length() == 0) 
                throw DBError::DBNotOpened<DBError::CreateIndexFailed>(query.table_name, query.field_name);

            DBTableManager* table_manager = openTable(query.table_name);
            // open failed
            if (!table_manager) 
                throw DBError::OpenTableFailed<DBError::CreateIndexFailed>(query.table_name, query.table_name, query.field_name);

            auto index_field_ite = std::find(
                table_manager->fieldsDesc().field_name().begin(),
                table_manager->fieldsDesc().field_name().end(),
                query.field_name);

            // invalid field name
            if (index_field_ite == table_manager->fieldsDesc().field_name().end())
                throw DBError::InvalidFieldName<DBError::CreateIndexFailed>(query.field_name, query.table_name, query.field_name);

            bool rtv = table_manager->createIndex(
                index_field_ite - table_manager->fieldsDesc().field_name().begin(),
                "Index name not supported yet");
            
            // create failed
            if (rtv) throw DBError::CreateIndexFailed(query.table_name, query.field_name);
            return 0;
        }
        return 1;
    }

    // parse as statement "DROP INDEX ON <table name> (<field name>)"
    // returns 0 if parse and execute  succeed
    // returns 1 if parse failed
    int parseAsDropIndexStatement(const std::string& str) {
        QueryProcess::DropIndexStatement query;
        bool ok = boost::spirit::qi::phrase_parse(str.begin(), 
                                                  str.end(),
                                                  dropIndexStatementParser,
                                                  boost::spirit::qi::space,
                                                  query);
        if (ok) {
            if (db_inuse.length() == 0) 
                throw DBError::DBNotOpened<DBError::DropIndexFailed>(query.table_name, query.field_name);

            DBTableManager* table_manager = openTable(query.table_name);
            // open failed
            if (!table_manager) 
                throw DBError::OpenTableFailed<DBError::DropIndexFailed>(query.table_name, query.table_name, query.field_name);

            auto index_field_ite = std::find(
                table_manager->fieldsDesc().field_name().begin(),
                table_manager->fieldsDesc().field_name().end(),
                query.field_name);

            // invalid field name
            if (index_field_ite == table_manager->fieldsDesc().field_name().end())
                throw DBError::InvalidFieldName<DBError::DropIndexFailed>(query.field_name, query.table_name, query.field_name);

            bool rtv = table_manager->removeIndex(
                index_field_ite - table_manager->fieldsDesc().field_name().begin());
            
            // remove failed
            if (rtv) throw DBError::DropIndexFailed(query.table_name, query.field_name);
            return 0;
        }
        return 1;
    }

    // parse as statement "INSERT INTO <table name> VALUES (...);"
    // returns 0 if parse and execute  succeed
    // returns 1 if parse failed
    int parseAsInsertRecordStatement(const std::string& str) {
        QueryProcess::InsertRecordStatement query;
        bool ok = boost::spirit::qi::phrase_parse(str.begin(), 
                                                  str.end(),
                                                  insertRecordStatementParser,
                                                  boost::spirit::qi::space,
                                                  query);
        if (ok) {
            // assert database is opened 
            if (db_inuse.length() == 0) 
                throw DBError::DBNotOpened<DBError::InsertRecordFailed>(query.table_name, query.value_tuples.front().value_tuple);
            
            // open table
            DBTableManager* table_manager = openTable(query.table_name);
            // open failed
            if (!table_manager) 
                throw DBError::OpenTableFailed<DBError::InsertRecordFailed>(query.table_name, query.table_name, query.value_tuples.front().value_tuple);
            
            // get fileds description
            const DBFields& fields_desc = table_manager->fieldsDesc();
            
            std::unique_ptr<char[]> buffer(new char[fields_desc.recordLength()]);
            
            // store all rids
            // if one record insert failed, remove all stored rids
            std::vector<RID> rids;
            try {
                auto ite = tables_check_constraints.find(query.table_name);
                std::vector<Condition>* check_constraint = ite == tables_check_constraints.end()? nullptr: &ite->second;

                for (const auto& value_tuple: query.value_tuples) 
                    rids.push_back(insertRecord(query.table_name, table_manager, 
                                                value_tuple.value_tuple, 
                                                buffer.get(),
                                                check_constraint));
            } catch (...) {
                for (const auto& rid: rids) 
                    assert(table_manager->removeRecord(rid) == 0);
                throw;
            }
            
            return 0;
        }
        return 1;

    }

    // parse as SELECT <table name>.<field name> [, <table name>.<field name>]* 
    //          FROM <table name> [, <table name>] [WHERE <condition>];
    // returns 0 if parse and execute  succeed
    // returns 1 if parse failed
    int parseAsComplexSelectStatement(const std::string& str) {
        QueryProcess::ComplexSelectStatement query;
        bool ok = boost::spirit::qi::phrase_parse(str.begin(), 
                                                  str.end(),
                                                  complexSelectStatementParser,
                                                  boost::spirit::qi::space,
                                                  query);
        if (ok) {
            if (db_inuse.length() == 0) 
                throw DBError::DBNotOpened<DBError::ComplexSelectFailed>(query.table_names);

            // open tables
            std::unordered_map<std::string, DBTableManager*> table_managers;
            std::unordered_map<std::string, const DBFields&> fields_descs;
            for (const auto& tn: query.table_names) {
                // open table
                DBTableManager* table_manager = openTable(tn);
                // open failed
                if (!table_manager) 
                    throw DBError::OpenTableFailed<DBError::ComplexSelectFailed>(tn, query.table_names);
                if (table_managers.emplace(tn, table_manager).second == 0)
                    throw DBError::DuplicateTableName(query.table_names, tn);

                fields_descs.emplace(tn, table_manager->fieldsDesc());
            }

            std::unordered_map< std::string, std::vector<Condition> > simple_conditions;
            for (const auto i: table_managers)
                simple_conditions.emplace(i.first, std::vector<Condition>());

            std::vector<ComplexCondition> complex_conditions;
            // check where clause
            for (const auto& cond: query.conditions) 
                // constant true condition
                if (std::regex_match(cond.left_expr.table_name, std::regex("(true)", std::regex_constants::icase)))
                    continue;
                // constant false condition
                else if (std::regex_match(cond.left_expr.table_name, std::regex("(false)", std::regex_constants::icase)))
                    for (auto& i: simple_conditions)
                        i.second.push_back(Condition(0, 0, 0, "", ""));
                // right expr is literal, convert to simple condition
                // or both exprs have the same table name
                else if (!cond.right_expr.field_name.length() || cond.left_expr.table_name == cond.right_expr.table_name) {
                    auto ite_fields_desc = fields_descs.find(cond.left_expr.table_name);
                    auto ite_simple_conds = simple_conditions.find(cond.left_expr.table_name);
                    if (ite_fields_desc == fields_descs.end()) 
                        throw DBError::InvalidConditionOperand<DBError::ComplexSelectFailed>(cond.left_expr.table_name, query.table_names); 
                    QueryProcess::SimpleCondition simple_cond;
                    simple_cond.left_expr = cond.left_expr.field_name;
                    simple_cond.op = cond.op;
                    simple_cond.right_expr = cond.right_expr.field_name.length()? 
                        cond.right_expr.field_name: cond.right_expr.table_name;
                    ite_simple_conds->second.push_back(parseSimpleCondition<DBError::ComplexSelectFailed>(simple_cond, ite_fields_desc->second, query.table_names));
                // right expr is tablename.fieldname
                } else {
                    auto ite_left_fields_desc = fields_descs.find(cond.left_expr.table_name);
                    auto ite_right_fields_desc = fields_descs.find(cond.right_expr.table_name);
                    if (ite_left_fields_desc == fields_descs.end())
                        throw DBError::InvalidConditionOperand<DBError::ComplexSelectFailed>(cond.left_expr.table_name, query.table_names); 
                    if (ite_right_fields_desc == fields_descs.end())
                        throw DBError::InvalidConditionOperand<DBError::ComplexSelectFailed>(cond.right_expr.table_name, query.table_names); 
                    uint64 left_field_id = std::find(ite_left_fields_desc->second.field_name().begin(),
                                                     ite_left_fields_desc->second.field_name().end(),
                                                     cond.left_expr.field_name) - 
                                           ite_left_fields_desc->second.field_name().begin();
                    if (left_field_id == ite_left_fields_desc->second.field_name().size())
                        throw DBError::InvalidConditionOperand<DBError::ComplexSelectFailed>(cond.left_expr.field_name, query.table_names);
                    uint64 right_field_id = std::find(ite_right_fields_desc->second.field_name().begin(),
                                                      ite_right_fields_desc->second.field_name().end(),
                                                      cond.right_expr.field_name) - 
                                            ite_right_fields_desc->second.field_name().begin();
                    if (right_field_id == ite_right_fields_desc->second.field_name().size())
                        throw DBError::InvalidConditionOperand<DBError::ComplexSelectFailed>(cond.right_expr.field_name, query.table_names);
                    if (cond.op != "=" && cond.op != "!=" && cond.op != ">" && cond.op != "<" && cond.op != ">=" && cond.op != "<=")
                        throw DBError::InvalidConditionOperator<DBError::ComplexSelectFailed>(cond.op, query.table_names);
                    if (ite_left_fields_desc->second.field_type()[left_field_id] != 
                        ite_right_fields_desc->second.field_type()[right_field_id])
                        throw DBError::InvalidConditionOperand<DBError::ComplexSelectFailed>(cond.right_expr.field_name, query.table_names);

                    complex_conditions.push_back({ cond.left_expr.table_name,
                                                   left_field_id,
                                                   cond.op,
                                                   cond.right_expr.table_name,
                                                   right_field_id });
                }
            std::unordered_map< std::string, std::vector<RID> > rids;

            // select rids meeting simple conditions
            for (const auto tm: table_managers) 
                rids.emplace(tm.first, selectRID(tm.second, simple_conditions[tm.first]));

            // intermediate result
            // ATTENTION: 
            // DONOT use vector,
            // use list instead to avoid reallocating when inserting.
            // Reallocatiing will lead to desctruct and then construct IntermediateTable
            // which isn't what we want.
            std::list<IntermediateTable> intermediates;

            std::unordered_map<std::string, DBTableManager*> temp_table_managers;

            // create temp table for each table
            for (const auto& tb: table_managers) {
                // create poitner to table manager
                intermediates.push_back(IntermediateTable(*this));
                std::tie(intermediates.back().table_manager_number,
                         intermediates.back().table_manager) = getTempTable(-1);
                auto temp_file = uniquePath(temp_dir);
                // create temp table
                intermediates.back().table_manager->create(
                    temp_file.string(), fields_descs[tb.first], 
                    DBTableManager::DEFAULT_PAGE_SIZE);
                // open temp table
                assert(intermediates.back().table_manager->open(temp_file.string()) == 0);
                // create index
                for (uint64 i = 0; i < intermediates.back().table_manager->fieldsDesc().size(); ++i)
                    if (intermediates.back().table_manager->fieldsDesc().indexed()[i] && 
                        i != intermediates.back().table_manager->fieldsDesc().primary_key_field_id())
                        assert(intermediates.back().table_manager->createIndex(i, "Index name not supported for now. "));
                // insert records
                std::unique_ptr<char[]> buff(new char[intermediates.back().table_manager->fieldsDesc().recordLength()]);
                std::vector<RID> temp_rids;
                for (const auto rid: rids[tb.first]) {
                    assert(table_managers[tb.first]->selectRecord(rid, buff.get()) == 0);
                    temp_rids.push_back(intermediates.back().table_manager->insertRecord(buff.get()));
                    assert(temp_rids.back());
                }
                rids[tb.first] = temp_rids;

                temp_table_managers.emplace(tb.first, intermediates.back().table_manager);
            }
            // now, records meeting simple conditions are all in temp table managers
            // and rids contains rid to temp table managers
            // table_managers are of no use any more


            std::vector<std::string> table_names_inorder;
            std::unordered_map<std::string, std::vector<ComplexCondition> > complex_conditions_inorder;
            for (const auto& tb: temp_table_managers) {
                table_names_inorder.push_back(tb.first);
                complex_conditions_inorder[tb.first] = std::vector<ComplexCondition>();
            }
            for (const auto& cc: complex_conditions) 
                if (std::find(table_names_inorder.begin(), table_names_inorder.end(), cc.left_name) >
                    std::find(table_names_inorder.begin(), table_names_inorder.end(), cc.right_name))
                    complex_conditions_inorder[cc.left_name].emplace_back(cc);
                else 
                    complex_conditions_inorder[cc.right_name].push_back(cc.reverse());

            // fields description for joined table
            DBFields joined_fields_desc;
            for (const auto& tn: table_names_inorder) {
                const DBFields& fields_desc = temp_table_managers[tn]->fieldsDesc();
                for (uint64 i = 0; i < fields_desc.size(); ++i) {
                    uint64 type = fields_desc.field_type()[i];
                    uint64 length = fields_desc.field_length()[i];
                    uint64 not_null = fields_desc.notnull()[i];
                    std::string field_name = tn + "." + fields_desc.field_name()[i];
                    // ignore primary key 
                    joined_fields_desc.insert(type, length - 1, 0, 0, not_null, field_name);
                }
            }
            joined_fields_desc.addPrimaryKey();
            
            // create intermediate table
            IntermediateTable intermediate(*this);
            std::tie(intermediate.table_manager_number,
                     intermediate.table_manager) = getTempTable(-1);

            auto temp_file = uniquePath(temp_dir);
            intermediate.table_manager->create(
                    temp_file.string(), joined_fields_desc, 
                    DBTableManager::DEFAULT_PAGE_SIZE);
            // open temp table
            assert(intermediate.table_manager->open(temp_file.string()) == 0);
            
            std::unique_ptr<char[]> buffer(new char[joined_fields_desc.recordLength()]);
            std::vector<RID> joined_rids;
            auto innerJoinResult = [this, &temp_table_managers, &table_names_inorder, &buffer, &intermediate, &joined_rids](const std::vector<RID>& new_record) {
                uint64 offset = 0;
                for (std::size_t i = 0; i < table_names_inorder.size(); ++i) {
                    DBTableManager* tb_mgr = temp_table_managers.at(table_names_inorder[i]);
                    tb_mgr->selectRecord(new_record.at(i), buffer.get() + offset);
                    offset += tb_mgr->fieldsDesc().recordLength();
                }

                // add primary key
                uint64 unique_number = uniqueNumber();
                buffer[offset] = '\xff';
                memcpy(buffer.get() + offset + 1, &unique_number, 
                    intermediate.table_manager->fieldsDesc().field_length()[
                        intermediate.table_manager->fieldsDesc().primary_key_field_id()] - 1);

                // insert record
                joined_rids.push_back(intermediate.table_manager->insertRecord(buffer.get()));
                assert(joined_rids.back());
            };

            innerJoin(table_names_inorder, temp_table_managers, complex_conditions_inorder, innerJoinResult);

            // code below is similar to that in SimpleSelect()
            DBFields new_fields_desc = intermediate.table_manager->fieldsDesc();
            
            std::vector<uint64> original_field_ids;
            std::vector<uint64> display_field_ids;
            std::vector<std::string> functions;
            for (const auto& field_name: query.field_names) 
                if (field_name.field_name.table_name == "*") {
                    if (field_name.func.length() && field_name.func != "count")
                        throw DBError::AggregateFailed<DBError::ComplexSelectFailed>(field_name.func, field_name.field_name.field_name, query.table_names);
                    if (field_name.func == "count") {
                        new_fields_desc.insert(DBFields::TYPE_UINT64, 
                                               DBFields::typeLength(DBFields::TYPE_UINT64),
                                               0, 0, 1, "count(*)");
                        display_field_ids.push_back(new_fields_desc.field_id().back());
                        functions.push_back(field_name.func);
                        original_field_ids.push_back(intermediate.table_manager->fieldsDesc().primary_key_field_id());
                    } else {
                        for (const auto field_id: intermediate.table_manager->fieldsDesc().field_id())
                            if (intermediate.table_manager->fieldsDesc().field_name()[field_id].length()) {
                                display_field_ids.push_back(field_id);
                                original_field_ids.push_back(field_id);
                                functions.push_back(std::string());
                            }
                    }
                } else {
                    auto ite = std::find(intermediate.table_manager->fieldsDesc().field_name().begin(),
                                         intermediate.table_manager->fieldsDesc().field_name().end(),
                                         std::string(field_name.field_name));
                    if (ite == intermediate.table_manager->fieldsDesc().field_name().end())
                        throw DBError::InvalidFieldName<DBError::ComplexSelectFailed>(field_name.field_name, query.table_names);
                    uint64 field_id = ite - intermediate.table_manager->fieldsDesc().field_name().begin();

                    if (field_name.func.length()) {
                        uint64 new_type, new_length, new_not_null = 0;
                        std::string new_name = field_name.func + "(" + std::string(field_name.field_name) + ")";
                        
                        if (field_name.func == "count") {
                            new_type = DBFields::TYPE_UINT64;
                            new_length = DBFields::typeLength(DBFields::TYPE_UINT64);
                            new_not_null = 1;
                        } else if (field_name.func == "max" || field_name.func == "min") {
                            new_type = intermediate.table_manager->fieldsDesc().field_type()[field_id];
                            new_length = intermediate.table_manager->fieldsDesc().field_length()[field_id] - 1;
                        } else if (field_name.func == "sum") {
                            if (intermediate.table_manager->fieldsDesc().field_type()[field_id] == DBFields::TYPE_FLOAT ||
                                intermediate.table_manager->fieldsDesc().field_type()[field_id] == DBFields::TYPE_DOUBLE)
                                new_type = DBFields::TYPE_UINT64,
                                new_length = DBFields::typeLength(DBFields::TYPE_DOUBLE);
                            else 
                                new_type = DBFields::TYPE_INT64;
                                new_length = DBFields::typeLength(DBFields::TYPE_INT64);
                        } else if (field_name.func == "avg") {
                            new_type = DBFields::TYPE_DOUBLE;
                            new_length = DBFields::typeLength(DBFields::TYPE_DOUBLE);
                        } else assert(0);

                        new_fields_desc.insert(new_type, new_length, 0, 0, new_not_null, new_name);
                        display_field_ids.push_back(new_fields_desc.field_id().back());
                    } else {
                        display_field_ids.push_back(field_id);
                    }
                    original_field_ids.push_back(field_id);
                    functions.push_back(field_name.func);
                }
            
            IntermediateTable group_by_intermediate(*this);

            // group by 
            // or aggregate function(s) without group by
            if (std::string(query.group_by_field_name).length() || 
                new_fields_desc.size() > intermediate.table_manager->fieldsDesc().size()) {
                joined_rids = groupBy<DBError::ComplexSelectFailed>
                    (std::string(query.group_by_field_name), 
                     group_by_intermediate, new_fields_desc, 
                     intermediate.table_manager->fieldsDesc(), 
                     original_field_ids, display_field_ids, 
                     functions, intermediate.table_manager, joined_rids,
                     query.table_names);
            }

            // order by
            if (std::string(query.order_by.field_name).length()) {
                auto ite = std::find(new_fields_desc.field_name().begin(),
                                     new_fields_desc.field_name().end(),
                                     std::string(query.order_by.field_name));
                if (ite == new_fields_desc.field_name().end())
                    throw DBError::InvalidFieldName<DBError::ComplexSelectFailed>(query.order_by.field_name, query.table_names);
                sortRID(group_by_intermediate.table_manager? 
                            group_by_intermediate.table_manager: intermediate.table_manager,
                        joined_rids, ite - new_fields_desc.field_name().begin(),
                        query.order_by.order == "" || query.order_by.order == "asc");
            }
            
            // output result
            outputRID((group_by_intermediate.table_manager?
                          group_by_intermediate.table_manager: intermediate.table_manager),
                      display_field_ids, joined_rids);

            return 0;
        }
        return 1;
    }
 

    // parse as statement "SELECT <field name> [, <field name>]* FROM <table name> [WHERE <condition>];"
    //                 or "SELECT * FROM <table name> [WHERE <condition>];"
    // returns 0 if parse and execute  succeed
    // returns 1 if parse failed
    int parseAsSimpleSelectStatement(const std::string& str) {
        QueryProcess::SimpleSelectStatement query;
        bool ok = boost::spirit::qi::phrase_parse(str.begin(), 
                                                  str.end(),
                                                  simpleSelectStatementParser,
                                                  boost::spirit::qi::space,
                                                  query);
        if (ok) {
            if (db_inuse.length() == 0) 
                throw DBError::DBNotOpened<DBError::SimpleSelectFailed>(query.table_name);

            DBTableManager* table_manager = openTable(query.table_name);
            // open failed
            if (!table_manager) 
                throw DBError::OpenTableFailed<DBError::SimpleSelectFailed>(query.table_name, query.table_name);

            const DBFields& fields_desc = table_manager->fieldsDesc();
            // used for internmediate result
            DBFields new_fields_desc = fields_desc;
            for (std::size_t i = 0; i < new_fields_desc.indexed().size(); ++i)
                if (i != new_fields_desc.primary_key_field_id())
                    new_fields_desc.indexed()[i] = false;

            // check field names
            // id of fields aggregation function really applied to.
            // won't expand even if there's aggregation
            std::vector<uint64> original_field_ids;
            // id of fields to display, expand if there's aggregations
            std::vector<uint64> display_field_ids;
            std::vector<std::string> functions;
            for (const auto& field_name: query.field_names) {
                if (field_name.field_name == "*") { 
                    // no aggregate functions can be applied to * except 'count'
                    if (field_name.func.length() && field_name.func != "count")
                        throw DBError::AggregateFailed<DBError::SimpleSelectFailed>(field_name.func, field_name.field_name, query.table_name);
                    if (field_name.func == "count") {
                        new_fields_desc.insert(DBFields::TYPE_UINT64, 
                                              DBFields::typeLength(DBFields::TYPE_UINT64),
                                              0, 0, 1, "count(*)");
                        display_field_ids.push_back(new_fields_desc.field_id().back());
                        functions.push_back(field_name.func);
                        original_field_ids.push_back(fields_desc.primary_key_field_id());
                    } else {
                        for (const auto field_id: fields_desc.field_id()) 
                            if (fields_desc.field_name()[field_id].length()) {
                                display_field_ids.push_back(field_id);
                                original_field_ids.push_back(field_id);
                                functions.push_back(std::string());
                            }
                    }
                } else {
                    auto ite = std::find(fields_desc.field_name().begin(),
                                         fields_desc.field_name().end(),
                                         field_name.field_name);
                    // invalid field name
                    if (ite == fields_desc.field_name().end()) 
                        throw DBError::InvalidFieldName<DBError::SimpleSelectFailed>(field_name.field_name, query.table_name);
                    uint64 field_id = ite - fields_desc.field_name().begin();
                    
                    // if there's a aggregate function 
                    if (field_name.func.length()) {
                        uint64 new_type, new_length, new_not_null = 0;
                        std::string new_name = field_name.func + "(" + field_name.field_name + ")";

                        // use uint64 for count
                        if (field_name.func == "count") {
                            new_type = DBFields::TYPE_UINT64;
                            new_length = DBFields::typeLength(DBFields::TYPE_UINT64);
                            new_not_null = 1;
                        // use original type for max and min
                        } else if (field_name.func == "max" || field_name.func == "min") {
                            new_type = fields_desc.field_type()[field_id];
                            new_length = fields_desc.field_length()[field_id] - 1;
                        // use int64 for sum of integral type 
                        // double for sum of floating point type
                        } else if (field_name.func == "sum") {
                            if (fields_desc.field_type()[field_id] == DBFields::TYPE_FLOAT ||
                                fields_desc.field_type()[field_id] == DBFields::TYPE_DOUBLE)
                                new_type = DBFields::TYPE_DOUBLE,
                                new_length = DBFields::typeLength(DBFields::TYPE_DOUBLE);
                            else 
                                new_type = DBFields::TYPE_INT64,
                                new_length = DBFields::typeLength(DBFields::TYPE_INT64);
                        // use double for average
                        } else if (field_name.func == "avg") {
                            new_type = DBFields::TYPE_DOUBLE;
                            new_length = DBFields::typeLength(DBFields::TYPE_DOUBLE);
                        // other aggregate funtions not supported
                        } else assert(0);
                        
                        new_fields_desc.insert(new_type, new_length, 0, 0, new_not_null, new_name);

                        display_field_ids.push_back(new_fields_desc.field_id().back());
                    } else {
                        display_field_ids.push_back(field_id);
                    }
                    original_field_ids.push_back(field_id);
                    functions.push_back(field_name.func);
                }
            }

            // check where clause
            std::vector<Condition> conditions;
            for (const auto& cond: query.conditions)
                conditions.push_back(parseSimpleCondition<DBError::SimpleSelectFailed>(cond, fields_desc, query.table_name));

            // select records
            auto rids = selectRID(table_manager, conditions);

            
            IntermediateTable intermediate(*this);
            
            
            // group by 
            // or aggregate function(s) without group by
            if (query.group_by_field_name.length() || new_fields_desc.size() > fields_desc.size()) {
                rids = groupBy<DBError::SimpleSelectFailed>
                    (query.group_by_field_name, intermediate, new_fields_desc, 
                     fields_desc, original_field_ids, display_field_ids, 
                     functions, table_manager, rids, query.table_name);
            }

            // order by
            if (query.order_by.field_name.length()) {
                auto ite = std::find(new_fields_desc.field_name().begin(),
                                     new_fields_desc.field_name().end(),
                                     query.order_by.field_name);
                if (ite == new_fields_desc.field_name().end())
                    throw DBError::InvalidFieldName<DBError::SimpleSelectFailed>(query.order_by.field_name, query.table_name);
                sortRID(intermediate.table_manager? intermediate.table_manager: table_manager,
                        rids, ite - new_fields_desc.field_name().begin(),
                        query.order_by.order == "" || query.order_by.order == "asc");
            }

            outputRID(intermediate.table_manager? intermediate.table_manager: table_manager, display_field_ids, rids);
            return 0;
        }
        return 1;
    }

    // parse as statement "DELETE FROM <table name> [WHERE <condition>];"
    // returns 0 if parse and execute  succeed
    // returns 1 if parse failed
    int parseAsDeleteStatement(const std::string& str) {
        QueryProcess::DeleteStatement query;
        bool ok = boost::spirit::qi::phrase_parse(str.begin(), 
                                                  str.end(),
                                                  deleteStatementParser,
                                                  boost::spirit::qi::space,
                                                  query);
        if (ok) {
            if (db_inuse.length() == 0) 
                throw DBError::DBNotOpened<DBError::DeleteRecordFailed>(query.table_name);

            DBTableManager* table_manager = openTable(query.table_name);
            // open failed
            if (!table_manager) 
                throw DBError::OpenTableFailed<DBError::DeleteRecordFailed>(query.table_name, query.table_name);

            const DBFields& fields_desc = table_manager->fieldsDesc();

            // check where clause
            std::vector<Condition> conditions;
            for (const auto& cond: query.conditions)
                conditions.push_back(parseSimpleCondition<DBError::DeleteRecordFailed>(cond, fields_desc, query.table_name));

            // select records
            auto rids = selectRID(table_manager, conditions);
                

            // check foreign key constraint
            std::unique_ptr<char[]> record_buff(new char[fields_desc.recordLength()]);
            for (const auto rid: rids) {
                assert(table_manager->selectRecord(rid, record_buff.get()) == 0);
                auto eqr = referenced_tables.equal_range(query.table_name);
                for (auto ite = eqr.first; ite != eqr.second; ++ite) {
                    DBTableManager* foreign_table_manager = openTable(std::get<1>(ite->second));
                    assert(foreign_table_manager);
                    assert(fields_desc.primary_key_field_id() == std::get<0>(ite->second));
                    Condition cond(2, std::get<2>(ite->second),
                                   std::numeric_limits<uint64>::max(), "=",
                                   std::string(record_buff.get() + fields_desc.offset()[std::get<0>(ite->second)],
                                               fields_desc.field_length()[std::get<0>(ite->second)]));
                    if (selectRID(foreign_table_manager, std::vector<Condition>(1, cond)).size())
                        throw DBError::RecordReferenced<DBError::DeleteRecordFailed>(std::get<1>(ite->second), query.table_name);
                }
            }

            // remove rids
            for (const auto& rid: rids)
                assert(table_manager->removeRecord(rid) == 0);

            return 0;
        }
        return 1;
    }

    // parse as statement "UPDATE <table name> SET <field name> = <new value> 
    //                                          [, <field name> = <new value>]* 
    //                     [WHERE <condition>];"
    // returns 0 if parse and execute  succeed
    // returns 1 if parse failed
    int parseAsUpdateStatement(const std::string& str) {
        QueryProcess::UpdateStatement query;
        bool ok = boost::spirit::qi::phrase_parse(str.begin(), 
                                                  str.end(),
                                                  updateStatementParser,
                                                  boost::spirit::qi::space,
                                                  query);
        if (ok) {
            if (db_inuse.length() == 0) 
                throw DBError::DBNotOpened<DBError::UpdateRecordFailed>(query.table_name);

            DBTableManager* table_manager = openTable(query.table_name);
            // open failed
            if (!table_manager) 
                throw DBError::OpenTableFailed<DBError::UpdateRecordFailed>(query.table_name, query.table_name);

            const DBFields& fields_desc = table_manager->fieldsDesc();
            // check new values
            std::vector<uint64> modify_field_ids;
            std::unique_ptr<char[]> buffer(new char[fields_desc.recordLength()]);
            memset(buffer.get(), 0x00, fields_desc.recordLength());
            std::vector<void*> args;
            std::set<uint64> check_duplicate_field_id;
            for (const auto& new_value: query.new_values) {
                auto ite = std::find(fields_desc.field_name().begin(),
                                     fields_desc.field_name().end(),
                                     new_value.field_name);
                // invalid field name
                if (ite == fields_desc.field_name().end()) 
                    throw DBError::InvalidFieldName<DBError::UpdateRecordFailed>(new_value.field_name, query.table_name);
                uint64 field_id = ite - fields_desc.field_name().begin();
                modify_field_ids.push_back(field_id);
                check_duplicate_field_id.insert(field_id);

                // parse new value
                int rtv = literalParser(new_value.value,
                                        fields_desc.field_type()[field_id],
                                        fields_desc.field_length()[field_id],
                                        buffer.get() + fields_desc.offset()[field_id]
                                       );
                // parse failed
                if (rtv == 1) 
                    throw DBError::LiteralParseFailed<DBError::UpdateRecordFailed>(new_value.value, query.table_name);
                if (rtv == 2) 
                    throw DBError::LiteralOutOfRange<DBError::UpdateRecordFailed>(new_value.value, query.table_name);
                args.push_back(buffer.get() + fields_desc.offset()[field_id]);
            }
            // duplicate field_name
            if (modify_field_ids.size() != check_duplicate_field_id.size()) 
                throw DBError::DuplicateFieldName<DBError::UpdateRecordFailed>("", query.table_name);

            // check where clause
            std::vector<Condition> conditions;
            for (const auto& cond: query.conditions)
                conditions.push_back(parseSimpleCondition<DBError::UpdateRecordFailed>(cond, fields_desc, query.table_name));

            // select records
            auto rids = selectRID(table_manager, conditions);

            
            // if there's check constraint in this table
            // read the record to be updated
            auto ite = tables_check_constraints.find(query.table_name);
            std::unique_ptr<char[]> record_buff;
            if (ite != tables_check_constraints.end()) 
                record_buff.reset(new char[fields_desc.recordLength()]);

            // check foreign key constraints, referencing other tables
            auto eqr = referencing_tables.equal_range(query.table_name);
            for (auto ite = eqr.first; ite != eqr.second; ++ite) {
                // if constraint field is updated
                auto ite2 = std::find(modify_field_ids.begin(), modify_field_ids.end(), 
                                      std::get<0>(ite->second));
                if (ite2 != modify_field_ids.end()) {
                    // null is permitted
                    if (pointer_convert<const char*>(args[ite2 - modify_field_ids.begin()])[0] == '\x00')
                        continue;
                    DBTableManager* foreign_table_manager = openTable(std::get<1>(ite->second));
                    assert(foreign_table_manager);
                    Condition cond(2, std::get<2>(ite->second),
                                   std::numeric_limits<uint64>::max(), "=",
                                   std::string(pointer_convert<const char*>(args[ite2 - modify_field_ids.begin()]),
                                               fields_desc.field_length()[*ite2]));
                    if (selectRID(foreign_table_manager, std::vector<Condition>(1, cond)).size() == 0)
                        throw DBError::ReferencedNotExists<DBError::UpdateRecordFailed>(
                            query.new_values[ite2 - modify_field_ids.begin()].value, 
                            std::get<1>(ite->second), query.table_name);
                }
            }

            // check foreign key constraint, referenced by other tables
            // only check when primary key is to be modified
            std::size_t primary_field_id = std::find(
                modify_field_ids.begin(), modify_field_ids.end(),
                fields_desc.primary_key_field_id()) - modify_field_ids.begin();
            if (primary_field_id != modify_field_ids.size()) {
                DBFields::Comparator comp;
                comp.type = fields_desc.field_type()[fields_desc.primary_key_field_id()];
                std::unique_ptr<char[]> record_buff2(new char[fields_desc.recordLength()]);
                // read each record
                for (const auto rid: rids) {
                    assert(table_manager->selectRecord(rid, record_buff2.get()) == 0);
                    // if primary key won't be modified
                    if (comp(record_buff2.get() + fields_desc.offset()[fields_desc.primary_key_field_id()],
                             args[primary_field_id], 
                             fields_desc.field_length()[fields_desc.primary_key_field_id()]) == 0)
                        continue;
                    
                    // check whether this record is referenced by other tables
                    auto eqr = referenced_tables.equal_range(query.table_name);
                    for (auto ite = eqr.first; ite != eqr.second; ++ite) {
                        DBTableManager* foreign_table_manager = openTable(std::get<1>(ite->second));
                        assert(foreign_table_manager);
                        assert(fields_desc.primary_key_field_id() == std::get<0>(ite->second));
                        Condition cond(2, std::get<2>(ite->second),
                                       std::numeric_limits<uint64>::max(), "=",
                                       std::string(record_buff2.get() + fields_desc.offset()[std::get<0>(ite->second)],
                                                   fields_desc.field_length()[std::get<0>(ite->second)]));
                        if (selectRID(foreign_table_manager, std::vector<Condition>(1, cond)).size())
                            throw DBError::RecordReferenced<DBError::UpdateRecordFailed>(std::get<1>(ite->second), query.table_name);
                    }
                }
            }

            std::unique_ptr<char[]> old_args_buffer(new char[fields_desc.recordLength() * rids.size()]);
            memset(old_args_buffer.get(), 0x00, fields_desc.recordLength() * rids.size());
            // record operations, roll back if error occurs
            std::vector< std::tuple<RID, uint64, void*> > rollback_info;
            try {
                for (std::size_t i = 0; i < rids.size(); ++i) {
                    // check constraint
                    if (record_buff.get()) {
                        int rtv = table_manager->selectRecord(rids[i], record_buff.get());
                        assert(rtv == 0);
                        for (std::size_t j = 0; j < modify_field_ids.size(); ++j)
                            memcpy(record_buff.get() + fields_desc.offset()[modify_field_ids[j]], 
                                   args[j], fields_desc.field_length()[modify_field_ids[j]]);
                        if (!meetConditions(record_buff.get(), ite->second, table_manager)) 
                            throw DBError::CheckConstraintFailed<DBError::UpdateRecordFailed>(query.table_name);
                    }

                    for (std::size_t j = 0; j < modify_field_ids.size(); ++j) {
                        char* old_arg_pos = old_args_buffer.get() + i * fields_desc.recordLength() + fields_desc.offset()[modify_field_ids[j]];
                        int rtv = table_manager->modifyRecord(rids[i], modify_field_ids[j], args[j], old_arg_pos);
                        // modify succeed
                        if (rtv == 0) 
                            rollback_info.push_back({ rids[i], modify_field_ids[j], old_arg_pos });
                        // error occurs
                        else {
                            if (rtv == 2) 
                                throw DBError::NotNullExpected<DBError::UpdateRecordFailed>(query.table_name);
                            if (rtv == 3) 
                                throw DBError::DuplicatePrimaryKey<DBError::UpdateRecordFailed>(query.table_name);
                            throw DBError::UpdateRecordFailed(query.table_name);
                        }
                    }
                }
            } catch (...) {
                // roll back
                while (rollback_info.size()) {
                   int rtv = table_manager->modifyRecord(std::get<0>(rollback_info.back()), 
                                                         std::get<1>(rollback_info.back()),
                                                         std::get<2>(rollback_info.back()),
                                                         nullptr);
                   // assert roll back always succeed
                   assert(rtv == 0);
                   rollback_info.pop_back();
                }
                // re-throw the exception
                throw;
            }
            
            return 0;
        }
        return 1;
    }

private: 
    // output a certain record
    void outputRID(const DBTableManager* table_manager,
                   const std::vector<uint64>& display_field_ids,
                   const std::vector<RID>& rids) const {
        const DBFields& fields_desc = table_manager->fieldsDesc();
        std::unique_ptr<char[]> buff(new char[fields_desc.recordLength()]);

        AlignedOutputer outputer(out);

        std::string output_buff;
        for (const auto rid: rids) {
            table_manager->selectRecord(rid, buff.get());
            for (const auto id: display_field_ids) {
                literalParser(buff.get() + fields_desc.offset()[id],
                              fields_desc.field_type()[id],
                              fields_desc.field_length()[id],
                              output_buff);
                outputer << output_buff;
            }
            outputer << AlignedOutputer::endl;
        }
    }


    // group by field_id
    // assert rids is sorted
    std::vector<std::vector<RID>::const_iterator> grouping(
        const DBTableManager* table_manager,
        const std::vector<RID>& rids, const uint64 field_id) {
        std::vector<std::vector<RID>::const_iterator> groups;
        if (!rids.size()) return groups;

        const DBFields& fields_desc = table_manager->fieldsDesc();

        DBFields::Comparator comp;
        comp.type = fields_desc.field_type()[field_id];
        
        std::unique_ptr<char[]> buff1(new char[fields_desc.recordLength()]);
        std::unique_ptr<char[]> buff2(new char[fields_desc.recordLength()]);

        // equal(0) or not(1)
        auto comp_rule = [&table_manager, &fields_desc, &field_id, &comp, &buff1, &buff2]
            (const RID rid1, const RID rid2)->bool {
            table_manager->selectRecord(rid1, buff1.get());
            table_manager->selectRecord(rid2, buff2.get());
            int comp_result = comp(buff1.get() + fields_desc.offset()[field_id],
                                   buff2.get() + fields_desc.offset()[field_id],
                                   fields_desc.field_length()[field_id]);
            return comp_result == 0;
        };
        
        groups.push_back(rids.begin());
        for (auto ite = rids.begin() + 1; ite != rids.end(); ++ite) 
            if (!comp_rule(*ite, *(ite - 1)))
                groups.push_back(ite);

        return groups;
    }

    // sort rids in asc(1) | desc(0) order
    void sortRID(const DBTableManager* table_manager,
                 std::vector<RID>& rids, const uint64 field_id, bool order) {
        const DBFields& fields_desc = table_manager->fieldsDesc();

        DBFields::Comparator comp;
        comp.type = fields_desc.field_type()[field_id];
        
        std::unique_ptr<char[]> buff1(new char[fields_desc.recordLength()]);
        std::unique_ptr<char[]> buff2(new char[fields_desc.recordLength()]);

        auto comp_rule = [&table_manager, &fields_desc, &field_id, &comp, &buff1, &buff2, &order]
            (const RID rid1, const RID rid2)->bool {
            table_manager->selectRecord(rid1, buff1.get());
            table_manager->selectRecord(rid2, buff2.get());
            int comp_result = comp(buff1.get() + fields_desc.offset()[field_id],
                                   buff2.get() + fields_desc.offset()[field_id],
                                   fields_desc.field_length()[field_id]);
            return order? comp_result < 0: comp_result > 0;
        };
        std::sort(rids.begin(), rids.end(), comp_rule);
    }
    
    template <class CALLBACK>
    void innerJoin(const std::vector<std::string>& table_names,
                   const std::unordered_map<std::string, DBTableManager*>& table_managers,
                   const std::unordered_map<std::string, std::vector<ComplexCondition> >& conditions,
                   CALLBACK callback) const {
        uint64 max_length = 0;
        for (const auto tm: table_managers)
            max_length = std::max(max_length, tm.second->fieldsDesc().recordLength());
        std::unique_ptr<char[]> buffer(new char[max_length]);
        std::vector<RID> new_record;
        innerJoin_assis(table_names, table_managers, conditions, new_record, buffer.get(), callback);
    }


    template <class CALLBACK>
    void innerJoin_assis(const std::vector<std::string>& table_names,
                         const std::unordered_map<std::string, DBTableManager*>& table_managers,
                         const std::unordered_map<std::string, std::vector<ComplexCondition> >& conditions,
                         std::vector<RID>& new_record,
                         char* buffer,
                         CALLBACK callback) const {
        if (new_record.size() < table_names.size()) {
            std::string table_name = table_names[new_record.size()];
            // construct conditions
            std::vector<Condition> local_conds;
            for (const auto& cond: conditions.at(table_name)) {
                assert(cond.left_name == table_name);
                DBTableManager* table_manager = table_managers.at(cond.right_name);
                table_manager->selectRecord(
                    new_record[std::find(table_names.begin(), table_names.end(), cond.right_name) - table_names.begin()], 
                    buffer);
                std::string right_literal(buffer + table_manager->fieldsDesc().offset()[cond.right_id],
                                          table_manager->fieldsDesc().field_length()[cond.right_id]);
                local_conds.push_back({ 2, cond.left_id, std::numeric_limits<uint64>::max(), cond.op, right_literal });
            }
            // select rids
            auto rids = selectRID(table_managers.at(table_name), local_conds);
            // for each rid
            for (const auto rid: rids) {
                // insert into new_record 
                new_record.push_back(rid);
                // recursive 
                innerJoin_assis(table_names, table_managers, conditions, new_record, buffer, callback);
                // backtrace
                new_record.pop_back();
            }
        } else 
            // call back
            callback(new_record);
    }

    // check whether data meets all conditions
    bool meetConditions(const char* data, 
                        const std::vector<Condition>& conditions,
                        const DBTableManager* table_manager) const {
        const auto& fields_desc = table_manager->fieldsDesc();
        std::string null_value = std::string(fields_desc.recordLength(), '\x00');
        DBFields::Comparator comp;
        
        bool comp_result = true;
        for (const auto& cond: conditions) {
            // condition is constantly true or false
            if (cond.type == 1) continue;
            if (cond.type == 0) return false;

            comp.type = fields_desc.field_type()[cond.left_id];
            assert(cond.type == 2 || cond.type == 3);
            assert(!(cond.type == 3 && fields_desc.field_type()[cond.left_id] != fields_desc.field_type()[cond.right_id]));
            // process like or not like operators
            if (cond.op == "like" || cond.op == "not like") {
                std::string literal;
                bool isnull;
                literalParser(data + fields_desc.offset()[cond.left_id], 
                              fields_desc.field_type()[cond.left_id],
                              fields_desc.field_length()[cond.left_id],
                              literal, &isnull);
                // if left values is null, result is always false
                if (isnull) { comp_result &= 0; return false; }

                // match with EMACScript regex grammar, case insensive
                bool match_result = std::regex_match(literal, std::regex(cond.right_literal.substr(2, cond.right_literal.length() - 3), std::regex::icase));
                comp_result &= cond.op == "like" == match_result;
                if (!comp_result) return false;
                continue;
            }
            
            // process other operators
            const char* right_value = cond.type == 2? 
                cond.right_literal.data(): 
                data + fields_desc.offset()[cond.right_id]; 
            // comp left and right
            int tmp_result  = comp(data + fields_desc.offset()[cond.left_id],
                                   right_value,
                                   fields_desc.field_length()[cond.left_id]);
            // comp left and null
            int tmp_result2 = comp(data + fields_desc.offset()[cond.left_id],
                                   null_value.data(),
                                   fields_desc.field_length()[cond.left_id]);
            // comp right and null
            int tmp_result3 = comp(right_value,
                                   null_value.data(),
                                   fields_desc.field_length()[cond.left_id]);
// DEBUG
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wlogical-op-parentheses"
            // magic, DONOT touch
            if (cond.op == "=")
                comp_result &= tmp_result ==  0 && 
                    (tmp_result2 != 0 && tmp_result3 != 0 || cond.type == 2 && tmp_result3 == 0);
            else if (cond.op == ">")
                comp_result &= tmp_result >=  1 && 
                    (tmp_result2 != 0 && tmp_result3 != 0 || cond.type == 2 && tmp_result3 == 0);
            else if (cond.op == "<")
                comp_result &= tmp_result <= -1 && 
                    (tmp_result2 != 0 && tmp_result3 != 0 || cond.type == 2 && tmp_result3 == 0);
            else if (cond.op == ">=")
                comp_result &= tmp_result >=  0 && 
                    (tmp_result2 != 0 && tmp_result3 != 0 || cond.type == 2 && tmp_result3 == 0);
            else if (cond.op == "<=")
                comp_result &= tmp_result <=  0 && 
                    (tmp_result2 != 0 && tmp_result3 != 0 || cond.type == 2 && tmp_result3 == 0);
            else if (cond.op == "!=")
                comp_result &= tmp_result !=  0 && 
                    (tmp_result2 != 0 && tmp_result3 != 0 || cond.type == 2 && tmp_result3 == 0);
            else assert(0);
#pragma GCC diagnostic pop
            if (!comp_result) return false;
        }
        if (comp_result) return true;
        else return false;
    }

    // select rids meeting all conditions
    std::vector<RID> selectRID(const DBTableManager* table_manager,
                               const std::vector<Condition>& conditions) const {
        std::vector<RID> rids;
        // conditions with right value is literal
        std::vector<Condition> condition_right_literal;
        // conditions with right value is field id
        std::vector<Condition> condition_right_fieldID;
        // classify orig conditions
        for (const auto& cond: conditions) 
            // constant-false
            if (cond.type == 0) return rids;
            else if (cond.type == 2) condition_right_literal.push_back(cond);
            else if (cond.type == 3) condition_right_fieldID.push_back(cond);

        // no condition, means constant-true
        if (condition_right_literal.size() + condition_right_fieldID.size() == 0)
            return table_manager->findRecords(0, [](const char*) { return 1; });


        const auto& fields_desc = table_manager->fieldsDesc();
        std::string null_value = std::string(fields_desc.recordLength(), '\x00');

        // if all right values are literal and conrresponding fields are indexed
        // then find records using index, store them in set and calculate intersection
        if (condition_right_fieldID.size() == 0 && 
            std::find_if(condition_right_literal.begin(), 
                    condition_right_literal.end(),
                    [&table_manager](const Condition& cond) { 
                        return table_manager->fieldsDesc().indexed()[cond.left_id] == 0 ||
                               cond.op == "like" || 
                               cond.op == "not like";
                    }) == condition_right_literal.end()) {
            // intersected rids
            std::set<RID> intersected_rids;
            std::unique_ptr<char[]> min_value(new char[fields_desc.recordLength()]);
            bool first_loop = 1;
            for (const auto& cond: condition_right_literal) {
                // generate min value
                memset(min_value.get(), 0x00, fields_desc.recordLength());
                minGenerator(fields_desc.field_type()[cond.left_id], min_value.get(), fields_desc.field_length()[cond.left_id]);

                std::vector< std::vector<RID> > include_rids;
                std::vector< std::vector<RID> > exclude_rids;
                if (cond.op == "=") {
                    // [literal, literal]
                    include_rids.push_back(table_manager->findRecords(cond.left_id, cond.right_literal.data()));
                } else if (cond.op == ">=") {
                    // [literal, null)
                    include_rids.push_back(table_manager->findRecords(cond.left_id, cond.right_literal.data(), null_value.data()));
                } else if (cond.op == ">") {
                    // [literal, null)
                    include_rids.push_back(table_manager->findRecords(cond.left_id, cond.right_literal.data(), null_value.data()));
                    // [literal, literal]
                    exclude_rids.push_back(table_manager->findRecords(cond.left_id, cond.right_literal.data()));
                } else if (cond.op == "<=") {
                    // [min, literal)
                    include_rids.push_back(table_manager->findRecords(cond.left_id, min_value.get(), cond.right_literal.data()));
                    // [literal, literal]
                    include_rids.push_back(table_manager->findRecords(cond.left_id, cond.right_literal.data()));
                } else if (cond.op == "<") {
                    // [min, literal)
                    include_rids.push_back(table_manager->findRecords(cond.left_id, min_value.get(), cond.right_literal.data()));
                } else if (cond.op == "!=") {
                    // [min, null)
                    include_rids.push_back(table_manager->findRecords(cond.left_id, min_value.get(), null_value.data()));
                    // [literal, literal]
                    exclude_rids.push_back(table_manager->findRecords(cond.left_id, cond.right_literal.data()));
                } else assert(0);
                // all include sets
                std::set<RID> include_rids_set;
                std::size_t size_include = 0, size_exclude = 0;
                for (const auto& i: include_rids) {
                    include_rids_set.insert(i.begin(), i.end());
                    size_include += i.size();
                }
                assert(include_rids_set.size() == size_include);
                // all exclude sets
                std::set<RID> exclude_rids_set;
                for (const auto& e: exclude_rids) {
                    exclude_rids_set.insert(e.begin(), e.end());
                    size_exclude += e.size();
                }
                assert(exclude_rids_set.size() == size_exclude);
                // difference set
                std::set<RID> difference_rids;
                std::set_difference(include_rids_set.begin(), include_rids_set.end(), 
                                    exclude_rids_set.begin(), exclude_rids_set.end(),
                                    std::inserter(difference_rids, difference_rids.end()));
                assert(difference_rids.size() == size_include - size_exclude);
                // get total intersection set
                // first time in loop
                if (first_loop) {
                    intersected_rids = difference_rids;
                    first_loop = 0;
                    continue;
                }
                
                std::set<RID> tmp;
                std::set_intersection(difference_rids.begin(), difference_rids.end(),
                                      intersected_rids.begin(), intersected_rids.end(), 
                                      std::inserter(tmp, tmp.end()));
                intersected_rids = tmp;
            }
            rids.assign(intersected_rids.begin(), intersected_rids.end());
        } else {
        // else, just traverse each record
            std::vector<Condition> all_conditions;
            all_conditions.insert(all_conditions.end(), condition_right_literal.begin(), condition_right_literal.end());
            all_conditions.insert(all_conditions.end(), condition_right_fieldID.begin(), condition_right_fieldID.end());

            auto comp_rule = [this, &rids, &all_conditions, &table_manager](const char* data, const RID rid) {
                if (meetConditions(data, all_conditions, table_manager))
                    rids.push_back(rid);
            };
            table_manager->traverseRecords(comp_rule);
        }
        return rids; 
    }
    
    // parse simple condition
    // left value must be field id
    // right value is literal or fild id
    //     condition.left_expr must be field name
    //     condition.right_expr and op mustn't be empty
    //     condition.right_expr can be field name or literal
    // OR
    //     condition.left_expr is true or false
    //     condition.op and condition.right_expr is empty
    template <class ERRORTYPE, class ...ERRORINFO>
    Condition parseSimpleCondition(QueryProcess::SimpleCondition condition, 
                                   const DBFields& fields_desc,
                                   const ERRORINFO&... error_info) const {
        // convert "nOt(\blank)+nULl" to "not null", "nULl" to "null", "iS" to "is"
        // convert "Not(\black)+ lIKE" to "not like", "lIKE" to "like"
        // i.e. convert all to lower case and remove redundant blank chars
        if (std::regex_match(condition.left_expr, std::regex("((not)(\\s+))?(null)", std::regex_constants::icase)))
            condition.left_expr = condition.left_expr.length() != 4? "not null": "null";
        if (std::regex_match(condition.right_expr, std::regex("((not)(\\s+))?(null)", std::regex_constants::icase)))
            condition.right_expr = condition.right_expr.length() != 4? "not null": "null";
        if (std::regex_match(condition.op, std::regex("(is)", std::regex_constants::icase)))
            condition.op = "is";
        if (std::regex_match(condition.op, std::regex("((not)(\\s+))?(like)", std::regex_constants::icase)))
            condition.op = condition.op.length() != 4? "not like": "like";

        // convert "tRuE" to "true", "FaLse" to "false"
        if (std::regex_match(condition.left_expr, std::regex("(true)", std::regex_constants::icase)))
            condition.left_expr = "true";
        if (std::regex_match(condition.left_expr, std::regex("(false)", std::regex_constants::icase)))
            condition.left_expr = "false";
        
        if (condition.left_expr == "true" || condition.left_expr == "false") {
            if (condition.op.length())
                throw DBError::InvalidConditionOperator<ERRORTYPE>(condition.op, error_info...);
            if (condition.right_expr.length())
                throw DBError::InvalidConditionOperand<ERRORTYPE>(condition.right_expr, error_info...);
            return { condition.left_expr == "true"? 1: 0, 0, 0, "", "" };
        }

        // check field name in left expr
        auto ite = std::find(fields_desc.field_name().begin(),
                             fields_desc.field_name().end(),
                             condition.left_expr);
        // invalid field name
        if (ite == fields_desc.field_name().end()) 
            throw DBError::InvalidConditionOperand<ERRORTYPE>(condition.left_expr, error_info...);
        uint64 left_field_id = ite - fields_desc.field_name().begin();
        uint64 right_field_id = std::numeric_limits<uint64>::max();

        std::string right_value(fields_desc.field_length()[left_field_id], '\x00');
        // check right value 
        // something to do with null/not null/is
        if (condition.op != "is" && condition.right_expr == "null") 
            return { 0, left_field_id, right_field_id, condition.op, right_value };
        if (condition.op != "is" && condition.right_expr == "not null") 
            return { 0, left_field_id, right_field_id, condition.op, right_value };
        if (condition.op == "is") {
            if (condition.right_expr == "null") 
                return { 2, left_field_id, right_field_id, "=", right_value };
            else if (condition.right_expr == "not null") 
                return { 2, left_field_id, right_field_id, "<", right_value };
            else 
                throw DBError::InvalidConditionOperator<ERRORTYPE>(condition.op, error_info...);
        }

        // if operator is "like" or "not like", right value must be string literal
        if (condition.op == "like" || condition.op == "not like") {
            if (condition.right_expr.length() < 2 || 
                condition.right_expr.front() != '\'' || 
                condition.right_expr.back() != '\'')
                // throw InvalidExpr_Criteria(condition.right_expr);
                throw DBError::InvalidConditionOperand<ERRORTYPE>(condition.right_expr, error_info...);
            right_value = "\xff" + condition.right_expr; // .substr(1, condition.right_expr.length() - 2);
            return { 2, left_field_id, right_field_id, condition.op, right_value };
        }

        // try to parse right value as field name
        ite = std::find(fields_desc.field_name().begin(),
                        fields_desc.field_name().end(),
                        condition.right_expr);
        if (ite != fields_desc.field_name().end()) {
            right_field_id = ite - fields_desc.field_name().begin();
            // comparands types not match
            if (fields_desc.field_type()[left_field_id] != fields_desc.field_type()[right_field_id])
                throw DBError::InvalidConditionOperand<ERRORTYPE>(condition.right_expr, error_info...);
            return { 3, left_field_id, right_field_id, condition.op, right_value };
        }

        std::unique_ptr<char[]> buff(new char[fields_desc.field_length()[left_field_id]]);
        memset(buff.get(), 0x00, fields_desc.field_length()[left_field_id]);
        // parse right value as literal
        if (literalParser(condition.right_expr, fields_desc.field_type()[left_field_id],
                          fields_desc.field_length()[left_field_id], buff.get()))
            throw DBError::InvalidConditionOperand<ERRORTYPE>(condition.right_expr, error_info...);
        right_value.assign(buff.get(), fields_desc.field_length()[left_field_id]);
         
        return { 2, left_field_id, right_field_id, condition.op, right_value };
    }

    RID insertRecord(const std::string& table_name, DBTableManager* table_manager, 
                     const std::vector<std::string>& values, char* buffer,
                     const std::vector<Condition>* check_constraint) {
        const DBFields& fields_desc = table_manager->fieldsDesc();

        // buffer is asserted to be cleared by callee
        memset(buffer, 0x00, fields_desc.recordLength());

        // args with null flags
        std::vector<void*> args;

        // check fields size
        uint64 expected_size = fields_desc.size() - 
            (fields_desc.field_name()[fields_desc.primary_key_field_id()].length() == 0? 1: 0);
        if (values.size() != expected_size)
            throw DBError::WrongTupleSize(table_name, values, expected_size);

        for (std::size_t i = 0; i < values.size(); ++i) {
            int rtv = literalParser(values[i],
                                    fields_desc.field_type()[i],
                                    fields_desc.field_length()[i],
                                    buffer + fields_desc.offset()[i]
                                   );
            // parse failed
            if (rtv == 1) 
                throw DBError::LiteralParseFailed<DBError::InsertRecordFailed>(values[i], table_name, values);
            // out of range
            if (rtv == 2) 
                throw DBError::LiteralOutOfRange<DBError::InsertRecordFailed>(values[i], table_name, values);
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

        // check constraint
        if (check_constraint && !meetConditions(buffer, *check_constraint, table_manager)) 
            throw DBError::CheckConstraintFailed<DBError::InsertRecordFailed>(table_name, values);

        // check foreign key constraint
        auto eqr = referencing_tables.equal_range(table_name);
        for (auto ite = eqr.first; ite != eqr.second; ++ite) {
            // null value is permited
            if (pointer_convert<const char*>(args[std::get<0>(ite->second)])[0] == '\x00')
                continue;
            DBTableManager* foreign_table_manager = openTable(std::get<1>(ite->second));
            assert(foreign_table_manager);
            Condition cond(2, std::get<2>(ite->second), 
                           std::numeric_limits<uint64>::max(), "=", 
                           std::string(pointer_convert<const char*>(args[std::get<0>(ite->second)]), 
                                       fields_desc.field_length()[std::get<0>(ite->second)]));
            if (selectRID(foreign_table_manager, std::vector<Condition>(1, cond)).size() == 0)
                throw DBError::ReferencedNotExists<DBError::InsertRecordFailed>(values[std::get<0>(ite->second)], std::get<1>(ite->second), table_name, values);
        }

        auto rid = table_manager->insertRecord(args);
        // insert failed
        if (rid == RID(0, 1)) 
            throw DBError::WrongTupleSize(table_name, values, expected_size);
        else if (rid == RID(0, 3)) 
            throw DBError::NotNullExpected<DBError::InsertRecordFailed>(table_name, values);
        else if (rid == RID(0, 4))
            throw DBError::DuplicatePrimaryKey<DBError::InsertRecordFailed>(table_name, values);
        else if (!rid)
            throw DBError::InsertRecordFailed(table_name, values);
        return rid;
    }

    template <class ERRORTYPE, class ...ERRORINFO>
    // FIXME: aggregate function returns empty set of rid 
    // when rid is originally empty.
    std::vector<RID> groupBy(const std::string& group_by_field_name,
                             IntermediateTable& intermediate,
                             const DBFields& new_fields_desc, 
                             const DBFields& fields_desc,
                             const std::vector<uint64>& original_field_ids,
                             const std::vector<uint64>& display_field_ids,
                             const std::vector<std::string>& functions,
                             const DBTableManager* table_manager, 
                             std::vector<RID>& rids,
                             const ERRORINFO&... error_info) {
        // create intermediate table manager
        std::tie(intermediate.table_manager_number, intermediate.table_manager) = getTempTable(-1);
        assert(intermediate.table_manager_number >= 0);
        assert(intermediate.table_manager);
        auto temp_file = uniquePath(temp_dir);
        assert(intermediate.table_manager->create(temp_file.string(), new_fields_desc, DBTableManager::DEFAULT_PAGE_SIZE) == 0);
        assert(intermediate.table_manager->open(temp_file.string()) == 0);

        // divide into groups
        std::vector<std::vector<RID>::const_iterator> groups;

        // if group by
        if (group_by_field_name.length()) {
            auto ite = std::find(fields_desc.field_name().begin(),
                                 fields_desc.field_name().end(),
                                 group_by_field_name);
            if (ite == fields_desc.field_name().end())
                throw DBError::InvalidFieldName<ERRORTYPE>(group_by_field_name, error_info...);
            // sort 
            sortRID(table_manager, rids, ite - fields_desc.field_name().begin(), 1);

            groups = grouping(table_manager, rids, ite - fields_desc.field_name().begin());
        // else aggregate funtion(s) without group by
        // this is the same as just one group
        // except for table is empty
        } else if (rids.size())
            groups.push_back(rids.begin());

        // aggeragate
        // store record to be inserted into intermediate table
        std::unique_ptr<char[]> inter_record_buffer(new char[new_fields_desc.recordLength()]);
        std::vector<void*> inter_insert_args;
        for (const auto off: new_fields_desc.offset())
            inter_insert_args.push_back(inter_record_buffer.get() + off);
        // record intermediate rids, to replace rids later
        std::vector<RID> inter_rids;

        std::unique_ptr<char[]> aggregate_tmp(new char[fields_desc.recordLength() * rids.size()]);
        DBFields::Aggregator aggregator;
        // rids between groups[i] and groups[i + 1] is a group
        for (std::size_t i = 0; i < groups.size(); ++i) {
            // read the first in the group to result buffer
            table_manager->selectRecord(*groups[i], inter_record_buffer.get());
            std::vector<void*> args;
            // read all of this group to aggregate buffer
            for (auto ite2 = groups[i]; ite2 != (i + 1 == groups.size()? rids.end(): groups[i + 1]); ++ite2) {
                args.push_back(aggregate_tmp.get() + fields_desc.recordLength() * (ite2 - groups[i]));
                table_manager->selectRecord(*ite2, args.back());
            }
            // calculate aggregate value and save to result buffer
            // check each field to be displayed
            for (std::size_t j = 0; j < display_field_ids.size(); ++j) {
                // if there's a fucntion
                if (functions[j].size()) {
                    // paras of aggregator: pointers to all data, offset, type, length, result save to where
                    int rtv;
                    if (functions[j] == "sum") {
                        rtv = aggregator.sum(args, fields_desc.offset()[original_field_ids[j]], 
                            fields_desc.field_type()[original_field_ids[j]],
                            fields_desc.field_length()[original_field_ids[j]],
                            inter_record_buffer.get() + 
                            new_fields_desc.offset()[display_field_ids[j]]);
                    } else if (functions[j] == "avg") {
                        rtv = aggregator.avg(args, fields_desc.offset()[original_field_ids[j]], 
                            fields_desc.field_type()[original_field_ids[j]],
                            inter_record_buffer.get() + 
                            new_fields_desc.offset()[display_field_ids[j]]);
                    } else if (functions[j] == "max") {
                        rtv = aggregator.max(args, fields_desc.offset()[original_field_ids[j]], 
                            fields_desc.field_type()[original_field_ids[j]],
                            fields_desc.field_length()[original_field_ids[j]],
                            inter_record_buffer.get() + 
                            new_fields_desc.offset()[display_field_ids[j]]);
                    } else if (functions[j] == "min") {
                        rtv = aggregator.min(args, fields_desc.offset()[original_field_ids[j]], 
                            fields_desc.field_type()[original_field_ids[j]],
                            fields_desc.field_length()[original_field_ids[j]],
                            inter_record_buffer.get() + 
                            new_fields_desc.offset()[display_field_ids[j]]);
                    } else if (functions[j] == "count") {
                        rtv = aggregator.count(args, fields_desc.offset()[original_field_ids[j]],
                                               inter_record_buffer.get() + 
                                               new_fields_desc.offset()[display_field_ids[j]]);
                    } else assert(0);
                    if (rtv) 
                        throw DBError::AggregateFailed<ERRORTYPE>(functions[j], fields_desc.field_name()[original_field_ids[j]], error_info...);
                }
            }
            // insert into intermediate table
            auto inter_rid = intermediate.table_manager->insertRecord(inter_insert_args);
            assert(inter_rid);
            inter_rids.push_back(inter_rid);
        }
        return inter_rids;
    }



    // get temporary table, if k < 0, create a new table
    std::tuple<int, DBTableManager*> getTempTable(const int k) {
        if (k < 0) {
            DBTableManager* ptr = new DBTableManager;
            temp_tables.push_back(ptr);
            return { temp_tables.size() - 1, ptr };
        }
        return { k, temp_tables.at(k) };
    }

    boost::filesystem::path uniquePath(const boost::filesystem::path& dir = ".") const {
        while (true) {
            boost::filesystem::path path = boost::filesystem::unique_path("temp%%%%%%%%%%%%%%%%");
            auto temp = dir;
            if (!boost::filesystem::exists(temp /= path)) return temp;
        }
    }

    void removeTempTable(const int k) {
        if (k < 0) return;
        assert(temp_tables[k]);
        assert(temp_tables[k]->remove() == 0);
        delete temp_tables[k];
        temp_tables[k] = nullptr;
    }

    void clearTempTables() {
        for (auto& ptr: temp_tables) {
            if (!ptr) continue;
            assert(ptr->remove() == 0);
            delete ptr;
            ptr = nullptr;
        }
    }


    DBTableManager* openTable(const std::string& table_name) {
        auto ptr = tables_inuse.find(table_name);
        if (ptr != tables_inuse.end()) return ptr->second;

        // check file exists or not
        if (!boost::filesystem::exists(db_inuse + '/' + table_name + DBTableManager::TABLE_SUFFIX) || 
            !boost::filesystem::is_regular_file(db_inuse + '/' + table_name + DBTableManager::TABLE_SUFFIX)) 
            return nullptr;

        // open table
        DBTableManager* table_manager(new DBTableManager);
        // if failed
        int rtv = table_manager->open(db_inuse + '/' + table_name);
        if (rtv) {
            delete table_manager;
            return nullptr;
        }
        // insert to open table map
        tables_inuse.emplace(table_name, table_manager);
        // load constraints if exists
        if (boost::filesystem::exists(db_inuse + '/' + table_name + CHECK_CONSTRAINT_SUFFIX) &&
            boost::filesystem::is_regular_file(db_inuse + '/' + table_name + CHECK_CONSTRAINT_SUFFIX))
            tables_check_constraints.emplace(table_name, loadConditions(db_inuse + '/' + table_name + CHECK_CONSTRAINT_SUFFIX));
        return table_manager;
    }

    void closeTable(const std::string& table_name) {
        auto ptr = tables_inuse.find(table_name);
        assert(ptr!= tables_inuse.end());
        delete ptr->second;
        tables_inuse.erase(ptr);
        tables_check_constraints.erase(table_name);
    }

    void closeDBInUse() {
        for (auto& ptr: tables_inuse) 
            delete ptr.second;
        tables_inuse.clear();
        referenced_tables.clear();
        referencing_tables.clear();
        tables_check_constraints.clear();
        db_inuse.clear();
    }

    void saveConditions(const std::vector<Condition>& conditions, const std::string& filename) const {
        std::ofstream fout(filename, std::fstream::out | std::fstream::binary);
        for (const auto& cond: conditions) {
            fout.write(pointer_convert<const char*>(&cond.type), sizeof(cond.type));
            fout.write(pointer_convert<const char*>(&cond.left_id), sizeof(cond.left_id));
            fout.write(pointer_convert<const char*>(&cond.right_id), sizeof(cond.right_id));
            uint64 length = cond.op.length();
            fout.write(pointer_convert<const char*>(&length), sizeof(length));
            fout.write(cond.op.data(), length);
            length = cond.right_literal.length();
            fout.write(pointer_convert<const char*>(&length), sizeof(length));
            fout.write(cond.right_literal.data(), length);
        }
    }

    std::vector<Condition> loadConditions(const std::string& filename) const {
        std::ifstream fin(filename, std::fstream::in | std::fstream::binary);
        std::vector<Condition> conditions;
        std::string buff((std::istreambuf_iterator<char>(fin)),
                          std::istreambuf_iterator<char>());
        const char* pos = buff.data();
        while (pos != buff.data() + buff.length()) {
            Condition cond;
            cond.type = *pointer_convert<const uint64*>(pos);
            pos += sizeof(cond.type);
            cond.left_id = *pointer_convert<const uint64*>(pos);
            pos += sizeof(cond.left_id);
            cond.right_id = *pointer_convert<const uint64*>(pos);
            pos += sizeof(cond.right_id);
            uint64 length = *pointer_convert<const uint64*>(pos);
            pos += sizeof(uint64);
            cond.op.assign(pos, length);
            pos += length;
            length = *pointer_convert<const uint64*>(pos);
            pos += sizeof(uint64);
            cond.right_literal.assign(pos, length);
            pos += length;
            conditions.push_back(cond);
        }
        return conditions;
    }

    void saveForeignKeyConstraints(
        const std::unordered_multimap< std::string, std::tuple<uint64, std::string, uint64> >& tables, 
        const std::string& filename) const {
        if (!tables.size()) {
            std::remove(filename.c_str());
            return;
        }
        std::ofstream fout(filename, std::fstream::binary);
        for (const auto& r: tables) {
            uint64 length = r.first.length();
            fout.write(pointer_convert<const char*>(&length), sizeof(uint64));
            fout.write(r.first.data(), length);
            fout.write(pointer_convert<const char*>(&std::get<0>(r.second)), sizeof(uint64));
            length = std::get<1>(r.second).length();
            fout.write(pointer_convert<const char*>(&length), sizeof(uint64));
            fout.write(std::get<1>(r.second).data(), length);
            fout.write(pointer_convert<const char*>(&std::get<2>(r.second)), sizeof(uint64));
        }
    }

    void loadForeignKeyConstraints(
        std::unordered_multimap< std::string, std::tuple<uint64, std::string, uint64> >& tables, 
        const std::string& filename) const {
        std::ifstream fin(filename, std::fstream::binary);
        std::string buff((std::istreambuf_iterator<char>(fin)),
                          std::istreambuf_iterator<char>());
        const char* pos = buff.data();
        while (pos != buff.data() + buff.length()) {
            uint64 name1_length = *pointer_convert<const uint64*>(pos);
            pos += sizeof(uint64);
            std::string name1(pos, name1_length);
            pos += name1_length;
            uint64 field1_id = *pointer_convert<const uint64*>(pos);
            pos += sizeof(uint64);
            uint64 name2_length = *pointer_convert<const uint64*>(pos);
            pos += sizeof(uint64);
            std::string name2(pos, name2_length);
            pos += name2_length;
            uint64 field2_id = *pointer_convert<const uint64*>(pos);
            pos += sizeof(uint64);
            tables.emplace(name1, std::make_tuple(field1_id, name2, field2_id));
        }
    }


private:
    // member function pointers to parser action
    typedef int (DBQuery::*ParseFunctions)(const std::string&);
    constexpr static int kParseFunctions = 15;
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
        &DBQuery::parseAsSimpleSelectStatement,
        &DBQuery::parseAsDeleteStatement,
        &DBQuery::parseAsUpdateStatement,
        &DBQuery::parseAsComplexSelectStatement
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
    QueryProcess::DeleteStatementParser deleteStatementParser;
    QueryProcess::UpdateStatementParser updateStatementParser;
    QueryProcess::ComplexSelectStatementParser complexSelectStatementParser;
#ifdef DEBUG
public:
#endif
    
    // dir to save temp files
    boost::filesystem::path temp_dir;
    
    // database currently using
    std::string db_inuse;

    // tables currently opened
    std::unordered_map<std::string, DBTableManager*> tables_inuse;
    std::unordered_map< std::string, std::vector<Condition> > tables_check_constraints;
    // referencing table name -> referencing field id, referenced table name, referenced field id
    std::unordered_multimap< std::string, std::tuple<uint64, std::string, uint64> > referencing_tables;
    // referenced table name -> referenced field id, referencing table name, referenced field id
    std::unordered_multimap< std::string, std::tuple<uint64, std::string, uint64> > referenced_tables;

    // tables to save intermediate reuslt
    // tables will be removed when database is closed
    std::vector<DBTableManager*> temp_tables;

    // literal parser
    DBFields::LiteralParser literalParser;
    // min generator
    DBFields::MinGenerator minGenerator;

    // outputer
    std::ostream& out;
    std::ostream& err;

};


#endif /* DB_QUERY_H_ */
