/******************************************************************************
 *  Copyright (c) 2014 Jamis Hoo 
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: Database
 *  Filename: db_query_analyser.h 
 *  Version: 1.0
 *  Author: Jamis Hoo
 *  E-mail: hjm211324@gmail.com
 *  Date: Dec. 05, 2014
 *  Time: 20:33:43
 *  Description: Query analyser, parse a string statement.
 *****************************************************************************/
#ifndef DB_QUERY_ANALYSER_H_
#define DB_QUERY_ANALYSER_H_

#include <boost/fusion/adapted.hpp>
#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/repository/include/qi_distinct.hpp>
#include "db_common.h"

namespace Database{
namespace QueryProcess {
using namespace boost::spirit;

// structs to store match result
struct CreateDBStatement { std::string db_name; };
struct DropDBStatement { std::string db_name; };
struct UseDBStatement { std::string db_name; };
struct CreateTableStatement {
    struct FieldDesc {
        std::string field_name;
        std::string field_type;
        bool field_type_unsigned;
        bool field_type_signed;
        std::vector<uint64> field_length;
        bool field_not_null;
    };
    std::string table_name;
    std::vector<FieldDesc> field_descs;
    std::string primary_key_name;
};

// keyword symbols set
struct Keyword_symbols: qi::symbols<> {
    Keyword_symbols() {
        add("create")
           ("database")
           ("drop")
           ("show")
           ("use")
           ("databases")
           ("table")
           ("signed")
           ("unsigned")
           ("not")
           ("null")
           ("primary")
           ("key")
        ;
    }
};

// datatype symbols set
struct Datatype_symbols: qi::symbols<char, std::string> {
    Datatype_symbols() {
        add("int", "int32")
           ("smallint", "int16")
           ("tinyint", "int8")
           ("bigint", "int64")
           ("bool", "bool")
           ("float", "float")
           ("double", "double")
           ("varchar", "char")
           ("char", "char")
          ;
    }
};

// definition of datatype
const qi::rule<std::string::const_iterator, std::string(), qi::space_type> datatypes =
    repository::distinct(qi::alnum | qi::char_('_'))[qi::no_case[Datatype_symbols()]];

// definition of keywords
const qi::rule<std::string::const_iterator> keywords = 
    repository::distinct(qi::alnum | qi::char_('_'))[qi::no_case[Keyword_symbols()]];

const qi::rule<std::string::const_iterator, std::string(), qi::space_type> sql_identifier = 
    lexeme[(qi::alpha | qi::char_('_')) >> *(qi::alnum | qi::char_('_'))] - keywords - datatypes;

// parser of "CREATE DATABASE <database name>"
struct CreateDBStatementParser: qi::grammar<std::string::const_iterator, CreateDBStatement(), qi::space_type> {
    CreateDBStatementParser(): CreateDBStatementParser::base_type(start) {
        start = qi::no_case["create"] >>
                omit[no_skip[+qi::space]] >> 
                qi::no_case["database"] >> 
                omit[no_skip[+qi::space]] >> 
                sql_identifier >> 
                ';';
    }
private:
    qi::rule<std::string::const_iterator, CreateDBStatement(), qi::space_type> start;
};

// parser of "DROP DATABASE <database name>"
struct DropDBStatementParser: qi::grammar<std::string::const_iterator, DropDBStatement(), qi::space_type> {
    DropDBStatementParser(): DropDBStatementParser::base_type(start) {
        start = qi::no_case["drop"] >> 
                omit[no_skip[+qi::space]] >> 
                qi::no_case["database"] >> 
                omit[no_skip[+qi::space]] >> 
                sql_identifier >> 
                ';';
    }
private:
    qi::rule<std::string::const_iterator, DropDBStatement(), qi::space_type> start;
};

// parser of "USE DATABASE <database name>"
struct UseDBStatementParser: qi::grammar<std::string::const_iterator, UseDBStatement(), qi::space_type> {
    UseDBStatementParser(): UseDBStatementParser::base_type(start) {
        start = qi::no_case["use"] >>
                omit[no_skip[+qi::space]] >>
                sql_identifier >> 
                ';';
    }
private:
    qi::rule<std::string::const_iterator, UseDBStatement(), qi::space_type> start;
};

// parser of "SHOW DATABSES"
struct ShowDBStatementParser: qi::grammar<std::string::const_iterator, unused_type, qi::space_type> {
    ShowDBStatementParser(): ShowDBStatementParser::base_type(start) {
        start = qi::no_case["show"] >>
                omit[no_skip[+qi::space]] >>
                qi::no_case["databases"] >> 
                ';';
    }
private:
    qi::rule<std::string::const_iterator, unused_type, qi::space_type> start;
};

// parser of 
// "CREATE TABLE <table name> (<field name> <type>[(<size>)] [NOT NULL] 
//                          [, <filed name> <type>[(<size>)] [NOT NULL]]* 
//                          [, PRIMARY KEY (<field name>)]); "
struct CreateTableStatementParser: qi::grammar<std::string::const_iterator, CreateTableStatement(), qi::space_type> {
    CreateTableStatementParser(): CreateTableStatementParser::base_type(start) {
        start = 
                // create
                qi::no_case["create"] >>
                omit[no_skip[+qi::space]] >>
                // table
                qi::no_case["table"] >>
                omit[no_skip[+qi::space]] >> 
                // table name
                sql_identifier >> 
                '(' >>
                // field descriptions in parens
                (field_desc % ',') >>
                // possible primary key
                -(',' >> primary_key) >>
                ')' >>
                ';';

        field_desc = 
                     // field name
                     sql_identifier >>  
                     // data type
                     datatypes >> 
                     // possible length
                     -('(' >> ulong_long >> ')') >>
                     // possible unsigned or signed
                     qi::matches[qi::no_case["unsigned"]] >>
                     qi::matches[qi::no_case["signed"]] >>
                     // possible not null
                     qi::matches[qi::no_case["not"] >>
                                 no_skip[+qi::space] >>
                                 qi::no_case["null"]]
                     ;

        primary_key = 
                      // primary
                      qi::no_case["primary"] >>
                      omit[no_skip[+qi::space]] >>
                      // key
                      qi::no_case["key"] >>
                      '(' >>
                      // table name
                      sql_identifier >>
                      ')'
                      ;
    }

private:
    qi::rule<std::string::const_iterator, std::string(), qi::space_type> primary_key;
    qi::rule<std::string::const_iterator, CreateTableStatement::FieldDesc(), qi::space_type> field_desc;
    qi::rule<std::string::const_iterator, CreateTableStatement(), qi::space_type> start;
};

}
}

// BOOST_FUSION_ADAPT_STRUCT macro should be placed at global namespace, 
// according to the documentation.
BOOST_FUSION_ADAPT_STRUCT(::Database::QueryProcess::CreateDBStatement,
                          (std::string, db_name))
BOOST_FUSION_ADAPT_STRUCT(::Database::QueryProcess::DropDBStatement,
                          (std::string, db_name))
BOOST_FUSION_ADAPT_STRUCT(::Database::QueryProcess::UseDBStatement,
                          (std::string, db_name))
BOOST_FUSION_ADAPT_STRUCT(::Database::QueryProcess::CreateTableStatement::FieldDesc,
                          (std::string, field_name)
                          (std::string, field_type)
                          (std::vector<Database::uint64>, field_length)
                          (bool, field_type_unsigned)
                          (bool, field_type_signed)
                          (bool, field_not_null)
                         )
BOOST_FUSION_ADAPT_STRUCT(::Database::QueryProcess::CreateTableStatement,
                          (std::string, table_name)
                          (std::vector< ::Database::QueryProcess::CreateTableStatement::FieldDesc >, field_descs)
                          (std::string, primary_key_name)
                         )

#endif /* DB_QUERY_ANALYSER_H_ */
