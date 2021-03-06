/******************************************************************************
 *  Copyright (c) 2014-2015 Jamis Hoo 
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
struct SimpleCondition {
    std::string left_expr;
    std::string op;
    std::string right_expr;
};
struct FullFieldName {
    std::string table_name;
    std::string field_name;
    operator std::string() const {
        return table_name + (field_name.length()? ".": "") + field_name;
    }
};
struct ComplexCondition {
    FullFieldName left_expr;
    std::string op;
    FullFieldName right_expr;
};
struct CreateTableStatement {
    struct FieldDesc {
        std::string field_name;
        std::string field_type;
        bool field_type_unsigned;
        bool field_type_signed;
        std::vector<uint64> field_length;
        bool field_not_null;
    };
    struct ForeignKeyConstraint {
        std::string field_name;
        std::string foreign_table_name;
        std::string foreign_field_name;
    };
    std::string table_name;
    std::vector<FieldDesc> field_descs;
    std::string primary_key_name;
    std::vector<SimpleCondition> check;
    std::vector<ForeignKeyConstraint> foreignkeys;
};
struct DropTableStatement { std::string table_name; };
struct DescTableStatement { std::string table_name; };
struct CreateIndexStatement {
    std::string table_name;
    std::string field_name;
};
struct DropIndexStatement {
    std::string table_name;
    std::string field_name;

};
struct InsertRecordStatement {
    struct ValueTuple {
        std::vector<std::string> value_tuple;
    };
    std::string table_name;
    std::vector<ValueTuple> value_tuples;
};
struct SimpleSelectStatement {
    struct SelectFieldName {
        std::string func;
        std::string field_name;
    };
    std::vector<SelectFieldName> field_names;
    std::string table_name;
    std::vector<SimpleCondition> conditions;
    struct OrderByClause {
        std::string field_name;
        std::string order;
    };
    std::string group_by_field_name;
    OrderByClause order_by;
};
struct ComplexSelectStatement {
    struct SelectFieldName {
        std::string func;
        FullFieldName field_name;
    };
    std::vector<SelectFieldName> field_names;
    std::vector<std::string> table_names;
    std::vector<ComplexCondition> conditions;
    struct OrderByClause {
        FullFieldName field_name;
        std::string order;
    };
    OrderByClause order_by;
    FullFieldName group_by_field_name;
};
        
struct DeleteStatement {
    std::string table_name;
    std::vector<SimpleCondition> conditions;
};
struct UpdateStatement {
    struct NewValue {
        std::string field_name;
        std::string value;
    };
    std::string table_name;
    std::vector<NewValue> new_values;
    std::vector<SimpleCondition> conditions;
};

// keyword symbols set
struct Keyword_symbols: qi::symbols<> {
    Keyword_symbols() {
        add
           ("and")
           ("asc")
           ("avg")
           ("by")
           ("check")
           ("count")
           ("create")
           ("database")
           ("databases")
           ("delete")
           ("desc")
           ("describe")
           ("drop")
           ("false")
           ("foreign")
           ("from")
           ("group")
           ("index")
           ("insert")
           ("into")
           ("is")
           ("key")
           ("like")
           ("min")
           ("max")
           ("not")
           ("null")
           ("on")
           ("order")
           ("primary")
           ("references")
           ("select")
           ("set")
           ("show")
           ("signed")
           ("sum")
           ("table")
           ("tables")
           ("true")
           ("unsigned")
           ("update")
           ("use")
           ("values")
           ("where")
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

struct Operator: qi::symbols<char, std::string> {
    Operator() {
        add("=", "=")
           ("!=", "!=")
           ("<>", "!=")
           (">", ">")
           ("<", "<")
           (">=", ">=")
           ("<=", "<=")
           ("is", "is")
          ;
    }
};

struct Aggregate: qi::symbols<char, std::string> {
    Aggregate() {
        add("avg", "avg")
           ("count", "count")
           ("max", "max")
           ("min", "min")
           ("sum", "sum")
          ;
    }
};

// definition of datatype
const qi::rule<std::string::const_iterator, std::string()> datatypes =
    repository::distinct(qi::alnum | qi::char_('_'))[qi::no_case[Datatype_symbols()]];

// definition of keywords
const qi::rule<std::string::const_iterator> keywords = 
    repository::distinct(qi::alnum | qi::char_('_'))[qi::no_case[Keyword_symbols()]];

// definition of sql identifier
const qi::rule<std::string::const_iterator, std::string()> sql_identifier = 
    lexeme[(qi::alpha | qi::char_('_')) >> *(qi::alnum | qi::char_('_'))] - keywords - datatypes;

// definition of full field name
const qi::rule<std::string::const_iterator, FullFieldName(), qi::space_type> full_field_name = 
    lexeme[sql_identifier >> '.' >> sql_identifier];

// definition of numeric, integer and float
const qi::rule<std::string::const_iterator, std::string(), qi::space_type> sql_float  =
    lexeme[-(qi::char_("+-")) >> 
        ((+qi::digit >> -(qi::char_('.') >> *qi::digit)) | 
         (qi::char_('.') >> +qi::digit))];

// definition of string with single quotes
const qi::rule<std::string::const_iterator, std::string(), qi::space_type> sql_string  =
    lexeme[qi::char_('\'') >> *(~qi::char_("\\\'") | ('\\' >> qi::char_)) >> qi::char_('\'')];

// definition of not null and null
const qi::rule<std::string::const_iterator, std::string(), qi::space_type> sql_null = 
    qi::as_string[qi::no_case["null"]];
const qi::rule<std::string::const_iterator, std::string(), qi::space_type> sql_not = 
    qi::as_string[qi::no_case["not"]];
const qi::rule<std::string::const_iterator, std::string(), qi::space_type> sql_notnull = 
    sql_not >> qi::no_skip[+qi::space] >> sql_null;

// definition of like and not like 
const qi::rule<std::string::const_iterator, std::string(), qi::space_type> sql_like = 
    qi::as_string[qi::no_case["like"]];
const qi::rule<std::string::const_iterator, std::string(), qi::space_type> sql_notlike = 
    sql_not >> qi::no_skip[+qi::space] >> sql_like;

// definition of bool, true and false
const qi::rule<std::string::const_iterator, std::string(), qi::space_type> sql_bool = 
    qi::as_string[qi::no_case["false"]] | qi::as_string[qi::no_case["true"]];

// definition of sql operators
const qi::rule<std::string::const_iterator, std::string(), qi::space_type> sql_operators = 
    qi::no_case[Operator()] | sql_like | sql_notlike;

// definition of aggregate funtions
const qi::rule<std::string::const_iterator, std::string(), qi::space_type> sql_aggregate_functions = 
    qi::no_case[Aggregate()];

// simple condition
// left_expr operator right_expr
// OR true(false)
const qi::rule<std::string::const_iterator, SimpleCondition(), qi::space_type> simple_condition =
    // true or false
    (sql_bool >> qi::attr(std::string()) >> qi::attr(std::string())) |
    // left expr
    ((sql_identifier | sql_string | sql_float | sql_null | sql_notnull | sql_bool) >>
    // op
     sql_operators >>
    // right expr
     (sql_identifier | sql_string | sql_float | sql_null | sql_notnull | sql_bool));

// complex condition
// left_expr operator right_expr
const qi::rule<std::string::const_iterator, ComplexCondition(), qi::space_type> complex_condition =
    // true or false
    (qi::as<FullFieldName>()[sql_bool >> qi::attr(std::string())] >> qi::attr(std::string()) >> qi::attr(FullFieldName())) |
    (
     // tablename.fieldname
     full_field_name >> 
     // op
     sql_operators >> 
     // tablebane.fieldname
     (full_field_name | 
     // literal
      ((sql_float | sql_string | sql_null | sql_notnull | sql_bool) >> qi::attr(std::string()))
     )
    );

// parser of "CREATE DATABASE <database name>"
struct CreateDBStatementParser: qi::grammar<std::string::const_iterator, CreateDBStatement(), qi::space_type> {
    CreateDBStatementParser(): CreateDBStatementParser::base_type(start) {
        start = qi::no_case["create"] >>
                omit[no_skip[+qi::space]] >> 
                qi::no_case["database"] >> 
                omit[no_skip[+qi::space]] >> 
                sql_identifier >> 
                ';' >>
                qi::eoi;
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
                ';' >>
                qi::eoi;
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
                ';' >> 
                qi::eoi;
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
                -(',' >> check_constraint) >>
                *(',' >> foreign_key_constraint) >> 
                ')' >>
                ';' >> 
                qi::eoi;

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

        check_constraint = qi::no_case["check"] >> 
                          '(' >>
                          (simple_condition % (omit[no_skip[+qi::space]] >> 
                                               qi::no_case["and"] >>
                                               omit[no_skip[+qi::space]])) >>
                          ')';
        foreign_key_constraint = qi::no_case["foreign"] >>
                                 omit[no_skip[+qi::space]] >>
                                 qi::no_case["key"] >>
                                 '(' >>
                                 sql_identifier >>
                                 ')' >>
                                 qi::no_case["references"] >>
                                 omit[no_skip[+qi::space]] >>
                                 sql_identifier >> 
                                 '(' >> 
                                 sql_identifier >>
                                 ')';

    }

private:
    qi::rule<std::string::const_iterator, CreateTableStatement::ForeignKeyConstraint(), qi::space_type> foreign_key_constraint;
    qi::rule<std::string::const_iterator, std::vector<SimpleCondition>(), qi::space_type> check_constraint;
    qi::rule<std::string::const_iterator, std::string(), qi::space_type> primary_key;
    qi::rule<std::string::const_iterator, CreateTableStatement::FieldDesc(), qi::space_type> field_desc;
    qi::rule<std::string::const_iterator, CreateTableStatement(), qi::space_type> start;
};

// parser of "SHOW TABLES"
struct ShowTablesStatementParser: qi::grammar<std::string::const_iterator, unused_type, qi::space_type> {
    ShowTablesStatementParser(): ShowTablesStatementParser::base_type(start) {
        start = qi::no_case["show"] >>
                omit[no_skip[+qi::space]] >>
                qi::no_case["tables"] >> 
                ';' >>
                qi::eoi;
    }
private:
    qi::rule<std::string::const_iterator, unused_type, qi::space_type> start;
};

// parser of "DROP DATABASE <database name>"
struct DropTableStatementParser: qi::grammar<std::string::const_iterator, DropTableStatement(), qi::space_type> {
    DropTableStatementParser(): DropTableStatementParser::base_type(start) {
        start = qi::no_case["drop"] >> 
                omit[no_skip[+qi::space]] >> 
                qi::no_case["table"] >> 
                omit[no_skip[+qi::space]] >> 
                sql_identifier >> 
                ';' >>
                qi::eoi;
    }
private:
    qi::rule<std::string::const_iterator, DropTableStatement(), qi::space_type> start;
};

// parser of "DESC[RIBE] <table name>"
struct DescTableStatementParser: qi::grammar<std::string::const_iterator, DescTableStatement(), qi::space_type> {
    DescTableStatementParser(): DescTableStatementParser::base_type(start) {
        start = qi::no_case["desc"] >>
                // DESC or DESCRIBE
                -omit[no_skip[qi::no_case["ribe"]]] >>
                omit[no_skip[+qi::space]] >>
                sql_identifier >> 
                ';' >> 
                qi::eoi;
    }
private:
    qi::rule<std::string::const_iterator, DescTableStatement(), qi::space_type> start;
};

// parser of "CREATE INDEX ON <table name> ( <field name> );"
struct CreateIndexStatementParser: qi::grammar<std::string::const_iterator, CreateIndexStatement(), qi::space_type> {
    CreateIndexStatementParser(): CreateIndexStatementParser::base_type(start) {
        start = qi::no_case["create"] >>
                omit[no_skip[+qi::space]] >>
                qi::no_case["index"] >>
                omit[no_skip[+qi::space]] >>
                qi::no_case["on"] >>
                omit[no_skip[+qi::space]] >>
                sql_identifier >>
                '(' >>
                sql_identifier >>
                ')' >>
                ';' >>
                qi::eoi;
    }
private:
    qi::rule<std::string::const_iterator, CreateIndexStatement(), qi::space_type> start;
};

// parser of "DROP INDEX ON <table name> ( <field name> );"
struct DropIndexStatementParser: qi::grammar<std::string::const_iterator, DropIndexStatement(), qi::space_type> {
    DropIndexStatementParser(): DropIndexStatementParser::base_type(start) {
        start = qi::no_case["drop"] >>
                omit[no_skip[+qi::space]] >>
                qi::no_case["index"] >>
                omit[no_skip[+qi::space]] >>
                qi::no_case["on"] >>
                omit[no_skip[+qi::space]] >>
                sql_identifier >>
                '(' >>
                sql_identifier >>
                ')' >>
                ';' >>
                qi::eoi;
    }
private:
    qi::rule<std::string::const_iterator, DropIndexStatement(), qi::space_type> start;
};


// parser of "INSERT INTO <table name> VALUES (<value> [, <value>]*)
//                                          (, <value> [, <value>]*);
struct InsertRecordStatementParser: qi::grammar<std::string::const_iterator, InsertRecordStatement(), qi::space_type> {
    InsertRecordStatementParser(): InsertRecordStatementParser::base_type(start) {
        start = qi::no_case["insert"] >>
                omit[no_skip[+qi::space]] >>
                qi::no_case["into"] >>
                omit[no_skip[+qi::space]] >>
                sql_identifier >>
                qi::no_case["values"] >>
                (value_tuple % ',') >>
                ';' >> 
                qi::eoi;
        value_tuple = 
                '(' >>
                (sql_string | sql_float | sql_null | sql_bool) % ',' >>
                ')';

    }
private:
    qi::rule<std::string::const_iterator, InsertRecordStatement::ValueTuple(), qi::space_type> value_tuple;
    qi::rule<std::string::const_iterator, InsertRecordStatement(), qi::space_type> start;
};

// parser of SELECT <field name> [, <field name>]* FROM <table name> [WHERE <condition>];
//           SELECT * FROM <table name> [WHERE <condition>];
struct SimpleSelectStatementParser: qi::grammar<std::string::const_iterator, SimpleSelectStatement(), qi::space_type> {
    SimpleSelectStatementParser(): SimpleSelectStatementParser::base_type(start) {
        start = qi::no_case["select"] >>
                omit[no_skip[+qi::space]] >>
                (field_name % ',') >>
                qi::no_case["from"] >>
                omit[no_skip[+qi::space]] >>
                sql_identifier >>
                -(qi::no_case["where"] >> 
                  omit[no_skip[+qi::space]] >> 
                  (simple_condition % 
                   (omit[no_skip[+qi::space]] >> 
                    qi::no_case["and"] >> 
                    omit[no_skip[+qi::space]]))) >>
                (group_by | qi::attr(std::string())) >>
                (order_by | qi::attr(SimpleSelectStatement::OrderByClause())) >>
                ';' >>
                qi::eoi;
        
        field_name = sql_aggregate_functions >> '(' >> (sql_identifier | qi::as_string[qi::char_('*')]) >> ')' |
                     qi::attr(std::string()) >> (sql_identifier | qi::as_string[qi::char_('*')]);
        
        group_by = qi::no_case["group"] >> 
                   omit[no_skip[+qi::space]] >>
                   qi::no_case["by"] >>
                   omit[no_skip[+qi::space]] >>
                   sql_identifier;
        
        order_by = qi::no_case["order"] >>
                   omit[no_skip[+qi::space]] >>
                   qi::no_case["by"] >>
                   omit[no_skip[+qi::space]] >>
                   sql_identifier >> 
                   (qi::as_string[qi::no_case["asc"]] | qi::as_string[qi::no_case["desc"]] | qi::attr(std::string()));
        
    }
private:
    qi::rule<std::string::const_iterator, SimpleSelectStatement::SelectFieldName(), qi::space_type> field_name;
    qi::rule<std::string::const_iterator, std::string(), qi::space_type> group_by;
    qi::rule<std::string::const_iterator, SimpleSelectStatement(), qi::space_type> start;
    qi::rule<std::string::const_iterator, SimpleSelectStatement::OrderByClause(), qi::space_type> order_by;
};

// parser of SELECT <table name>.<field name> [, <table name>.<field name>]* 
//           FROM <table name> [, <table name>] [WHERE <condition>];
struct ComplexSelectStatementParser: qi::grammar<std::string::const_iterator, ComplexSelectStatement(), qi::space_type> {
    ComplexSelectStatementParser(): ComplexSelectStatementParser::base_type(start) {
        start = qi::no_case["select"] >>
                omit[no_skip[+qi::space]] >>
                (field_name % ',') >>
                qi::no_case["from"] >>
                omit[no_skip[+qi::space]] >>
                (sql_identifier % ',') >>
                
                -(qi::no_case["where"] >> 
                  omit[no_skip[+qi::space]] >> 
                  (complex_condition % 
                   (omit[no_skip[+qi::space]] >> 
                    qi::no_case["and"] >> 
                    omit[no_skip[+qi::space]]))) >>
                
                (group_by | qi::attr(FullFieldName())) >>
                (order_by | qi::attr(ComplexSelectStatement::OrderByClause())) >>
                ';' >>
                qi::eoi;
        
        field_name = sql_aggregate_functions >> '(' >> 
                        (full_field_name | qi::as<FullFieldName>()[qi::as_string['*'] >> qi::attr(std::string())]) >> 
                                                ')'
                     |
                     qi::attr(std::string()) >> 
                        (full_field_name | qi::as<FullFieldName>()[qi::as_string['*'] >> qi::attr(std::string())]);
        
        group_by = qi::no_case["group"] >> 
                   omit[no_skip[+qi::space]] >>
                   qi::no_case["by"] >>
                   omit[no_skip[+qi::space]] >>
                   full_field_name;
        
        order_by = qi::no_case["order"] >>
                   omit[no_skip[+qi::space]] >>
                   qi::no_case["by"] >>
                   omit[no_skip[+qi::space]] >>
                   full_field_name >> 
                   (qi::as_string[qi::no_case["asc"]] | qi::as_string[qi::no_case["desc"]] | qi::attr(std::string()));
        
    }
private:
    qi::rule<std::string::const_iterator, ComplexSelectStatement::SelectFieldName(), qi::space_type> field_name;
    qi::rule<std::string::const_iterator, FullFieldName(), qi::space_type> group_by;
    qi::rule<std::string::const_iterator, ComplexSelectStatement(), qi::space_type> start;
    qi::rule<std::string::const_iterator, ComplexSelectStatement::OrderByClause(), qi::space_type> order_by;
};

// parser of DELETE FROM <table name> [WHERE <conditoon>];
struct DeleteStatementParser: qi::grammar<std::string::const_iterator, DeleteStatement(), qi::space_type> {
    DeleteStatementParser(): DeleteStatementParser::base_type(start) {
        start = qi::no_case["delete"] >>
                omit[no_skip[+qi::space]] >>
                qi::no_case["from"] >>
                omit[no_skip[+qi::space]] >>
                sql_identifier >>
                -(qi::no_case["where"] >> 
                  omit[no_skip[+qi::space]] >> 
                  (simple_condition % 
                   (omit[no_skip[+qi::space]] >> 
                    qi::no_case["and"] >> 
                    omit[no_skip[+qi::space]]))) >>
                ';' >>
                qi::eoi;
    }
private:
    qi::rule<std::string::const_iterator, DeleteStatement(), qi::space_type> start;
};

// parser of UPDATE <table name> SET <field name> = <new value> 
//                                [, <field name> = <new value>]* 
//           WHERE <condition>;
struct UpdateStatementParser: qi::grammar<std::string::const_iterator, UpdateStatement(), qi::space_type> {
    UpdateStatementParser(): UpdateStatementParser::base_type(start) {
        start = qi::no_case["update"] >>
                omit[no_skip[+qi::space]] >>
                sql_identifier >>
                qi::no_case["set"] >>
                (new_value % ',') >>
                -(qi::no_case["where"] >> 
                  omit[no_skip[+qi::space]] >> 
                  (simple_condition % 
                   (omit[no_skip[+qi::space]] >> 
                    qi::no_case["and"] >> 
                    omit[no_skip[+qi::space]]))) >>
                ';' >>
                qi::eoi;
        new_value = sql_identifier >> 
                    '=' >>
                    (sql_string | sql_float | sql_null | sql_notnull | sql_bool);
    }
private:
    qi::rule<std::string::const_iterator, UpdateStatement::NewValue(), qi::space_type> new_value;
    qi::rule<std::string::const_iterator, UpdateStatement(), qi::space_type> start;
};


} // namespace QueryProcess
} // namespace Database

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
BOOST_FUSION_ADAPT_STRUCT(::Database::QueryProcess::CreateTableStatement::ForeignKeyConstraint,
                          (std::string, field_name)
                          (std::string, foreign_table_name)
                          (std::string, foreign_field_name)
                         )
BOOST_FUSION_ADAPT_STRUCT(::Database::QueryProcess::CreateTableStatement,
                          (std::string, table_name)
                          (std::vector< ::Database::QueryProcess::CreateTableStatement::FieldDesc >, field_descs)
                          (std::string, primary_key_name)
                          (std::vector< ::Database::QueryProcess::SimpleCondition >, check)
                          (std::vector< ::Database::QueryProcess::CreateTableStatement::ForeignKeyConstraint >, foreignkeys)
                         )
BOOST_FUSION_ADAPT_STRUCT(::Database::QueryProcess::DropTableStatement,
                          (std::string, table_name))
BOOST_FUSION_ADAPT_STRUCT(::Database::QueryProcess::DescTableStatement,
                          (std::string, table_name))  
BOOST_FUSION_ADAPT_STRUCT(::Database::QueryProcess::CreateIndexStatement,
                          (std::string, table_name)
                          (std::string, field_name))
BOOST_FUSION_ADAPT_STRUCT(::Database::QueryProcess::DropIndexStatement,
                          (std::string, table_name)
                          (std::string, field_name))
BOOST_FUSION_ADAPT_STRUCT(::Database::QueryProcess::InsertRecordStatement::ValueTuple,
                          (std::vector<std::string>, value_tuple))
BOOST_FUSION_ADAPT_STRUCT(::Database::QueryProcess::InsertRecordStatement,
                          (std::string, table_name)
                          (std::vector< ::Database::QueryProcess::InsertRecordStatement::ValueTuple >, value_tuples))
BOOST_FUSION_ADAPT_STRUCT(::Database::QueryProcess::SimpleCondition,
                          (std::string, left_expr)
                          (std::string, op)
                          (std::string, right_expr))
BOOST_FUSION_ADAPT_STRUCT(::Database::QueryProcess::SimpleSelectStatement::SelectFieldName,
                          (std::string, func)
                          (std::string, field_name))
BOOST_FUSION_ADAPT_STRUCT(::Database::QueryProcess::SimpleSelectStatement::OrderByClause,
                          (std::string, field_name)
                          (std::string, order))
BOOST_FUSION_ADAPT_STRUCT(::Database::QueryProcess::SimpleSelectStatement,
                          (std::vector< ::Database::QueryProcess::SimpleSelectStatement::SelectFieldName >, field_names)
                          (std::string, table_name)
                          (std::vector< ::Database::QueryProcess::SimpleCondition >, conditions)
                          (std::string, group_by_field_name)
                          (::Database::QueryProcess::SimpleSelectStatement::OrderByClause, order_by))
BOOST_FUSION_ADAPT_STRUCT(::Database::QueryProcess::FullFieldName,
                          (std::string, table_name)
                          (std::string, field_name))
BOOST_FUSION_ADAPT_STRUCT(::Database::QueryProcess::ComplexCondition,
                          (::Database::QueryProcess::FullFieldName, left_expr)
                          (std::string, op)
                          (::Database::QueryProcess::FullFieldName, right_expr))
BOOST_FUSION_ADAPT_STRUCT(::Database::QueryProcess::ComplexSelectStatement::SelectFieldName,
                          (std::string, func)
                          (::Database::QueryProcess::FullFieldName, field_name))
BOOST_FUSION_ADAPT_STRUCT(::Database::QueryProcess::ComplexSelectStatement::OrderByClause,
                          (::Database::QueryProcess::FullFieldName, field_name)
                          (std::string, order))
BOOST_FUSION_ADAPT_STRUCT(::Database::QueryProcess::ComplexSelectStatement,
                          (std::vector< ::Database::QueryProcess::ComplexSelectStatement::SelectFieldName >, field_names)
                          (std::vector<std::string>, table_names)
                          (std::vector< ::Database::QueryProcess::ComplexCondition >, conditions)
                          (::Database::QueryProcess::FullFieldName, group_by_field_name)
                          (::Database::QueryProcess::ComplexSelectStatement::OrderByClause, order_by))
BOOST_FUSION_ADAPT_STRUCT(::Database::QueryProcess::DeleteStatement,
                          (std::string, table_name)
                          (std::vector< ::Database::QueryProcess::SimpleCondition >, conditions))
BOOST_FUSION_ADAPT_STRUCT(::Database::QueryProcess::UpdateStatement::NewValue,
                          (std::string, field_name)
                          (std::string, value))
BOOST_FUSION_ADAPT_STRUCT(::Database::QueryProcess::UpdateStatement,
                          (std::string, table_name)
                          (std::vector< ::Database::QueryProcess::UpdateStatement::NewValue >, new_values)
                          (std::vector< ::Database::QueryProcess::SimpleCondition >, conditions))


#endif /* DB_QUERY_ANALYSER_H_ */
