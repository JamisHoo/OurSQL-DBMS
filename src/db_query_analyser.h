/******************************************************************************
 *  Copyright (c) 2014 Jinming Hu 
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: Database
 *  Filename: db_query_analyser.h 
 *  Version: 1.0
 *  Author: Jinming Hu
 *  E-mail: hjm211324@gmail.com
 *  Date: Dec. 05, 2014
 *  Time: 20:33:43
 *  Description: Query analyser, parse a string statement.
 *****************************************************************************/
#ifndef DB_QUERY_ANALYSER_H_
#define DB_QUERY_ANALYSER_H_

#include <boost/fusion/adapted.hpp>
#include <boost/spirit/include/qi.hpp>
#include "db_common.h"

namespace Database{
namespace QueryProcess {
using namespace boost::spirit;

struct CreateDBStatement { std::string db_name; };
struct DropDBStatement { std::string db_name; };
struct UseDBStatement { std::string db_name; };

qi::rule<std::string::const_iterator, std::string(), qi::space_type> sql_identifier = 
    lexeme[(qi::alpha | qi::char_('_')) >> *(qi::alnum | qi::char_('_'))];

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


#endif /* DB_QUERY_ANALYSER_H_ */
