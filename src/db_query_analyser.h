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

struct CreateDBStatement {
    std::string db_name;
};

struct DropDBStatement {
    std::string db_name;
};


template <class Iterator, class Skipper = qi::space_type>
struct CreateDBStatementParser: qi::grammar<Iterator, CreateDBStatement(), Skipper> {
    CreateDBStatementParser(): CreateDBStatementParser::base_type(start) {
        using namespace qi;

        sql_identifier = lexeme[(alpha | char_('_')) >> *(alnum | char_('_'))];

        start = no_case["create database"] >> omit[no_skip[+space]] >> sql_identifier >> ';';
    }
private:
    qi::rule<Iterator, std::string(), Skipper> sql_identifier;
    qi::rule<Iterator, CreateDBStatement(), Skipper> start;
};

template <class Iterator, class Skipper = qi::space_type>
struct DropDBStatementParser: qi::grammar<Iterator, DropDBStatement(), Skipper> {
    DropDBStatementParser(): DropDBStatementParser::base_type(start) {
        using namespace qi;

        sql_identifier = lexeme[(alpha | char_('_')) >> *(alnum | char_('_'))];

        start = no_case["drop database"] >> omit[no_skip[+space]] >> sql_identifier >> ';';
    }
private:
    qi::rule<Iterator, std::string(), Skipper> sql_identifier;
    qi::rule<Iterator, DropDBStatement(), Skipper> start;
};

}
}

BOOST_FUSION_ADAPT_STRUCT(::Database::QueryProcess::CreateDBStatement,
                          (std::string, db_name))

BOOST_FUSION_ADAPT_STRUCT(::Database::QueryProcess::DropDBStatement,
                          (std::string, db_name))


#endif /* DB_QUERY_ANALYSER_H_ */
