/******************************************************************************
 *  Copyright (c) 2014 Jamis Hoo 
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: Datatase
 *  Filename: db_interface.h 
 *  Version: 1.0
 *  Author: Jamis Hoo
 *  E-mail: hjm211324@gmail.com
 *  Date: Dec. 21, 2014
 *  Time: 09:39:16
 *  Description: Use interaface for database.
 *****************************************************************************/
#ifndef DB_INTERFACE_H_
#define DB_INTERFACE_H_

#include "db_common.h"
#include <queue>
#include <string>

class Database::DBInterface {

public:
    DBInterface(): _in_quote(0), _escaped(0), _in_comment(0) { }
    ~DBInterface() { }

    void feed(const std::string& str) { 
        for (const auto c: str) {
            if (_in_comment) {
                if (c == '\n') {
                    _in_comment = 0;
                    _buff += c;
                }
                continue;
            } else if (_escaped) {
                _escaped = 0;
                switch (c) {
                    case 'b': _buff += '\b'; break;
                    case 'n': _buff += '\n'; break;
                    case 'r': _buff += '\r'; break;
                    case 't': _buff += '\t'; break;
                    default : _buff += c;
                }
            } else {
                switch (c) {
                    case '\\':
                        _escaped = 1;
                        break;
                    case '\'':
                        _in_quote = !_in_quote;
                        _buff += c;
                        break;
                    case ';':
                        _buff += c;
                        if (!_in_quote) {
                            _commands.push(trim(_buff));
                            _buff.clear();
                        }
                        break;
                    case '#':
                        if (!_in_quote) _in_comment = 1;
                        else _buff += c;
                        break;
                    default:
                        _buff += c;
                }
            }
        }
    }

    bool ready() const {
        return _commands.size();
    }

    bool emptyBuff() const {
        return !trim(_buff).size();
    }

    std::string get() {
        std::string tmp = _commands.front();
        _commands.pop();
        return tmp;
    }

private:
#ifdef DEBUG
public:
#endif
    std::string trim(std::string str) const {
        // prefixing spaces
        str.erase(0, str.find_first_not_of(" \n\t\r")); 
        // suffixing spaces
        str.erase(str.find_last_not_of(" \n\t\r") + 1);
        return str;
    }

    // copy is forbidden
    DBInterface (const DBInterface&) = delete;
    DBInterface (DBInterface&&) = delete;
    DBInterface& operator=(const DBInterface&)& = delete;
    DBInterface& operator=(DBInterface&&)& = delete;

    std::string _buff;
    bool _in_quote;
    bool _escaped;
    bool _in_comment;

    std::queue<std::string> _commands;


};

#endif /* DB_INTERFACE_H_ */
