/******************************************************************************
 *  Copyright (c) 2014-2015 Jamis Hoo 
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: Database
 *  Filename: db_outputer.h 
 *  Version: 1.0
 *  Author: Jamis Hoo
 *  E-mail: hjm211324@gmail.com
 *  Date: Dec. 23, 2014
 *  Time: 11:04:05
 *  Description: Left-Aligned list outputer.
 *               It's kind of low-efficient.
 *****************************************************************************/
#ifndef DB_OUTPUTER_H_
#define DB_OUTPUTER_H_

#include <string>

class Database::AlignedOutputer {
    std::vector< std::vector<std::string> > data;
    std::vector<std::size_t> max_length;
    std::ostream& out;
public:
    AlignedOutputer(std::ostream& o): out(o) {
        // new row
        data.push_back(std::vector<std::string>());
    }
    ~AlignedOutputer() {
        // flush before exit
        (*this) << AlignedOutputer::flush;
    }
    // add a new column
    template <class T>
    AlignedOutputer& operator<<(const T& i) {
        add(std::to_string(i));
        return *this;
    }
    AlignedOutputer& operator<<(const char* str) {
        add(str);
        return *this;
    }
    AlignedOutputer& operator<<(const std::string& str) {
        add(str);
        return *this;
    }
    // new row
    static AlignedOutputer& endl(AlignedOutputer& stream) {
        stream.data.push_back(std::vector<std::string>());
        return stream;
    }
    // flush
    static AlignedOutputer& flush(AlignedOutputer& stream) {
        // code below using std::cout is extremely slow.
        for (auto ite = stream.data.begin(); ite != stream.data.end() - 1; ++ite) {
            for (std::size_t i = 0; i < ite->size(); ++i)
                stream.out << (*ite)[i] << std::string(stream.max_length[i] + 1 - (*ite)[i].length(), ' ');
            stream.out << std::endl;
        }
        stream.data.clear(); 
        stream.data.push_back(std::vector<std::string>());
        stream.max_length.clear();
        return stream;
    }
    // for endl and flush
    typedef AlignedOutputer& (*AlignedOutputManipulator)(AlignedOutputer&);
    AlignedOutputer& operator<<(AlignedOutputManipulator manip) {
        return manip(*this);
    }
private:
    // add a new column
    void add(const std::string& str) {
        if (max_length.size() == data.back().size()) max_length.push_back(str.size());
        else max_length[data.back().size()] = std::max(max_length[data.back().size()], str.size());
        data.back().push_back(str);
    }
};

#endif /* DB_OUTPUTER_H_ */
