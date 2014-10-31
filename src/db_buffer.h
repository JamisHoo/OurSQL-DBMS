/******************************************************************************
 *  Copyright (c) 2014 Jinming Hu 
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: Database
 *  Filename: db_buffer.h 
 *  Version: 1.0
 *  Author: Jinming Hu
 *  E-mail: hjm211324@gmail.com
 *  Date: Oct. 31, 2014
 *  Time: 15:04:35
 *  Description: Fixed sized data buffer.
                 Read pages from disk, write pages back to disk.
 *****************************************************************************/
#ifndef DB_BUFFER_H_
#define DB_BUFFER_H_

class Database::DBBuffer {
public:
    DBBuffer(const std::string& filename): file(filename) { }

    ~DBBuffer() { }



private:
    // forbid copying
    DBBuffer(const DBBuffer&) = delete;
    DBBuffer(DBBuffer&&) = delete;
    DBBuffer& operator=(const DBBuffer&) & = delete;
    DBBuffer& operator=(DBBuffer&&) & = delete;

    DBFile _file;

};


#endif /* DB_BUFFER_H_ */
