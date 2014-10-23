/******************************************************************************
 *  Copyright (c) 2014 Jinming Hu 
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: Database
 *  Filename: db_filemanager.h 
 *  Version: 1.0
 *  Author: Jinming Hu
 *  E-mail: hjm211324@gmail.com
 *  Date: Oct. 23, 2014
 *  Time: 08:47:20
 *  Description: 
 *****************************************************************************/
#ifndef DB_FILEMANAGER_H_
#define DB_FILEMANAGER_H_

class Database::DBFileManagement {
private:


public:
    DBFileManagement(): _file(nullptr) { }
    ~DBFileManagement() {
        delete _file;
    }

    // create a table 
    // assert _file == nullptr
    // i.e. no table have been opened
    // returns 0 if succeed, 1 otherwise
    bool create(const string& table_name) {

    }

    // remove a table
    // assert no table is opened
    // returns 0 if succeed, 1 otherwise
    // the table will be closed if succeed
    bool remove(const string& table_name) {

    }
    
    // open an existing tabel
    // assert no table is opened
    // returns 0 if succeed, 1 otherwise
    bool open(const string& table_name) {

    }

    // close an open table
    // assert there's an open table
    // returns 0 if succeed, 1 otherwise
    bool close() {

    }

    // check if there's already table opened
    // returns 1 if there is, null string otherwise
    bool opened() const {


    }

private:
    // forbid copying
    DBFileManagement(const DBFileManagement&) = delete;
    DBFileManagement(const DBFileManagement&&) = delete;
    DFFileManagement& operator=(const DBFileManagement&)& = delete;
    DBFileManagement& operator=(const DBFileManagement&&)& = delete;

    DBFile* _file;

};

#endif /* DB_FILEMANAGER_H_ */
