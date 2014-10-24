/******************************************************************************
 *  Copyright (c) 2014 Jinming Hu 
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: Database
 *  Filename: db_file.h 
 *  Version: 1.0
 *  Author: Jinming Hu
 *  E-mail: hjm211324@gmail.com
 *  Date: Oct. 22, 2014
 *  Time: 18:57:07
 *  Description: read and write pages from/to raw file
 *****************************************************************************/
#ifndef DB_FILE_H_
#define DB_FILE_H_

#include <fstream>
#include <cstdio>
#include "db_common.h"

class Database::DBFile {
public:
    DBFile(const std::string& filename): _file(filename),
                                         _page_size(0),
                                         _num_pages(0) { }

    ~DBFile() {
        _fs.close();
    }

    // opens an existing data file.
    // returns 0 if file not exists or open failed.
    // returns page size if open correctly.
    uint64 open() {
        // fail if file not accessible
        if (!accessible()) return 0;

        _fs.open(_file, std::fstream::in |
                        std::fstream::out |
                        std::fstream::binary);

        // open failed
        if (!isopen()) return 0;


        // read file header
        _fs.seekg(0);
        char buffer[sizeof(_page_size) + sizeof(_num_pages)];
        _fs.read(buffer, sizeof(_page_size) + sizeof(_num_pages));
        
        _page_size = *(reinterpret_cast<uint64*>(buffer));
        _num_pages = *(reinterpret_cast<uint64*>(buffer + sizeof(_page_size)));

        return _page_size;
    };
    
    // create file and write raw file description(0th page).
    // returns 0 if succeed, 1 otherwise.
    // file won't be opened after creating
    bool create(const uint64 page_size, const char* buffer) {
        // fail if file exists
        if (accessible()) return 1;

        _fs.open(_file, std::fstream::out| std::fstream::binary);
        // create failed
        if (!isopen()) return 1;

        // _page_size is needed by writePage()
        _page_size = page_size;

        // write first page after creating
        writePage(0, buffer);
        
        _fs.close();
        _page_size = 0;
        return 0;
    }
    
    // delete a file.
    // assert file exists and isn't open.
    // return 0 if succeed, 1 otherwise.
    bool remove() {
        if (isopen()) return 1;
        if (!accessible()) return 1;
        int ret = std::remove(_file.c_str());
        if (ret) return 1;
        if (accessible()) return 1;
        
        _num_pages = 0;
        _page_size = 0;

        return 0;
    }
    
    // close an open file
    // returns 1 if file isn't opened or close failed
    // returns 0 otherwise
    bool close() {
        if (!isopen()) return 1;
        _fs.close();
        if (isopen()) return 1;
        _num_pages = 0;
        _page_size = 0;
        return 0;
    }

    // returns 1 if file is opened, 0 otherwise
    bool isopen() const { return _fs.is_open(); }

    // check whether the file is accessible
    bool accessible() const {
        std::ifstream fin(_file);
        return fin.good();
    }

    // write data in buffer to Page i
    void writePage(const uint64 i, const char* buffer) {
        _fs.seekp(_page_size * i);
        _fs.write(buffer, _page_size);
        // enlarge file
        if (i > _num_pages) {
            _num_pages = i;
            char buffer2[sizeof(_num_pages) + sizeof(_page_size)];
            memcpy(buffer2, &_page_size, sizeof(_page_size));
            memcpy(buffer2 + sizeof(_page_size), &_num_pages, sizeof(_num_pages));
            _fs.seekp(0);
            _fs.write(buffer2, sizeof(_num_pages) + sizeof(_num_pages));
        }
    }

    // read data in Page i to buffer
    void readPage(const uint64 i, char* buffer) {
        _fs.seekg(_page_size * i);
        _fs.read(buffer, _page_size);
    }

    uint64 pageSize() const { return _page_size; }

    uint64 numPages() const { return _num_pages; }

private:
    // forbid copying
    DBFile(const DBFile&) = delete;
    DBFile(const DBFile&&) = delete;
    DBFile& operator=(const DBFile&) & = delete;
    DBFile& operator=(const DBFile&&) & = delete;

    // file name
    std::string _file;
    // page size of this file, in byte
    uint64 _page_size;
    // number of pages existing in this file
    uint64 _num_pages;
    // file input and output stream
    std::fstream _fs;

};


#endif /* DB_FILE_H_ */
