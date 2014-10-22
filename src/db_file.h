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
 *  Description: 
 *****************************************************************************/
#ifndef DB_FILE_H_
#define DB_FILE_H_

#include "db_common.h"
#include <fstream>
#include <cstdio>

class Database::DBFile {
private:
    static constexpr char ALIGN = 0x00;
    // default page size, in Bytes
    static constexpr uint64 DEFAULT_PAGE_SIZE = 4096 * 1024;

public:
    DBFile(const std::string& filename): _file(filename),
                                         _page_size(0),
                                         _buffer(nullptr),
                                         _buffer_size(0),
                                         _num_pages(0) { }

    ~DBFile() {
        _fs.close();
        delete[] _buffer;
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


        expandBuffer();
        // read file header
        readPage(0, _buffer);

        _page_size = *(reinterpret_cast<uint64*>(_buffer));
        _num_pages = *(reinterpret_cast<uint64*>(_buffer + sizeof(_page_size)));
    };
    
    // create file and write raw file description(0th page).
    // returns 0 if succeed, 1 otherwise.
    // file won't be opened after creating
    bool create(const uint64 file_page_size = DEFAULT_PAGE_SIZE) {
        // fail if file exists
        if (accessible()) return 1;

        _fs.open(_file, std::fstream::out| std::fstream::binary);
        // create failed
        if (!isopen()) return 1;

        _page_size = file_page_size;        
        expandBuffer();

        // write file header
        // page size of this file
        uint64 pos = 0;
        memcpy(_buffer + pos, &file_page_size, sizeof(file_page_size));
        pos += sizeof(file_page_size);
        // number of pages existing in this file
        uint64 num_pages = 0;
        memcpy(_buffer + pos, &num_pages, sizeof(num_pages));
        pos += sizeof(num_pages);
        // align
        memset(_buffer + pos, ALIGN, file_page_size - pos);
        
        // write to file
        writePage(0, _buffer);

        _num_pages = 1;

        _fs.close();
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

        return 0;
    }
    
    // close an open file
    // returns 1 if file isn't opened or close failed
    // returns 0 otherwise
    bool close() {
        if (!isopen()) return 1;
        _fs.close();
        if (isopen()) return 1;
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
    }

    // read data in Page i to buffer
    void readPage(const uint64 i, char* buffer) {
        _fs.seekg(_page_size * i);
        _fs.read(buffer, _page_size);
    }

    uint64 pageSize() const { return _page_size; }

    uint64 numPages() const { return _num_pages; }

private:
    // keep buffer >= page size of this file.
    void expandBuffer() {
        if (_page_size <= _buffer_size) return;

        delete[] _buffer;
        _buffer = new char[_page_size];
        _buffer_size = _page_size;
    }


private:
    DBFile(const DBFile&) = delete;
    DBFile(const DBFile&&) = delete;
    DBFile& operator=(const DBFile&) & = delete;
    DBFile& operator=(const DBFile&&) & = delete;

    // file name
    std::string _file;
    // page size of this file
    uint64 _page_size;
    // number of pages existing in this file
    uint64 _num_pages;
    // file input and output stream
    std::fstream _fs;
    // read and write buffer
    char* _buffer;
    // size of the buffer
    uint64 _buffer_size;

};


#endif /* DB_FILE_H_ */
