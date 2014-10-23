/******************************************************************************
 *  Copyright (c) 2014 Jinming Hu 
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: Database
 *  Filename: db_tablemanager.h 
 *  Version: 1.0
 *  Author: Jinming Hu
 *  E-mail: hjm211324@gmail.com
 *  Date: Oct. 23, 2014
 *  Time: 08:47:20
 *  Description: read and write table page, manage table header
 *****************************************************************************/
#ifndef DB_TABLEMANAGER_H_
#define DB_TABLEMANAGER_H_

#include <cassert>
#include <string>
#include <array>
#include "db_common.h"
#include "db_file.h"

class Database::DBTableManager {
private:
    static constexpr char DB_SUFFIX[] = ".tb";
    static constexpr char ALIGN = 0x00;
    // default page size, in Bytes
    static constexpr uint64 DEFAULT_PAGE_SIZE = 4096 * 1024;

    struct PageHeader {
        PageHeader(const uint64 id, const uint64 nextid, const uint64 previd):
            pageID(id), nextPageID(nextid), prevPageID(previd) { }
        uint64 pageID;
        uint64 nextPageID;
        uint64 prevPageID;
    };

public:
    DBTableManager(): _file(nullptr) { }
    ~DBTableManager() {
        // open table should be closed explicitly
        // but we will close it if not
        if (isopen()) {
        // ignore close fail
            _file->close();
            delete _file;
            _file = nullptr;
        }
    }

    // create a table 
    // assert _file == nullptr
    // i.e. no table have been opened
    // returns 0 if succeed, 1 otherwise
    // table won't be opened  after created
    bool create(const std::string& table_name /* fields */, uint64 page_size = DEFAULT_PAGE_SIZE) {
        // there's already a table opened
        if (isopen()) return 1;
        
        // create DBFile
        _file = new DBFile(table_name + DB_SUFFIX);

        char* buffer = new char[page_size];
        
        // file header, 0th page
        // page size
        uint64 pos = 0;
        memcpy(buffer + pos, &page_size, sizeof(page_size));
        pos += sizeof(page_size);
        // number of pages existing in this file
        uint64 num_pages = 1;
        memcpy(buffer + pos, &num_pages, sizeof(num_pages));
        pos += sizeof(num_pages);
        // align
        memset(buffer + pos, ALIGN, page_size - pos);

        // create file and write file header(0th page)
        bool rtv = _file->create(page_size, buffer);

        // create failed
        if (rtv) {
            delete[] buffer;
            delete _file;
            return 1;
        }

        // otherwise, create successful


        // TODO
        // add table header


        // delete DBFile
        delete[] buffer;
        delete _file;
        _file = nullptr;

        return 0;
    }
    
    // open an existing tabel
    // assert no table is opened
    // returns 0 if succeed, 1 otherwise
    bool open(const std::string& table_name) {
        if (isopen()) return 1;

        // create DBFile
        _file = new DBFile(table_name + DB_SUFFIX);

        // openfile
        uint64 page_size = _file->open();

        return !page_size;
    }

    // close an open table
    // assert there's an open table
    // returns 0 if succeed, 1 otherwise
    bool close() {
        if (!isopen()) return 1;

        bool rtv = _file -> close();
        
        // close successful
        if (rtv == 0) {
            delete _file;
            _file = nullptr;
            return 0;
        } else {
        // close failed
            return 1;
        }

    }

    // remove a table
    // assert no table is opened
    // returns 0 if succeed, 1 otherwise
    // the table will be closed if succeed
    bool remove(const std::string& table_name) {
        if (isopen()) return 1;

        _file = new DBFile(table_name + DB_SUFFIX);
        bool rtv = _file->remove();

        delete _file;
        _file = nullptr;

        return rtv;
    }

    // check if there's already table opened
    // returns 1 if there is, null string otherwise
    bool isopen() const {
        if (!_file) return 0;
        assert(_file->isopen());
        return 1;
    }

private:
    std::array<char, 3 * sizeof(uint64)> makePageHeader(const uint64 id, 
                                                        const uint64 nextid, 
                                                        const uint64 previd) {
        std::array<char, 3 * sizeof(uint64)> buffer;
        memcpy(buffer.data(), &id, sizeof(uint64));
        memcpy(buffer.data() + sizeof(uint64), &nextid, sizeof(uint64));
        memcpy(buffer.data() + 2 * sizeof(uint64), &previd, sizeof(uint64));
        return buffer; 
    }

    std::array<uint64, 3> parsePageHeader(const char* buffer) {
        return std::array<uint64, 3>{
            *(reinterpret_cast<const uint64*>(buffer)),
            *(reinterpret_cast<const uint64*>(buffer + sizeof(uint64))),
            *(reinterpret_cast<const uint64*>(buffer + 2 * sizeof(uint64)))
        };
    }
private:
    // forbid copying
    DBTableManager (const DBTableManager&) = delete;
    DBTableManager (const DBTableManager&&) = delete;
    DBTableManager& operator=(const DBTableManager&)& = delete;
    DBTableManager& operator=(const DBTableManager&&)& = delete;

    DBFile* _file;
};

#endif /* DB_TABLEMANAGER_H_ */
