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
#include "db_fields.h"

class Database::DBTableManager {
private:
    static constexpr char DB_SUFFIX[] = ".tb";
    static constexpr char ALIGN = 0x00;
    // default page size, in Bytes
    static constexpr uint64 DEFAULT_PAGE_SIZE = 4 * 1024;

    // header pages format constants
    static constexpr uint64 TABLE_NAME_LENGTH = 512;
    static constexpr uint64 FIELD_INFO_LENGTH = 256;
    static constexpr uint64 PAGE_HEADER_LENGTH = 24;
    static constexpr uint64 FIRST_FIELDS_INFO_PAGE = 2;
    static constexpr uint64 FIRST_RECORD_PAGE = 5;
    static constexpr uint64 FIRST_EMPTY_SLOT_PAGE = 3;
    static constexpr uint64 FIRST_EMPTY_PAGE_PAGE = 4;

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
    bool create(const std::string& table_name,
                const DBFields& fields, 
                uint64 page_size = DEFAULT_PAGE_SIZE) {
        // there's already a table opened
        if (isopen()) return 1;
        
        // create DBFile
        _file = new DBFile(table_name + DB_SUFFIX);

        char* buffer = new char[page_size];

        // create file description page, 0th page
        createFileDescriptionPage(page_size, buffer);
        
        // create file and write file description page(0th page)
        bool rtv = _file->create(page_size, buffer);

        // create failed
        if (rtv) {
            delete[] buffer;
            delete _file;
            return 1;
        }
        // else create successful
        _file->open();
        
        // create 1st page
        createDataDescriptionPage(table_name, fields, page_size, buffer);

        // write 1st page
        _file->writePage(1, buffer);
        
        // create Fields description page(s), the first is 2nd page
        createFieldsDescriptionPages(fields, page_size, buffer);

        // write 2nd page
        _file->writePage(2, buffer);

        // create empty slots bitmap page, the first is 3rd page
        // 0 means full, 1 means there's at least one empty slot
        memset(buffer, 0x00, page_size);
        _file->writePage(3, buffer);

        // create empty pagess bitmap page, the first is 4nd page
        // 0 means full, 1 means there's at least one empty slot
        // memset(buffer, 0x00, page_size);
        _file->writePage(4, buffer);

        _file->close();
        

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
    // create file description page, 0th page
    void createFileDescriptionPage(uint64 page_size, char* buffer) {
        // page size
        uint64 pos = 0;
        memcpy(buffer + pos, &page_size, sizeof(page_size));
        pos += sizeof(page_size);
        // number of pages existing in this file
        // 0th - file description, 1st - table description
        // 2nd - fields description, 3rd - empty slots bitmap
        // 4th - empty pages bitmap
        uint64 num_pages = 4;
        memcpy(buffer + pos, &num_pages, sizeof(num_pages));
        pos += sizeof(num_pages);
        // align
        memset(buffer + pos, ALIGN, page_size - pos);
    }

    // create data description page, 1st page
    void createDataDescriptionPage(const std::string& table_name, 
                                   const DBFields& fields,
                                   const uint64 page_size,
                                   char* buffer) {
        // create data related description, 1st page
        uint64 pos = 0;
        // table name
        memset(buffer + pos, ALIGN, TABLE_NAME_LENGTH);
        memcpy(buffer + pos, table_name.c_str(), table_name.length()); 
        pos += TABLE_NAME_LENGTH;

        // fields num
        uint64 fields_num = fields.size();
        memcpy(buffer + pos, &fields_num, sizeof(fields_num));
        pos += sizeof(fields_num);
        /*
        // too many fields that need more than one description page are not supported for now
        // number of filed description in each page
        uint64 field_info_each_page = (page_size - PAGE_HEADER_LENGTH) /
                                      FIELD_INFO_LENGTH;
        memcpy(buffer + pos, &field_info_each_page, sizeof(field_info_each_page));
        pos += sizeof(field_info_each_page);
        */
        // first field description page
        uint64 first_page_info_page = FIRST_FIELDS_INFO_PAGE;
        memcpy(buffer + pos, &first_page_info_page, sizeof(first_page_info_page));
        pos += sizeof(first_page_info_page);
        
        // first empty slot map page
        uint64 first_empty_slot_page = FIRST_EMPTY_SLOT_PAGE;
        memcpy(buffer + pos, &first_empty_slot_page, sizeof(first_empty_slot_page));
        pos += sizeof(first_empty_slot_page);
        // first empty page map page
        uint64 first_empty_page_page = FIRST_EMPTY_PAGE_PAGE;
        memcpy(buffer + pos, &first_empty_page_page, sizeof(first_empty_page_page));
        pos += sizeof(first_empty_page_page);
        // mapped pages in each page
        uint64 pages_mapped_each_page = (page_size - PAGE_HEADER_LENGTH) * 8;
        memcpy(buffer + pos, &pages_mapped_each_page, sizeof(pages_mapped_each_page));
        pos += sizeof(pages_mapped_each_page);

        // record length
        uint64 record_length = fields.recordLength();
        memcpy(buffer + pos, &record_length, sizeof(record_length));
        pos += sizeof(record_length);
        // number of records in each page
        uint64 records_num_each_page = (page_size - PAGE_HEADER_LENGTH) /
                                       record_length;
        while (records_num_each_page * record_length + PAGE_HEADER_LENGTH + 
               (records_num_each_page + 7) / 8 > page_size) 
            --records_num_each_page;
        memcpy(buffer + pos, &records_num_each_page, sizeof(records_num_each_page));
        pos += sizeof(records_num_each_page);
        // first record page
        uint64 first_record_page = FIRST_RECORD_PAGE;
        memcpy(buffer + pos, &first_record_page, sizeof(first_record_page));
        pos += sizeof(first_record_page);
        // align
        memset(buffer + pos, ALIGN, page_size - pos);
        // 1st page end
    }

    void createFieldsDescriptionPages(const DBFields& fields, 
                                      uint64 page_size, char* buffer) {
        // generate page header
        auto page_header = makePageHeader(FIRST_FIELDS_INFO_PAGE, 0, 0);
        assert(page_header.size() == PAGE_HEADER_LENGTH);

        uint64 pos = 0;
        // copy page header to buffer
        memcpy(buffer + pos, page_header.data(), PAGE_HEADER_LENGTH);
        pos += PAGE_HEADER_LENGTH;

        // generate fields description
        char field_info_buffer[FIELD_INFO_LENGTH];
        for (uint64 i = 0; i < fields.size(); ++i) {
            memset(field_info_buffer, ALIGN, FIELD_INFO_LENGTH);
            fields.generateFieldDescription(i, field_info_buffer);
            memcpy(buffer + pos, field_info_buffer, FIELD_INFO_LENGTH);
            pos += FIELD_INFO_LENGTH;
        }

        // fields description larger than one page not supported for now
        assert(pos <= page_size);
        memset(buffer + pos, ALIGN, page_size - pos);
    }

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
