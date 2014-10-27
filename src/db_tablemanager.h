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
#include <initializer_list>
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
    static constexpr uint64 FIRST_RECORD_PAGE = 4;
    static constexpr uint64 FIRST_EMPTY_SLOTS_PAGE = 3;

public:
    DBTableManager(): _file(nullptr), 
                      _num_fields(0), 
                      _pages_each_map_page(0),
                      _record_length(0),
                      _num_records_each_page(0),
                      _last_empty_slots_map_page(0),
                      _last_record_page(0) { }

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
    // table name cannot be too long that exceeds system limit
    // assert _file == nullptr
    // i.e. no table have been opened
    // returns 0 if succeed, 1 otherwise
    // table won't be opened  after created
    bool create(const std::string& table_name,
                const DBFields& fields, 
                const uint64 page_size = DEFAULT_PAGE_SIZE) {
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
        _file->writePage(FIRST_FIELDS_INFO_PAGE, buffer);

        // create empty slots bitmap page, the first is 3rd page
        // 0 means full, 1 means there's at least one empty slot
        memset(buffer, 0x00, page_size);
        // write page header
        auto page_header = makePageHeader(FIRST_EMPTY_SLOTS_PAGE, 0, 0);
        memcpy(buffer, page_header.data(), page_header.size());
        // set 5th page as empty slots
        buffer[PAGE_HEADER_LENGTH] |= (1 << FIRST_RECORD_PAGE);

        // write 3rd page
        _file->writePage(FIRST_EMPTY_SLOTS_PAGE, buffer);

        // write the first empty record page, 4th page
        memset(buffer, 0x00, page_size);
        uint64 first_page = FIRST_RECORD_PAGE;
        memcpy(buffer, &first_page, sizeof(first_page));
        _file->writePage(FIRST_RECORD_PAGE, buffer);

        // close table
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
        
        char* buffer = new char[page_size];
        // read 1st page
        _file->readPage(1, buffer); 
        // parse 1st page
        parseDataDescriptionPage(buffer);

        // read 2nd page
        _file->readPage(FIRST_FIELDS_INFO_PAGE, buffer);
        // parse 2nd page
        parseFieldsDescriptionPages(buffer);

        // read 3rd page
        _file->readPage(FIRST_EMPTY_SLOTS_PAGE, buffer);
        // parse 3rd page
        parseEmptyMapPages(buffer);


        delete[] buffer;
        return !page_size;
    }
    
    // insert a record
    // args: { void*, void*, void* ... }
    // assert number of args == number of fields
    // assert file is open
    // returns 0 if succeed, 1 otherwise
    bool insertRecord(const std::initializer_list<void*> args) {
        if (!isopen()) return 1;
        if (args.size() != _fields.size()) return 1;

        // pass args to _field to generate a record in raw data
        char* buffer = new char[_fields.recordLength()];
        _fields.generateRecord(args, buffer);

        // find an empty record slot
        uint64 empty_slot_pageID = findEmptySlot();


        // TODO
        // if not found, create a new page, 
        // append this page to record pages list tail, 
        // modify _last_record_page.
        // add this page to empty map(this may lead to new map pages)

        // insert the record to the slot

        // check whether there's still any empty slot in this page
        // mark in empty in map
        delete[] buffer;
        return 0;
    }

    // remove a record
    // input: record ID
    // assert file is open
    // returns 0 if succeed, 1 otherwise
    bool removeRecord(const RID rid) {
        if (!isopen()) return 1;

        // TODO
        // find the record

        // remove it
        // mark the slot empty
        
        // check whether the page gets empty

    }

    // modify field field_id of record rid to arg
    // assert file is open
    // returns 0 if succeed, 1 otherwise
    bool modifyRecord(const RID rid, const uint64 field_id, const void* arg) {
        // TODO


    }

    // find records meet the conditions in field_id
    // return RIDs of the records
    // assert file is open
    template <class CONDITION>
    std::vector<RID> findRecords(const uint64 field_id, CONDITION condition) {
        // TODO

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
            _table_name = "";
            _num_fields = 0;
            _fields.clear();
            _pages_each_map_page = 0;
            _record_length = 0;
            _num_records_each_page = 0;
            _last_empty_slots_map_page = 0;
            _last_record_page = 0;
            _empty_slots_map.clear();
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
    void createFileDescriptionPage(const uint64 page_size, char* buffer) const {
        // page size
        uint64 pos = 0;
        memcpy(buffer + pos, &page_size, sizeof(page_size));
        pos += sizeof(page_size);
        // number of pages existing in this file
        // 0th - file description, 1st - table description
        // 2nd - fields description, 3rd - empty slots bitmap
        // 4th - empty pages bitmap
        uint64 num_pages = 5;
        memcpy(buffer + pos, &num_pages, sizeof(num_pages));
        pos += sizeof(num_pages);
        // align
        memset(buffer + pos, ALIGN, page_size - pos);
    }

    // create data description page, 1st page
    void createDataDescriptionPage(const std::string& table_name, 
                                   const DBFields& fields,
                                   const uint64 page_size,
                                   char* buffer) const {
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
        // if u wanna implement it, don't forget that 5th page is the first records page, reserve it
        // number of filed description in each page
        uint64 field_info_each_page = (page_size - PAGE_HEADER_LENGTH) /
                                      FIELD_INFO_LENGTH;
        memcpy(buffer + pos, &field_info_each_page, sizeof(field_info_each_page));
        pos += sizeof(field_info_each_page);
        */
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

        // last empty slots map page
        uint64 last_empty_slots_map_page = FIRST_EMPTY_SLOTS_PAGE;
        memcpy(buffer + pos, &last_empty_slots_map_page, sizeof(last_empty_slots_map_page));
        pos += sizeof(last_empty_slots_map_page);

        // last empty record page
        uint64 last_record_page = FIRST_RECORD_PAGE;
        memcpy(buffer + pos, &last_record_page, sizeof(last_record_page));
        pos += sizeof(last_record_page);
        // align
        memset(buffer + pos, ALIGN, page_size - pos);
        // 1st page end
    }

    // parse 2nd page of database file
    void parseDataDescriptionPage(const char* buffer) {
        uint64 page_size = _file->pageSize();
        uint64 pos = 0;
        // table name
        _table_name = std::string(buffer + pos, TABLE_NAME_LENGTH);
        pos += TABLE_NAME_LENGTH;

        // fields num
        _num_fields = *(pointer_convert<const uint64*>(buffer + pos));
        pos += sizeof(_num_fields);
        

        // mapped pages in each page
        _pages_each_map_page = *(pointer_convert<const uint64*>(buffer + pos));
        pos += sizeof(_pages_each_map_page);
        
        // record length
        _record_length = *(pointer_convert<const uint64*>(buffer + pos));
        pos += sizeof(_record_length);

        // number of records stored in each page
        _num_records_each_page = *(pointer_convert<const uint64*>(buffer + pos));
        pos += sizeof(_num_records_each_page);
        
        // last empty slots map page
        _last_empty_slots_map_page = *(pointer_convert<const uint64*>(buffer + pos));
        pos += sizeof(_last_empty_slots_map_page);


        // last record page
        _last_record_page = *(pointer_convert<const uint64*>(buffer + pos));
        pos += sizeof(_last_record_page);
    }
    
    // create 2nd page for a new database file
    void createFieldsDescriptionPages(const DBFields& fields, 
                                      const uint64 page_size, char* buffer) const {
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

    // parse 2nd page of a existing database file
    void parseFieldsDescriptionPages(const char* buffer) {
        uint64 pos = 0;

        // page header ignored
        pos += PAGE_HEADER_LENGTH;

        // parse fields description
        _fields.clear();
        for (uint64 i = 0; i < _num_fields; ++i) {
            _fields.insert(buffer + pos, FIELD_INFO_LENGTH);
            pos += FIELD_INFO_LENGTH;
        }
    }

    // recursively parse 3rd or 4th page of an existing databse file
    void parseEmptyMapPages(char* buffer) {
        uint64 page_id = *(pointer_convert<const uint64*>(buffer));
        _last_empty_slots_map_page = page_id;
        buffer += sizeof(uint64);
        buffer += sizeof(uint64);
        uint64 next_page_id = *(pointer_convert<const uint64*>(buffer)); 
        buffer += sizeof(uint64);
        
        const char* buffer_end = buffer + _file->pageSize() - PAGE_HEADER_LENGTH;
        while (buffer != buffer_end) {
            char bits = *(buffer++);
            _empty_slots_map.push_back(bits & 0x01);
            _empty_slots_map.push_back(bits & 0x02);
            _empty_slots_map.push_back(bits & 0x04);
            _empty_slots_map.push_back(bits & 0x08);
            _empty_slots_map.push_back(bits & 0x10);
            _empty_slots_map.push_back(bits & 0x20);
            _empty_slots_map.push_back(bits & 0x40);
            _empty_slots_map.push_back(bits & 0x80);
        }

        if (next_page_id != 0) {
            _file->readPage(next_page_id, buffer);
            parseEmptyMapPages(buffer);
        }
    }

    // make a page header
    std::array<char, 3 * sizeof(uint64)> makePageHeader(
            const uint64 id, const uint64 nextid, const uint64 previd) const {
        std::array<char, 3 * sizeof(uint64)> buffer;
        memcpy(buffer.data(), &id, sizeof(uint64));
        memcpy(buffer.data() + sizeof(uint64), &nextid, sizeof(uint64));
        memcpy(buffer.data() + 2 * sizeof(uint64), &previd, sizeof(uint64));
        return buffer; 
    }
    
    // parse a page header
    std::array<uint64, 3> parsePageHeader(const char* buffer) const {
        return std::array<uint64, 3>{
            *(pointer_convert<const uint64*>(buffer)),
            *(pointer_convert<const uint64*>(buffer + sizeof(uint64))),
            *(pointer_convert<const uint64*>(buffer + 2 * sizeof(uint64)))
        };
    }

    // returns page id in which there's at least one empty slot
    // returns 0 if not found
    uint64 findEmptySlot() const {
        for (uint64 i = 0; i < _empty_slots_map.size(); ++i) 
            if (_empty_slots_map[i] == 0) return i;
        return 0;
    }

    void createNewRecordPage() {
        char* buffer = new char[_file->pageSize()];
        _file->readPage(_last_record_page, buffer);
        

    }

    void createNewMapPage() {

    }

private:
#ifdef DEBUG
public:
#endif
    // forbid copying
    DBTableManager (const DBTableManager&) = delete;
    DBTableManager (DBTableManager&&) = delete;
    DBTableManager& operator=(const DBTableManager&)& = delete;
    DBTableManager& operator=(const DBTableManager&&)& = delete;

    DBFile* _file;
    
    // variables below descript an open table
    // they will be reset when closing the table
    std::string _table_name;
    uint64 _num_fields;
    DBFields _fields;
    uint64 _pages_each_map_page;
    uint64 _record_length;
    uint64 _num_records_each_page;
    
    uint64 _last_empty_slots_map_page;
    uint64 _last_record_page;
    
    // use vector<bool> to save memory
    // if you find it slow, replace it with vector<char>
    // 0 means this slot is available
    std::vector<bool> _empty_slots_map;

};

#endif /* DB_TABLEMANAGER_H_ */
