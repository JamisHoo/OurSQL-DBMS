/******************************************************************************
 *  Copyright (c) 2014 Jamis Hoo 
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: Database
 *  Filename: db_tablemanager.h 
 *  Version: 1.0
 *  Author: Jamis Hoo
 *  E-mail: hjm211324@gmail.com
 *  Date: Oct. 23, 2014
 *  Time: 08:47:20
 *  Description: read and write table page, manage table header
 *****************************************************************************/
#ifndef DB_TABLEMANAGER_H_
#define DB_TABLEMANAGER_H_

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <array>
#include <tuple>
#include <initializer_list>
#include "db_common.h"
#include "db_fields.h"
#include "db_buffer.h"
#include "db_indexmanager.h"

class Database::DBTableManager {
public:
    static constexpr char* TABLE_SUFFIX = (char*)".tb";
    static constexpr char* INDEX_SUFFIX = (char*)".idx";
    static constexpr char ALIGN = 0x00;
    // default page size, in Bytes
    static constexpr uint64 DEFAULT_PAGE_SIZE = 8 * 1024;
    static constexpr uint64 DEFAULT_BUFFER_SIZE = 4096 * 1024;

private:
    // header pages format constants
    static constexpr uint64 FIELD_INFO_LENGTH = DBFields::FIELD_INFO_LENGTH;
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
        if (isopen())
            close();
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

        // must have a primary key
        if (!fields.hasPrimaryKey()) return 1;

        // create DBFile
        _file = new DBBuffer(table_name + TABLE_SUFFIX, DEFAULT_BUFFER_SIZE);

        std::unique_ptr<char[]> buffer(new char[page_size]);

        // create file description page, 0th page
        createFileDescriptionPage(page_size, buffer.get());
        
        // create file and write file description page(0th page)
        bool rtv = _file->create(page_size, buffer.get());

        // create failed
        if (rtv) {
            delete _file;
            _file = nullptr;
            return 1;
        }
        // else create successful
        _file->open();

        // create 1st page
        createDataDescriptionPage(fields, page_size, buffer.get());

        // write 1st page
        _file->writePage(1, buffer.get());
        
        // create Fields description page(s), the first is 2nd page
        createFieldsDescriptionPages(fields, page_size, buffer.get());

        // write 2nd page
        _file->writePage(FIRST_FIELDS_INFO_PAGE, buffer.get());

        // create empty slots bitmap page, the first is 3rd page
        // 0 means full, 1 means there's at least one empty slot
        memset(buffer.get(), 0x00, page_size);
        // write page header
        auto page_header = makePageHeader(FIRST_EMPTY_SLOTS_PAGE, 0, 0);
        memcpy(buffer.get(), page_header.data(), page_header.size());
        // set 5th page as empty slots
        buffer[PAGE_HEADER_LENGTH] |= (1 << FIRST_RECORD_PAGE);

        // write 3rd page
        _file->writePage(FIRST_EMPTY_SLOTS_PAGE, buffer.get());

        // write the first empty record page, 4th page
        memset(buffer.get(), 0x00, page_size);
        uint64 first_page = FIRST_RECORD_PAGE;
        memcpy(buffer.get(), &first_page, sizeof(first_page));
        
        // initialize bitmap
        uint64 records_num_each_page = (page_size - PAGE_HEADER_LENGTH) /
                                       fields.recordLength();
        while (records_num_each_page * fields.recordLength() + PAGE_HEADER_LENGTH + 
               (records_num_each_page + 8 * sizeof(uint64) - 1) / (8 * sizeof(uint64)) * 8 > page_size) 
            --records_num_each_page;

        // mark bitmap as empty(1)
        for (uint64 i = 0; i < records_num_each_page;) {
            if (i + 8 < records_num_each_page) {
                assert(i % 8 == 0);
                buffer[PAGE_HEADER_LENGTH + i / 8] = 0xff;
                i += 8;
                continue;
            }
            buffer[PAGE_HEADER_LENGTH + i / 8] |= '\x01' << i % 8;
            ++i;
        }

        _file->writePage(FIRST_RECORD_PAGE, buffer.get());

        
        // INDEX MANIPULATE
        // create index for primary key
        assert(_index.size() == 0);
        _index.assign(fields.size(), nullptr);
        _index[fields.primary_key_field_id()] = new DBIndexManager<DBFields::Comparator>(
            table_name + "_" + 
            fields.field_name().at(fields.primary_key_field_id()) + 
            INDEX_SUFFIX);
        rtv = _index[fields.primary_key_field_id()]->create(page_size, 
                             fields.field_length()[fields.primary_key_field_id()],
                             fields.field_type()[fields.primary_key_field_id()]);
        assert(rtv == 0);
        _index[fields.primary_key_field_id()]->close();
        
        // close table
        _file->close();

        // delete DBFile
        // INDEX MANIPULATE
        delete _index[fields.primary_key_field_id()];
        _index[fields.primary_key_field_id()] = nullptr;
        _index.clear();
        
        delete _file;
        _file = nullptr;

        return 0;
    }
    
    // open an existing tabel
    // assert no table is opened
    // returns 0 if succeed, 1 otherwise
    bool open(const std::string& table_name) {
        if (isopen()) return 1;

        _table_name = table_name;

        // create DBFile
        _file = new DBBuffer(table_name + TABLE_SUFFIX, DEFAULT_BUFFER_SIZE);

        // openfile
        uint64 page_size = _file->open();
        
        std::unique_ptr<char[]> buffer(new char[page_size]);

        // read 1st page
        _file->readPage(1, buffer.get()); 
        // parse 1st page
        parseDataDescriptionPage(buffer.get());

        // read 2nd page
        _file->readPage(FIRST_FIELDS_INFO_PAGE, buffer.get());
        // parse 2nd page
        parseFieldsDescriptionPages(buffer.get());

        // read 3rd page
        _file->readPage(FIRST_EMPTY_SLOTS_PAGE, buffer.get());
        // parse 3rd page
        parseEmptyMapPages(buffer.get());

        // INDEX MANIPULATE
        // open index
        assert(_index.size() == 0);
        _index.assign(_fields.size(), nullptr);
        for (const auto id: _fields.field_id())
            // if this field has index
            if (_fields.indexed()[id]) {
                _index[id] = new DBIndexManager<DBFields::Comparator>(
                    table_name + "_" + 
                    _fields.field_name()[id] + 
                    INDEX_SUFFIX);
                uint64 rtv = _index[id]->open();
                assert(rtv);
            }
        
        return !page_size;
    }

    const DBFields& fieldsDesc() const {
        return _fields;
    }

    // close an open table
    // assert there's an open table
    // returns 0 if succeed, 1 otherwise
    bool close() {
        if (!isopen()) return 1;

        bool rtv = _file->close();

        // close successful
        if (rtv == 0) {
            // INDEX MANIPULATE
            for(auto& index_pointer: _index) {
                if (index_pointer == nullptr) continue;
                assert(index_pointer->close() == 0);
                delete index_pointer;
                index_pointer = nullptr;
            }
            _index.clear();
            
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

    // remove an open table
    // returns 0 if succeed, 1 otherwise
    // the table will be closed if succeed
    bool remove() {
        if (!isopen()) return 1;

        // store all index name
        std::vector<std::string> index_names;
        for (auto id: _fields.field_id())
            if (_index[id])
                index_names.push_back(_fields.field_name()[id]);

        // store table name
        std::string table_name = _table_name;

        // close this table
        assert(close() == 0);

        // construct a file operator
        _file = new DBBuffer(table_name + TABLE_SUFFIX, DEFAULT_BUFFER_SIZE);

        // remove data file
        bool rtv = _file->remove();
        // if remove succeeded, remove all related index files
        // INDEX MANIPULATE
        if (rtv == 0) {
            for (const auto& index_name: index_names) {
                auto index = new DBIndexManager<DBFields::Comparator>(
                    table_name + "_" + index_name + INDEX_SUFFIX);
                assert(index->remove() == 0);
                delete index;
            }
            
        }

        delete _file;
        _file = nullptr;

        return rtv;
        
    }

    // insert a record
    // args: { void*, void*, void* ... }
    // assert number of args == number of fields
    // assert file is open
    // returns rid if succeed, rid(0, 0) otherwise
    // RID insertRecord(const std::initializer_list<void*> args) {
    RID insertRecord(const std::vector<void*> args) {
        if (!isopen()) return { 0, 0 };
        if (args.size() != _fields.size() && _fields.field_name()[_fields.primary_key_field_id()] != "") 
            return { 0, 0 };

        if (_fields.field_name()[_fields.primary_key_field_id()] != "" && args.size() != _fields.size() - 1) 
            return { 0, 0 };

        // check null
        for (uint i = 0; i < args.size(); ++i) 
            if (_fields.notnull()[i] == 1 && args[i] == nullptr) 
                return { 0, 0 };

        //args with null flags
        std::vector<void*> null_flag_args;
        for (uint64 i = 0; i < args.size(); ++i) {
            char* arg = new char[_fields.field_length()[i]];
            // if not null
            if (args[i]) {
                arg[0] = '\xff';
                memcpy(arg + 1, args[i], _fields.field_length()[i] - 1);
            } else
                memset(arg, 0, _fields.field_length()[i]);
            null_flag_args.push_back(arg);
        }

        // if this table doesn't have a primary key
        // add a unique number as primary key
        if (_fields.field_name()[_fields.primary_key_field_id()] == "") {
            char* arg = new char[_fields.field_length()[_fields.primary_key_field_id()]];
            uint64 unique_number = uniqueNumber();
            arg[0] = '\xff';
            memcpy(arg + 1, arg, _fields.field_length()[_fields.primary_key_field_id()] - 1);
            null_flag_args.insert(null_flag_args.begin() + _fields.primary_key_field_id(),
                                  arg);
        }

        // INDEX MANIPULATE
        // find in index
        auto rid = _index[_fields.primary_key_field_id()]->searchRecord(
            pointer_convert<char*>(null_flag_args[_fields.primary_key_field_id()]));

        // if exist already
        if (rid) {
            for (const auto arg: null_flag_args)
                delete[] pointer_convert<const char*>(arg);
            return  { 0, 0 };
        }
        // else not exist, continue inserting


        // find an empty record slot
        uint64 empty_slot_pageID = findEmptySlot();

        // if not found, create a new page, 
        // append this page to record pages list tail, 
        // modify _last_record_page.
        // add this page to empty map(this may lead to new map pages)
        if (!empty_slot_pageID)
            empty_slot_pageID = createNewRecordPage();

        // pass args to _field to generate a record in raw data
        // allocate more space, reserve for later
        std::unique_ptr<char[]> buffer(new char[_file->pageSize()]);

        _fields.generateRecord(null_flag_args, buffer.get());

        // insert the record to the slot
        auto rtv = insertRecordtoPage(empty_slot_pageID, buffer.get());
        // rtv[0]: RID, rtv[1]: empty_slots_remained

        // if there isn't any empty slot in this page
        // mark it as full in map
        if (!std::get<1>(rtv)) {
            _empty_slots_map[empty_slot_pageID] = 0;

            // write back map page to file
            uint64 pageID = empty_slot_pageID;
            _file->readPage(FIRST_EMPTY_SLOTS_PAGE, buffer.get());
            while (pageID >= _pages_each_map_page) {
                pageID -= _pages_each_map_page;
                uint64 nextPageID = *pointer_convert<uint64*>(buffer.get() + 2 * sizeof(uint64));
                _file->readPage(nextPageID, buffer.get());
            }
            buffer[PAGE_HEADER_LENGTH + pageID / 8] &= ~('\x01' << pageID % 8);
            _file->writePage(*pointer_convert<uint64*>(buffer.get()), buffer.get());
        }
        
        // INDEX MANIPULATE
        // insert to index
        bool successful = 1;
        for (auto id: _fields.field_id()) 
            if (_index[id])
                successful &= _index[id]->insertRecord(
                    pointer_convert<char*>(null_flag_args[id]),
                    std::get<0>(rtv),
                    id == _fields.primary_key_field_id());
        assert(successful == 1);

        for (const auto arg: null_flag_args)
            delete[] pointer_convert<const char*>(arg);
        return std::get<0>(rtv);
    }

    // remove a record
    // input: record ID
    // assert file is open
    // returns 0 if succeed, 1 otherwise
    bool removeRecord(const RID rid) {
        if (!isopen()) return 1;

        std::unique_ptr<char[]> buffer(new char[_file->pageSize()]);

        // find the record
        _file->readPage(rid.pageID, buffer.get());
        // assert slot is originally full
        assert(!(buffer[PAGE_HEADER_LENGTH + rid.slotID / 8] & '\x01' << rid.slotID % 8));
        // mark this slot as empty
        buffer[PAGE_HEADER_LENGTH + rid.slotID / 8] |= '\x01' << rid.slotID % 8;
        _file->writePage(rid.pageID, buffer.get());


        // INDEX MANIPULATE
        for (const auto id: _fields.field_id()) 
            if (_index[id]) {
                char* oldRecord = /* base */
                                  buffer.get() + 
                                  /* page header offset */
                                  PAGE_HEADER_LENGTH + 
                                  /* bitmap offset */
                                  (_num_records_each_page + 8 * sizeof(uint64) - 1) / (8 * sizeof(uint64)) * sizeof(uint64) + 
                                  /* record offset */
                                  _record_length * rid.slotID +
                                  /* field offset */
                                  _fields.offset()[id];
                assert(_index[id]->removeRecord(oldRecord, rid));
            }
        
        // check whether this page get empty
        if (_empty_slots_map[rid.pageID] != 1) {
            // mark this page as empty
            _empty_slots_map[rid.pageID] = 1;
            
            // write back to map page
            uint64 pageID = rid.pageID;
            _file->readPage(FIRST_EMPTY_SLOTS_PAGE, buffer.get());
            while (pageID >= _pages_each_map_page) {
                pageID -= _pages_each_map_page;
                _file->readPage(*pointer_convert<uint64*>(buffer.get() + 2 * sizeof(uint64)), buffer.get());
            }
            buffer[PAGE_HEADER_LENGTH + pageID / 8] |= '\x01' << pageID % 8;
            _file->writePage(*pointer_convert<uint64*>(buffer.get()), buffer.get());
        }
        
        return 0;
    }

    // modify field field_id of record rid to arg
    // assert file is open
    // returns 0 if succeed, 1 otherwise
    bool modifyRecord(const RID rid, const uint64 field_id, const void* arg) {
        if (!isopen()) return 1;

        // check null
        if (_fields.notnull()[field_id] == 1 && arg == nullptr) 
            return 1;

        std::unique_ptr<char[]> null_flag_arg(new char[_fields.field_length()[field_id]]);
        if (arg) {
            null_flag_arg[0] = '\xff';
            memcpy(null_flag_arg.get() + 1, arg, _fields.field_length()[field_id] - 1);
        } else
            memset(null_flag_arg.get(), 0, _fields.field_length()[field_id]);

        // INDEX MANIPULATE
        // if this is primary key field, find in index
        if (field_id == _fields.primary_key_field_id()) {
            auto rtv = _index[_fields.primary_key_field_id()]->
                searchRecord(pointer_convert<const char*>(null_flag_arg.get()));
            // if exist already
            if (rtv) return 1;
        } // else not exist, continue modifying
    

        std::unique_ptr<char[]> buffer(new char[_file->pageSize()]);

        // read in this page
        _file->readPage(rid.pageID, buffer.get());
        
        // assert this slot is used
        assert(!(buffer[PAGE_HEADER_LENGTH + rid.slotID / 8] & '\x01' << rid.slotID % 8));

        // modify record
        char* oldRecord = /* base */
                          buffer.get() + 
                          /* page header offset */
                          PAGE_HEADER_LENGTH + 
                          /* bitmap offset */
                          (_num_records_each_page + 8 * sizeof(uint64) - 1) / (8 * sizeof(uint64)) * sizeof(uint64) + 
                          /* record offset */
                          _record_length * rid.slotID +
                          /* field offset */
                          _fields.offset()[field_id];

        // INDEX MANIPULATE
        // remove old, insert new in index
        if (_index[field_id]) {
            bool rtv;
            rtv = _index[field_id]->removeRecord(oldRecord, rid);
            assert(rtv);
            rtv = _index[field_id]->insertRecord(pointer_convert<const char*>(null_flag_arg.get()), 
                                                 rid, 
                                                 field_id == _fields.primary_key_field_id());
            assert(rtv);
        }

        // modify record
        memcpy(oldRecord, null_flag_arg.get(), _fields.field_length()[field_id]);
        
        // write back
        _file->writePage(rid.pageID, buffer.get());


        return 0;
    }

    // find records meet the conditions in field_id
    // CONDITION is conditon(const char*)
    // NULL value will give a null pointer
    // rid of record will be added to vector if condition returns 1
    // return RIDs of the records
    // assert file is open
    template <class CONDITION>
    std::vector<RID> findRecords(const uint64 field_id, CONDITION condition) const {
        // traverse all records, this cannot use index
        std::vector<RID> rids;

        if (!isopen()) return rids;

        auto traverseCallback = [&rids, this, &field_id, &condition](const char* record, const RID rid) {
            if (condition(record[_fields.offset()[field_id]]? record + _fields.offset()[field_id] + 1: nullptr)) 
                rids.push_back(rid);
        };

        traverseRecords(traverseCallback);
        
        return rids;
    }
    
    // find record(s) that field[field_id] == key
    // return RIDs of the records
    // assert file is open
    // assert there's already index for this field
    std::vector<RID> findRecords(const uint64 field_id, const char* key) const {
        // INDEX MANIPULATE
        assert(_index[field_id]);
        std::unique_ptr<char[]> null_flag_key(new char[_fields.field_length()[field_id]]);
        if (key) {
            null_flag_key[0] = '\xff';
            memcpy(null_flag_key.get() + 1, key, _fields.field_length()[field_id] - 1);
        } else
            memset(null_flag_key.get(), 0, _fields.field_length()[field_id]);
        auto rids = _index[field_id]->searchRecords(null_flag_key.get());
        return rids;
    }

    // find record(s) that lb <= field[field_id] < ub
    // return RIDs of the records
    // assert file is open
    // assert there's already index for this field
    std::vector<RID> findRecords(const uint64 field_id, const char* lb, const char* ub) const {
        assert(_index[field_id]);
        // INDEX MANIPULATE
        std::unique_ptr<char[]> null_flag_lb(new char[_fields.field_length()[field_id]]);
        std::unique_ptr<char[]> null_flag_ub(new char[_fields.field_length()[field_id]]);
        if (lb) {
            null_flag_lb[0] = '\xff';
            memcpy(null_flag_lb.get() + 1, lb, _fields.field_length()[field_id] - 1);
        } else
            memset(null_flag_lb.get(), 0, _fields.field_length()[field_id]);
        if (ub) {
            null_flag_ub[0] = '\xff';
            memcpy(null_flag_ub.get() + 1, ub, _fields.field_length()[field_id] - 1);
        } else
            memset(null_flag_ub.get(), 0, _fields.field_length()[field_id]);

        return _index[field_id]->rangeQuery(null_flag_lb.get(), null_flag_ub.get());
    }


    // check if there's already table opened
    // returns 1 if there is, null string otherwise
    bool isopen() const {
        if (!_file) return 0;
        assert(_file->isopen());
        return 1;
    }

    // create index for field field_id.
    // returns 0 if succeed, 1 otherwise
    bool createIndex(const uint64 field_id, const std::string& /* index_name */) {
        // table not open
        if (!isopen()) return 1;
        // invalid field_id
        if (field_id >= _fields.size()) return 1;
        // attemp to create a already existing index
        if (_index[field_id] != nullptr) return 1;

        // create index
        _index[field_id] = new DBIndexManager<DBFields::Comparator>(
            _table_name + "_" + 
            _fields.field_name()[field_id] + 
            INDEX_SUFFIX);
        bool rtv = _index[field_id]->create(_file->pageSize(),
                                            _fields.field_length()[field_id],
                                            _fields.field_type()[field_id]);
        assert(rtv == 0);

        // modify fields description page
        std::unique_ptr<char[]> buffer(new char[_file->pageSize()]);
        _file->readPage(2, buffer.get());
        char* offset = buffer.get() +
                       PAGE_HEADER_LENGTH + 
                       FIELD_INFO_LENGTH * field_id + 
                       sizeof(uint64) + sizeof(uint64) + sizeof(uint64) + 
                       sizeof(bool);
        assert(offset[0] == '\x00');
        offset[0] = '\x01';
        _file->writePage(2, buffer.get());

        _index[field_id]->open();

        // insert existing data
        auto insertExistingRecords = [this, &field_id](const char* record, const RID rid) {
            int rtv =
            _index[field_id]->insertRecord(record + _fields.offset()[field_id],
                                           rid, 0);
            assert(rtv == 1);
        };

        traverseRecords(insertExistingRecords);

        assert(_index[field_id]->getNumRecords() == _index[_fields.primary_key_field_id()]->getNumRecords());
        
        return 0;
    }
    
    // remove index of field field_id
    // returns 0 if succeed, 1 otherwise
    bool removeIndex(const uint64 field_id) {
        // table not open
        if (!isopen()) return 1;
        // invalid field_id
        if (field_id >= _fields.size()) return 1;
        // attemp to remove primary key field index
        if (field_id == _fields.primary_key_field_id()) return 1;
        // remove non-existing index
        if (_index[field_id] == nullptr) return 1;
        // if close failed
        if (_index[field_id]->close()) return 1;

        // if remove failed
        if (_index[field_id]->remove()) return 1;
        // else remove successful
        delete _index[field_id];
        _index[field_id] = nullptr;

        // modify fields description page
        std::unique_ptr<char[]> buffer(new char[_file->pageSize()]);
        _file->readPage(2, buffer.get());
        char* offset = buffer.get() +
                       PAGE_HEADER_LENGTH + 
                       FIELD_INFO_LENGTH * field_id + 
                       sizeof(uint64) + sizeof(uint64) + sizeof(uint64) + 
                       sizeof(bool);
        assert(offset[0] != '\x00');
        offset[0] = '\x00';
        _file->writePage(2, buffer.get());

        return 0;
    }

private:   
#ifdef DEBUG
public:
#endif
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
    void createDataDescriptionPage(const DBFields& fields,
                                   const uint64 page_size,
                                   char* buffer) const {
        // create data related description, 1st page
        uint64 pos = 0;

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
               (records_num_each_page + 8 * sizeof(uint64) - 1) / (8 * sizeof(uint64)) * 8 > page_size) 
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

        // fields num
        _num_fields = *pointer_convert<const uint64*>(buffer + pos);
        pos += sizeof(_num_fields);
        

        // mapped pages in each page
        _pages_each_map_page = *pointer_convert<const uint64*>(buffer + pos);
        pos += sizeof(_pages_each_map_page);
        
        // record length
        _record_length = *pointer_convert<const uint64*>(buffer + pos);
        pos += sizeof(_record_length);

        // number of records stored in each page
        _num_records_each_page = *pointer_convert<const uint64*>(buffer + pos);
        pos += sizeof(_num_records_each_page);
        
        // last empty slots map page
        _last_empty_slots_map_page = *pointer_convert<const uint64*>(buffer + pos);
        pos += sizeof(_last_empty_slots_map_page);


        // last record page
        _last_record_page = *pointer_convert<const uint64*>(buffer + pos);
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
            assert(field_info_buffer[FIELD_INFO_LENGTH - 1] == '\x00');
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
            _fields.insert(buffer + pos);
            pos += FIELD_INFO_LENGTH;
        }
    }

    // recursively parse 3rd page of an existing databse file
    void parseEmptyMapPages(char* buffer) {
        uint64 page_id = *pointer_convert<const uint64*>(buffer);
        _last_empty_slots_map_page = page_id;
        uint64 next_page_id = *pointer_convert<uint64*>(buffer + 2 * sizeof(uint64)); 
            
        // Here, we may add extra bits mapping non-existing pages
        char* buffer_start = buffer + PAGE_HEADER_LENGTH;
        const char* buffer_end = buffer + _file->pageSize();


        while (buffer_start != buffer_end) {
            char bits = *(buffer_start++);
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
            *pointer_convert<const uint64*>(buffer),
            *pointer_convert<const uint64*>(buffer + sizeof(uint64)),
            *pointer_convert<const uint64*>(buffer + 2 * sizeof(uint64))
        };
    }

    // returns page id in which there's at least one empty slot
    // returns 0 if not found
    uint64 findEmptySlot() const {
        for (uint64 i = 0; i < _empty_slots_map.size(); ++i) 
            if (_empty_slots_map[i] == 1) return i;
        return 0;
    }
    
    // create a new record page
    uint64 createNewRecordPage() {
        uint64 newPageID = _file->numPages();

        std::unique_ptr<char[]> buffer(new char[_file->pageSize()]);

        // record pages are double-linked list, need to modify the last page
        _file->readPage(_last_record_page, buffer.get());

        // page header of the last page
        auto oldPageHeader = parsePageHeader(buffer.get());

        
        // modify "Next Page" to new page id
        auto modifiedPageHeader = makePageHeader(oldPageHeader[0], 
                                                 oldPageHeader[1],
                                                 newPageID);
        // copy to buffer
        memcpy(buffer.get(), modifiedPageHeader.data(), PAGE_HEADER_LENGTH);
        // and overwrite
        _file->writePage(_last_record_page, buffer.get());
        
        // generate new page header
        auto newPageHeader = makePageHeader(newPageID, 
                                            oldPageHeader[0],
                                            0);
        // copy to buffer
        memset(buffer.get(), 0x00, _file->pageSize());
        memcpy(buffer.get(), newPageHeader.data(), PAGE_HEADER_LENGTH);

        // mark bitmap as empty(1)
        for (uint64 i = 0; i < _num_records_each_page;) {
            if (i + 8 < _num_records_each_page) {
                assert(i % 8 == 0);
                buffer[PAGE_HEADER_LENGTH + i / 8] = 0xff;
                i += 8;
                continue;
            }
            buffer[PAGE_HEADER_LENGTH + i / 8] |= '\x01' << i % 8;
            ++i;
        }

        // write to file 
        _file->writePage(newPageID, buffer.get());
        // modify _last_record_page
        _last_record_page = newPageID;

        // write back last record page
        _file->readPage(1, buffer.get());
        memcpy(buffer.get() + 5 * sizeof(uint64), 
               &_last_record_page, 
               sizeof(_last_record_page));
        _file->writePage(1, buffer.get());


        assert(_file->numPages() == newPageID + 1);
        
        // this may lead to new map page
        if (_empty_slots_map.size() == newPageID)
            createNewMapPage();

        // add the new page to slots map
        _empty_slots_map[newPageID] = 1;

        // write map page back to file
        _file->readPage(_last_empty_slots_map_page, buffer.get());
        buffer[PAGE_HEADER_LENGTH + (newPageID % _pages_each_map_page) / 8] |=
            '\x01' << (newPageID % _pages_each_map_page) % 8;
        _file->writePage(_last_empty_slots_map_page, buffer.get());

        return newPageID;
    }

    // create new map page
    void createNewMapPage() {
        uint64 newPageID = _file->numPages();

        std::unique_ptr<char[]> buffer(new char[_file->pageSize()]);

        // record pages are double-linked list, need to modify the last page
        _file->readPage(_last_empty_slots_map_page, buffer.get());

        // page header of the last page
        auto oldPageHeader = parsePageHeader(buffer.get());
        
        // modify "Next Page" to new page id
        auto modifiedPageHeader = makePageHeader(oldPageHeader[0], 
                                                 oldPageHeader[1],
                                                 newPageID);
        // copy to buffer
        memcpy(buffer.get(), modifiedPageHeader.data(), PAGE_HEADER_LENGTH);
        // and overwrite
        _file->writePage(_last_empty_slots_map_page, buffer.get());
        
        // generate new page header
        auto newPageHeader = makePageHeader(newPageID, 
                                            oldPageHeader[0],
                                            0);
        // copy to buffer
        memset(buffer.get(), 0x00, _file->pageSize());
        memcpy(buffer.get(), newPageHeader.data(), PAGE_HEADER_LENGTH);
        // need not to write bits into this page
        // becasue 0 stands for non-empty or non-existing page

        // write to file, and modify _last_empty_slots_map_page
        _file->writePage(newPageID, buffer.get());
        _last_empty_slots_map_page = newPageID;;

        assert(_file->numPages() == newPageID + 1);

        // write back _last_map_page
        _file->readPage(1, buffer.get());
        memcpy(buffer.get() + 4 * sizeof(uint64), 
               &_last_empty_slots_map_page, 
               sizeof(_last_empty_slots_map_page));
        _file->writePage(1, buffer.get());

        
        // add the new page to slots map
        _empty_slots_map.resize(_empty_slots_map.size() + 
                                8 * (_file->pageSize() - PAGE_HEADER_LENGTH), 0);
    }
    
    // select a slot in page pageID, insert buffer to this slot.
    // returns RID of this slot
    // returns whether there's still empty slot in this page
    std::tuple<RID, bool> insertRecordtoPage(const uint64 pageID, const char* recordBuffer) {
        std::unique_ptr<char[]> pageBuffer(new char[_file->pageSize()]);
        // read target page
        _file->readPage(pageID, pageBuffer.get());
        
        uint64 empty_slot_num = _num_records_each_page;
        // scan slots bitmap, find an empty slot
        for (uint64 i = 0; i < _num_records_each_page; ++i) 
            // slot[i] == 1, it's empty
            if (pageBuffer[PAGE_HEADER_LENGTH + i / 8] & ('\x01' << i % 8)) {
                empty_slot_num = i;
                // set this bit as full(0)
                pageBuffer[PAGE_HEADER_LENGTH + i / 8] &= ~('\x01' << i % 8);
                break;
            }
        
        // there's still empty slot remained
        bool empty_slot_remained = 0;
        for (uint64 i = empty_slot_num; i < _num_records_each_page; ++i)
            if (pageBuffer[PAGE_HEADER_LENGTH + i / 8] & ('\x01' << i % 8)) {
                empty_slot_remained = 1;
                break;
            }

        assert(empty_slot_num >= 0 && empty_slot_num < _num_records_each_page);

        // write record
        memcpy(pageBuffer.get() + 
                   /* page header offset */
                   PAGE_HEADER_LENGTH + 
                   /* bitmap offset, bitmap is n bytes aligned */
                   (_num_records_each_page + 8 * sizeof(uint64) - 1) / (8 * sizeof(uint64)) * sizeof(uint64) + 
                   /* record offset */
                   _record_length * empty_slot_num, 
               recordBuffer,
               _record_length);

        // write to file
        _file->writePage(pageID, pageBuffer.get());
        
        return std::make_tuple(RID(pageID, empty_slot_num), empty_slot_remained);
    }
#ifdef DEBUG
public:
    void checkIndex() const {
        // traverse each record in data file, verify in index
        uint64 num_records = 0;
        auto verifyIndex = [this, &num_records](const char* record, const RID rid) {
            for (const auto id: _fields.field_id()) 
                if (_index[id]) {
                    auto rids = _index[id]->searchRecords(record + _fields.offset()[id]);
                    auto ite = find(rids.begin(), rids.end(), rid);
                    assert(ite != rids.end());
                }
            ++num_records;
        };

        traverseRecords(verifyIndex);

        for (const auto id: _fields.field_id())
            if (_index[id]) 
                assert(num_records == _index[id]->getNumRecords());

        uint64 num_records2 = 0;
        std::unique_ptr<char[]> buffer(new char[_file->pageSize()]);

        uint64 verifyRecord_field_id = 0;
        // traverse each record in index, verify in data file
        auto verifyRecord = [this, &buffer, &num_records2, &verifyRecord_field_id](const char* record, const RID rid) {
            _file->readPage(rid.pageID, buffer.get());
            
            char* rightRecord = /* base */
                                buffer.get() + 
                                /* page header offset */
                                PAGE_HEADER_LENGTH + 
                                /* bitmap offset */
                                (_num_records_each_page + 8 * sizeof(uint64) - 1) / (8 * sizeof(uint64)) * sizeof(uint64) + 
                                /* record offset */
                                _record_length * rid.slotID +
                                /* field offset */
                                _fields.offset()[verifyRecord_field_id];
            assert(!memcmp(rightRecord, record, _fields.field_length()[verifyRecord_field_id]));
            ++num_records2;
        };
        
        for (auto const id: _fields.field_id())
            if (_index[id]) {
                verifyRecord_field_id = id;
                _index[id]->traverseRecords(verifyRecord);
            }

        assert(num_records2 % num_records == 0);
    }
#endif
    
    // traverse all records
    // callback function is: func(const char* record buffer, RID)
    // record buffer get invalid after func returns
    template<class CALLBACKFUNC>
    void traverseRecords(CALLBACKFUNC func) const {
        assert(isopen());
        
        std::unique_ptr<char[]> buffer(new char[_file->pageSize()]);

        // current page id
        uint64 pageID = FIRST_RECORD_PAGE;

        // while page id != 0
        while (pageID) {
            _file->readPage(pageID, buffer.get());
            
            char* bitmap_offset = buffer.get() + PAGE_HEADER_LENGTH;
            char* record_offset = buffer.get() + PAGE_HEADER_LENGTH + 
                (_num_records_each_page + 8 * sizeof(uint64) - 1) / (8 * sizeof(uint64)) * sizeof(uint64);

            // traverse each slot
            for (uint64 i = 0; i < _num_records_each_page; ++i) 
                // if slot is not empty
                if ((bitmap_offset[i / 8] & ('\x01' << i % 8)) == 0) 
                    // callback
                    func(record_offset + _record_length * i, RID(pageID, i));

            // next page id
            pageID = *pointer_convert<uint64*>(buffer.get() + sizeof(uint64) * 2);
        }
    }
    
private:
#ifdef DEBUG
public:
#endif
    // forbid copying
    DBTableManager (const DBTableManager&) = delete;
    DBTableManager (DBTableManager&&) = delete;
    DBTableManager& operator=(const DBTableManager&)& = delete;
    DBTableManager& operator=(DBTableManager&&)& = delete;

    DBBuffer* _file;

    // indexes
    std::vector< DBIndexManager<DBFields::Comparator>* > _index;

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
    // 1 means there's empty slot in this page
    // 0 means page is full or non-existing
    std::vector<bool> _empty_slots_map;

};

#endif /* DB_TABLEMANAGER_H_ */
