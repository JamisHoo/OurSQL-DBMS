/******************************************************************************
 *  Copyright (c) 2014 Jinming Hu 
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: Database
 *  Filename: db_fields.h 
 *  Version: 1.0
 *  Author: Jinming Hu
 *  E-mail: hjm211324@gmail.com
 *  Date: Oct. 23, 2014
 *  Time: 17:51:54
 *  Description: Fields of a table.
 *****************************************************************************/
#ifndef DB_FIELDS_H_
#define DB_FIELDS_H_

#include <vector>
#include <string>
#include <initializer_list>
#include "db_common.h"

class Database::DBFields {
public:
    static constexpr uint64 TYPE_INT8    = 0;
    static constexpr uint64 TYPE_UINT8   = 1;
    static constexpr uint64 TYPE_INT16   = 2;
    static constexpr uint64 TYPE_UINT16  = 3;
    static constexpr uint64 TYPE_INT32   = 4;
    static constexpr uint64 TYPE_UINT32  = 5;
    static constexpr uint64 TYPE_INT64   = 6;
    static constexpr uint64 TYPE_UINT64  = 7;
    static constexpr uint64 TYPE_BOOL    = 8;
    // TODO: portable?
    static constexpr uint64 TYPE_CHAR    = 9;
    static constexpr uint64 TYPE_UCHAR   = 10;
    static constexpr uint64 TYPE_FLOAT   = 11;
    static constexpr uint64 TYPE_DOUBLE  = 12;

public:
    DBFields(): _total_length(0) { }
    ~DBFields() { }

    // insert a field with known types data
    // usually used by upper layer control module
    void insert(const uint64 field_type, const uint64 field_length,
                const bool is_primary_key, const std::string& field_name) {
        _field_id.push_back(_field_id.size());
        _offset.push_back(_total_length);
        _field_type.push_back(field_type);
        _field_length.push_back(field_length);
        _is_primary_key.push_back(is_primary_key);
        _field_name.push_back(field_name);
        _total_length += field_length;
    }
    
    // insert a field with unknown types data
    // usually read from existing database
    void insert(const char* field_description, const uint64 length) {
        uint64 pos = 0;
        _field_id.push_back(*(pointer_convert<const uint64*>(field_description + pos)));
        pos += sizeof(uint64);

        _offset.push_back(_total_length);

        _field_type.push_back(*(pointer_convert<const uint64*>(field_description + pos)));
        pos += sizeof(uint64);

        _field_length.push_back(*(pointer_convert<const uint64*>(field_description + pos)));
        pos += sizeof(uint64);

        _is_primary_key.push_back(*(pointer_convert<const bool*>(field_description + pos)));
        pos += sizeof(bool);

        _field_name.push_back(std::string(field_description + pos, length - pos));
        _total_length += _field_length.back();
    }

    // generate description of field[i]
    // the buffer is supposed to clear by caller
    void generateFieldDescription(const uint64 i, char* buffer) const {
        uint64 pos = 0;
        // field id
        uint64 tmp = _field_id[i];
        memcpy(buffer + pos, &tmp, sizeof(uint64));
        pos += sizeof(uint64);
        // field type
        tmp = _field_type[i];
        memcpy(buffer + pos, &tmp, sizeof(uint64));
        pos += sizeof(uint64);
        // field length
        tmp = _field_length[i];
        memcpy(buffer + pos, &tmp, sizeof(uint64));
        pos += sizeof(uint64);
        // primary key
        *(buffer + pos) = _is_primary_key[i];
        pos += sizeof(bool);
        // field name
        memcpy(buffer + pos, _field_name[i].c_str(), _field_name[i].length());
    }

    // generate a record with fields info
    void generateRecord(const std::initializer_list<void*> args, char* buffer) const {
        uint64 i = 0;
        for (auto arg: args) {
            memcpy(buffer, arg, _field_length[i]);
            buffer += _field_length[i];
        }
    }

    // number of fields
    uint64 size() const {
        return _field_id.size();
    }

    // total length of each field
    uint64 recordLength() const {
        return _total_length;
    }

    // clear all saved info
    void clear() {
        _field_id.clear();
        _offset.clear();
        _field_type.clear();
        _field_length.clear();
        _is_primary_key.clear();
        _field_name.clear();
        _total_length = 0;
    }

private:
#ifdef DEBUG
public:
#endif
    const std::vector<uint64>& field_id() const {
        return _field_id;
    }

    const std::vector<uint64>& offset() const {
        return _offset;
    }

    const std::vector<uint64>& field_type() const {
        return _field_type;
    }
    
    const std::vector<uint64>& field_length() const {
        return _field_length;
    }

    const std::vector<bool>& primary_key() const {
        return _is_primary_key;
    }

    const std::vector<std::string>& field_name() const {
        return _field_name;
    }

    
private:
    std::vector<uint64> _field_id;
    std::vector<uint64> _offset;
    std::vector<uint64> _field_type;
    std::vector<uint64> _field_length;
    std::vector<bool> _is_primary_key;
    std::vector<std::string> _field_name;
    uint64 _total_length;
};

#endif /* DB_FIELDS_H_ */
