/******************************************************************************
 *  Copyright (c) 2014 Jamis Hoo 
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: Database
 *  Filename: db_fields.h 
 *  Version: 1.0
 *  Author: Jamis Hoo
 *  E-mail: hjm211324@gmail.com
 *  Date: Oct. 23, 2014
 *  Time: 17:51:54
 *  Description: Fields of a table.
 *****************************************************************************/
#ifndef DB_FIELDS_H_
#define DB_FIELDS_H_

#include <cstring>
#include <string>
#include <vector>
#include <cassert>
#include <tuple>
#include <limits>
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
    static constexpr uint64 TYPE_CHAR    = 9;
    static constexpr uint64 TYPE_UCHAR   = 10;
    static constexpr uint64 TYPE_FLOAT   = 11;
    static constexpr uint64 TYPE_DOUBLE  = 12;

    static constexpr uint64 FIELD_INFO_LENGTH = 256;
    static constexpr uint64 FIELD_NAME_LENGTH = 229;

    // <type name, is unsigned, is signed> -> <type, default length, explicit length required>
    static const std::map< std::tuple<std::string, bool, bool>, 
                           std::tuple<uint64, uint64, bool> > datatype_map;

    struct Comparator {
        uint64 type;
        // return 0 if a == b
        // return 1 if a > b
        // return -1 if a < b
        // 1st byte of a(b) is 00 means this is null
        // null value is larger than non-null value
        // null value is equal to null
        int operator()(const void* a, const void* b, const uint64 length) {
            char a_null_flag = pointer_convert<const char*>(a)[0];
            char b_null_flag = pointer_convert<const char*>(b)[0];
            if (a_null_flag == '\x00' && b_null_flag == '\x00') return 0;
            if (a_null_flag == '\x00' && b_null_flag != '\x00') return 1;
            if (a_null_flag != '\x00' && b_null_flag == '\x00') return -1;

            const void* a_data = pointer_convert<const char*>(a) + 1;
            const void* b_data = pointer_convert<const char*>(b) + 1;
            switch (type) {
                case 0:
                    return *pointer_convert<const int8_t*>(a_data) - 
                           *pointer_convert<const int8_t*>(b_data);
                case 1:
                    return *pointer_convert<const uint8_t*>(a_data) -
                           *pointer_convert<const uint8_t*>(b_data);
                case 2:
                    return *pointer_convert<const int16_t*>(a_data) - 
                           *pointer_convert<const int16_t*>(b_data);
                case 3:
                    return *pointer_convert<const uint16_t*>(a_data) - 
                           *pointer_convert<const uint16_t*>(b_data);
                case 4:
                    return *pointer_convert<const int32_t*>(a_data) -
                           *pointer_convert<const int32_t*>(b_data);
                case 5: {
                    uint32_t aa = *pointer_convert<const uint32_t*>(a_data);
                    uint32_t bb = *pointer_convert<const uint32_t*>(b_data);
                    if (aa > bb) return 1;
                    if (aa < bb) return -1;
                    return 0;
                }
                case 6: {
                    int64_t aa = *pointer_convert<const int64_t*>(a_data);
                    int64_t bb = *pointer_convert<const int64_t*>(b_data);
                    if (aa > bb) return 1;
                    if (aa < bb) return -1;
                    return 0;
                }
                case 7: {
                    uint64_t aa = *pointer_convert<const uint64_t*>(a_data);
                    uint64_t bb = *pointer_convert<const uint64_t*>(b_data);
                    if (aa > bb) return 1;
                    if (aa < bb) return -1;
                    return 0;
                }
                case 8:
                    return bool(*pointer_convert<const bool*>(a_data)) -
                           bool(*pointer_convert<const bool*>(b_data));
                case 9:
                case 10:
                    return memcmp(a, b, length - 1);
                case 11: {
                    float aa = *pointer_convert<const float*>(a_data);
                    float bb = *pointer_convert<const float*>(b_data);
                    if (aa > bb) return 1;
                    if (aa < bb) return -1;
                    return 0;
                }
                case 12: {
                    double aa = *pointer_convert<const double*>(a_data);
                    double bb = *pointer_convert<const double*>(b_data);
                    if (aa > bb) return 1;
                    if (aa < bb) return -1;
                    return 0;
                }
                default:
                    assert(0);
            }
        }
    };

public:
    DBFields(): _total_length(0), 
                _primary_key_field_id(std::numeric_limits<decltype(_primary_key_field_id)>::max()) { }
    ~DBFields() { }

    // insert a field with known types data
    // usually used by upper layer control module
    void insert(const uint64 field_type, const uint64 field_length,
                const bool is_primary_key, const bool indexed, 
                const bool notnull, const std::string& field_name) {
        _field_id.push_back(_field_id.size());
        _offset.push_back(_total_length);
        _field_type.push_back(field_type);
        // length + 1, because the first byte is null flag
        _field_length.push_back(field_length + 1);
        _indexed.push_back(indexed);
        // primary key cannot be null
        _notnull.push_back(notnull || is_primary_key);
        assert(!is_primary_key || notnull);
        if (is_primary_key) _primary_key_field_id = _field_id.back();
        if (field_name.length() > FIELD_NAME_LENGTH)
            _field_name.push_back(field_name.substr(0, FIELD_NAME_LENGTH));
        else
            _field_name.push_back(field_name);
        _total_length += field_length + 1;
    }
    
    // insert a field with unknown types data
    // usually read from existing database
    void insert(const char* field_description) {
        uint64 pos = 0;
        _field_id.push_back(*pointer_convert<const uint64*>(field_description + pos));
        pos += sizeof(uint64);

        _offset.push_back(_total_length);

        _field_type.push_back(*pointer_convert<const uint64*>(field_description + pos));
        pos += sizeof(uint64);

        _field_length.push_back(*pointer_convert<const uint64*>(field_description + pos));
        pos += sizeof(uint64);
  
        if (*pointer_convert<const bool*>(field_description + pos)) 
            _primary_key_field_id = _field_id.back();
        pos += sizeof(bool);

        _indexed.push_back(bool(field_description[pos] != '\x00'));
        pos += sizeof(bool);

        _notnull.push_back(bool(field_description[pos] != '\x00'));
        pos += sizeof(bool);

        _field_name.push_back(std::string(field_description + pos));
        if (_field_name.back().length() > FIELD_NAME_LENGTH)
            _field_name.back().substr(0, FIELD_NAME_LENGTH);
        _total_length += _field_length.back();
    }
    
    // generate description of field[i]
    // the buffer is supposed to be cleared by caller
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
        *(buffer + pos) = i == _primary_key_field_id;
        pos += sizeof(bool);
        // indexed
        buffer[pos] = _indexed[i];
        pos += sizeof(bool);
        // not null
        buffer[pos] = _notnull[i];
        pos += sizeof(bool);
        // field name
        memcpy(buffer + pos, _field_name[i].c_str(), _field_name[i].length());
    }

    // generate a record with fields info
    // void generateRecord(const std::initializer_list<void*> args, char* buffer) const {
    void generateRecord(const std::vector<void*> args, char* buffer) const {
        uint64 i = 0;
        for (const auto arg: args) {
            memcpy(buffer, arg, _field_length[i]);
            buffer += _field_length[i];
            ++i;
        }
    }

    // check whether there's a primary key in all fields
    // returns 1 if there's
    // returns 0 otherwise
    bool hasPrimaryKey() const {
        if (std::find(_field_id.begin(), _field_id.end(), _primary_key_field_id) != _field_id.end())
            return 1;
        return 0;
    }

    // add primary key if all fields are not primary key
    // returns 1 if primary key is added
    // returns 0 if there'is already a primary key
    bool addPrimaryKey() {
        // find primary key field id
        if (hasPrimaryKey()) return 0;

        _field_id.push_back(_field_id.size());
        _offset.push_back(_total_length);
        _field_type.push_back(TYPE_UINT64);
        _field_length.push_back(sizeof(uint64) + 1);
        _indexed.push_back(true);
        _notnull.push_back(true);
        _primary_key_field_id = _field_id.back();
        _field_name.push_back("");
        _total_length += sizeof(uint64) + 1;
        
        return 1;
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
        _field_name.clear();
        _indexed.clear();
        _notnull.clear();
        _total_length = 0;
    }

public:
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

    const std::vector<std::string>& field_name() const {
        return _field_name;
    }

    const std::vector<bool> indexed() const {
        return _indexed;
    }

    const std::vector<bool> notnull() const {
        return _notnull;
    }

    uint64 primary_key_field_id() const {
        return _primary_key_field_id;
    }

private:
    std::vector<uint64> _field_id;
    std::vector<uint64> _offset;
    std::vector<uint64> _field_type;
    std::vector<uint64> _field_length;
    // if field name is empty, then this is an unnamed primary key.
    std::vector<std::string> _field_name;
    std::vector<bool> _indexed;
    std::vector<bool> _notnull;
    uint64 _total_length;
    uint64 _primary_key_field_id;

};

constexpr Database::uint64 Database::DBFields::TYPE_UINT64;

// TODO: move this to a seperate cc file.
const std::map< std::tuple<std::string, bool, bool>, 
                           std::tuple<Database::uint64, Database::uint64, bool> > Database::DBFields::datatype_map = { 
        // (type name, is unsigned, is signed) ------> (type number, length, must provide a length)
        // 8 bit int, defalut signed
        { std::make_tuple("int8", 0, 0),       std::make_tuple( 0, 1, 0) },
        // signed 8 bit int
        { std::make_tuple("int8", 0, 1),       std::make_tuple( 0, 1, 0) },
        // unsigned 8 bit int
        { std::make_tuple("int8", 1, 0),       std::make_tuple( 1, 1, 0) },
        { std::make_tuple("int16", 0, 0),      std::make_tuple( 2, 2, 0) },
        { std::make_tuple("int16", 0, 1),      std::make_tuple( 2, 2, 0) },
        { std::make_tuple("int16", 1, 0),      std::make_tuple( 3, 2, 0) },
        { std::make_tuple("int32", 0, 0),      std::make_tuple( 4, 4, 0) },
        { std::make_tuple("int32", 0, 1),      std::make_tuple( 4, 4, 0) },
        { std::make_tuple("int32", 1, 0),      std::make_tuple( 5, 4, 0) },
        { std::make_tuple("int64", 0, 0),      std::make_tuple( 6, 8, 0) },
        { std::make_tuple("int64", 0, 1),      std::make_tuple( 6, 8, 0) },
        { std::make_tuple("int64", 1, 0),      std::make_tuple( 7, 8, 0) },
        // signed, unsigned cannot apply to bool and char(varchar)
        { std::make_tuple("bool", 0, 0),       std::make_tuple( 8, 1, 0) },
        { std::make_tuple("char", 0, 0),       std::make_tuple(10, 0, 1) },
        // unsigned float and double not supported.
        { std::make_tuple("float", 0, 0),      std::make_tuple(11, 4, 0) },
        { std::make_tuple("float", 0, 1),      std::make_tuple(11, 4, 0) },
        { std::make_tuple("double", 0, 0),     std::make_tuple(12, 8, 0) },
        { std::make_tuple("double", 0, 1),     std::make_tuple(12, 8, 0) }
};

#endif /* DB_FIELDS_H_ */
