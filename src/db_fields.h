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

    // insert field 
    void insertField(const uint64 field_type, const uint64 field_length,
                     const bool is_primary_key, const std::string& field_name) {
        _field_id.push_back(_field_id.size());
        _field_type.push_back(field_type);
        _field_length.push_back(field_length);
        _is_primary_key.push_back(is_primary_key);
        _field_name.push_back(field_name);
        _total_length += field_length;
    }

    uint64 size() const {
        return _field_id.size();
    }

    const std::vector<uint64>& field_id() const {
        return _field_id;
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

    uint64 totalLength() const {
        return _total_length;
    }

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
        ++pos;
        // field name
        memcpy(buffer + pos, _field_name[i].c_str(), _field_name[i].length());
    }

private:
    std::vector<uint64> _field_id;
    std::vector<uint64> _field_type;
    std::vector<uint64> _field_length;
    std::vector<bool> _is_primary_key;
    std::vector<std::string> _field_name;
    uint64 _total_length;
};

#endif /* DB_FIELDS_H_ */
