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
#include <algorithm>
#include <map>
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
    // <type code> -> <type name>
    static const std::map< uint64, std::string > datatype_name_map;

    struct MinGenerator {
        // generate minimum value of type type, save to buff
        // assert buff is cleared by caller
        void operator()(const uint64 type, void* buff, const uint64 length) const {
            // not null flag
            pointer_convert<char*>(buff)[0] = '\xff';
            char* data = pointer_convert<char*>(buff) + 1;
            switch (type) {
                case TYPE_INT8: {
                    int8_t x = std::numeric_limits<int8_t>::min();
                    memcpy(data, &x, sizeof(int8_t));
                    break;
                } case TYPE_UINT8: {
                    uint8_t x = std::numeric_limits<uint8_t>::min();
                    memcpy(data, &x, sizeof(uint8_t));
                    break;
                } case TYPE_INT16: {
                    int16_t x = std::numeric_limits<int16_t>::min();
                    memcpy(data, &x, sizeof(int16_t));
                    break;
                } case TYPE_UINT16: {
                    uint16_t x = std::numeric_limits<uint16_t>::min();
                    memcpy(data, &x, sizeof(uint16_t));
                    break;
                } case TYPE_INT32: {
                    int32_t x = std::numeric_limits<int32_t>::min();
                    memcpy(data, &x, sizeof(int32_t));
                    break;
                } case TYPE_UINT32: {
                    uint32_t x = std::numeric_limits<uint32_t>::min();
                    memcpy(data, &x, sizeof(uint32_t));
                    break;
                } case TYPE_INT64: {
                    int64_t x = std::numeric_limits<int64_t>::min();
                    memcpy(data, &x, sizeof(int64_t));
                    break;
                } case TYPE_UINT64: {
                    uint64_t x = std::numeric_limits<uint64_t>::min();
                    memcpy(data, &x, sizeof(uint64_t));
                    break;
                } case TYPE_BOOL: {
                    bool x = std::numeric_limits<bool>::min();
                    memcpy(data, &x, sizeof(bool));
                    break;
                } case TYPE_CHAR: 
                  case TYPE_UCHAR: 
                    memset(data, 0x00, length);
                    break;
                  case TYPE_FLOAT: {
                    float x = std::numeric_limits<float>::lowest();
                    memcpy(data, &x, sizeof(float));
                    break;
                } case TYPE_DOUBLE: {
                    double x = std::numeric_limits<double>::lowest();
                    memcpy(data, &x, sizeof(double));
                    break;
                }
            }
        }
    };
    
    struct LiteralParser {
        // parse string(data) as Type(type), save the result to buffer
        // result contains null flag
        // ASSERT buff is cleared by caller
        // returns 0 if parse succeed.
        // returns 1 if parse failed
        // returns 2 if out of range
        int operator()(const std::string& data, const uint64 type, const uint64 length, void* buffer) const {
            std::string str;
            std::transform(data.begin(), data.end(), std::back_inserter(str), ::tolower);
            if (str == "true") str = "1";
            else if (str == "false") str = "0";
            else if (str == "null") return 0; 
            else str = data;

            char* buff = pointer_convert<char*>(buffer);
            // not null flag
            buff[0] = '\xff';
            ++buff;

            switch (type) {
                case TYPE_INT8:
                case TYPE_INT16:
                case TYPE_INT32:
                case TYPE_INT64: {
                    intmax_t x;
                    try {
                        x = std::stoll(str);
                    } catch (const std::invalid_argument&) {
                        return 1;
                    } catch (const std::out_of_range&) {
                        return 2;
                    }
                    switch (type) {
                        case TYPE_INT8: {
                            if (x < std::numeric_limits<int8_t>::min() || x > std::numeric_limits<int8_t>::max())
                                return 2;
                            int8_t xx = x;
                            memcpy(buff, &xx, sizeof(int8_t));
                            return 0;
                        } case TYPE_INT16: {
                            if (x < std::numeric_limits<int16_t>::min() || x > std::numeric_limits<int16_t>::max())
                                return 2;
                            int16_t xx = x;
                            memcpy(buff, &xx, sizeof(int16_t));
                            return 0;
                        } case TYPE_INT32: {
                            if (x < std::numeric_limits<int32_t>::min() || x > std::numeric_limits<int32_t>::max())
                                return 2;
                            int32_t xx = x;
                            memcpy(buff, &xx, sizeof(int32_t));
                            return 0;
                        } case TYPE_INT64: {
                            if (x < std::numeric_limits<int64_t>::min() || x > std::numeric_limits<int64_t>::max())
                                return 2;
                            int64_t xx = x;
                            memcpy(buff, &xx, sizeof(int64_t));
                            return 0;
                        } default:
                            assert(0);
                    }

                }
                case TYPE_UINT8:
                case TYPE_UINT16:
                case TYPE_UINT32:
                case TYPE_UINT64: {
                    uintmax_t x;
                    try {
                        x = std::stoull(str);
                    } catch (const std::invalid_argument&) {
                        return 1;
                    } catch (const std::out_of_range&) {
                        return 2;
                    }
                    switch (type) {
                        case TYPE_UINT8: {
                            if (x > std::numeric_limits<uint8_t>::max())
                                return 2;
                            uint8_t xx = x;
                            memcpy(buff, &xx, sizeof(uint8_t));
                            return 0;
                        } case TYPE_UINT16: {
                            if (x > std::numeric_limits<uint16_t>::max())
                                return 2;
                            uint16_t xx = x;
                            memcpy(buff, &xx, sizeof(uint16_t));
                            return 0;
                        } case TYPE_UINT32: {
                            if (x > std::numeric_limits<uint32_t>::max())
                                return 2;
                            uint32_t xx = x;
                            memcpy(buff, &xx, sizeof(uint32_t));
                            return 0;
                        } case TYPE_UINT64: {
                            if (x > std::numeric_limits<uint64_t>::max())
                                return 2;
                            uint64_t xx = x;
                            memcpy(buff, &xx, sizeof(uint64_t));
                            return 0;
                        } default:
                            assert(0);
                    }

                }
                case TYPE_BOOL: {
                    intmax_t x;
                    try {
                        x = std::stoull(str);
                    } catch (const std::invalid_argument&) {
                        return 1;
                    } catch (const std::out_of_range&) {
                        return 2;
                    }
                    bool y = x? true: false;
                    memcpy(buff, &y, sizeof(bool));
                    return 0;
                } case TYPE_CHAR: 
                  case TYPE_UCHAR:
                    if (str.length() < 2) return 1;
                    if (str.front() != '\'' || str.back() != '\'') return 1;
                    if (str.length() > length + 2) return 2;
                    // don't copy single quotes
                    memcpy(buff, str.data() + 1, str.length() - 2);
                    return 0;
                case TYPE_FLOAT: {
                    float x;
                    try {
                        x = stof(str);
                    } catch (const std::invalid_argument&) {
                        return 1;
                    } catch (const std::out_of_range&) {
                        return 2;
                    }
                    memcpy(buff, &x, sizeof(float));
                    return 0;
                } case TYPE_DOUBLE: {
                    double x;
                    try {
                        x = stod(str);
                    } catch (const std::invalid_argument&) {
                        return 1;
                    } catch (const std::out_of_range&) {
                        return 2;
                    }
                    memcpy(buff, &x, sizeof(double));
                    return 0;
                }
                default:
                    assert(0);
            }
        }

        // convert raw data of type to literal string str
        // returns 0 if succeeded
        // returns 1 if failed
        int operator()(const void* data, const uint64 type, const uint64 length,  std::string& str, bool* isnull = nullptr) const {
            if (isnull) *isnull = false;
            if (pointer_convert<const char*>(data)[0] == '\x00') {
                if (isnull) *isnull = true;
                str = "NULL";
                return 0;
            }
            if (pointer_convert<const char*>(data)[0] != '\xff') 
                return 1;
            const char* buff = pointer_convert<const char*>(data) + 1;
            switch (type) {
                case TYPE_INT8: 
                    str = std::to_string(*pointer_convert<const int8_t*>(buff));
                    break;
                case TYPE_UINT8:
                    str = std::to_string(*pointer_convert<const uint8_t*>(buff));
                    break;
                case TYPE_INT16:
                    str = std::to_string(*pointer_convert<const int16_t*>(buff));
                    break;
                case TYPE_UINT16:
                    str = std::to_string(*pointer_convert<const uint16_t*>(buff));
                    break;
                case TYPE_INT32:
                    str = std::to_string(*pointer_convert<const int32_t*>(buff));
                    break;
                case TYPE_UINT32:
                    str = std::to_string(*pointer_convert<const uint32_t*>(buff));
                    break;
                case TYPE_INT64:
                    str = std::to_string(*pointer_convert<const int64_t*>(buff));
                    break;
                case TYPE_UINT64:
                    str = std::to_string(*pointer_convert<const uint64_t*>(buff));
                    break;
                case TYPE_BOOL:
                    str = *pointer_convert<const bool*>(buff)? "TRUE": "FALSE";
                    break;
                case TYPE_CHAR:
                case TYPE_UCHAR:
                    str.assign(buff, length - 1);
                    str.erase(str.find_last_not_of('\x00') + 1);
                    break;
                case TYPE_FLOAT:
                    str = std::to_string(*pointer_convert<const float*>(buff));
                    break;
                case TYPE_DOUBLE:
                    str = std::to_string(*pointer_convert<const double*>(buff));
                    break;
            }
            return 0;
        }
    };

    struct Aggregator {
        // returns 0 if succeed, 1 if type error
        int sum(const std::vector<void*>& data, const uint64 offset, 
                const uint64 type, const uint64 length, void* result) const {
            uint64 count;
            switch (type) {
                case TYPE_INT8: {
                    int8_t res = 0;
                    count = getSum<int8_t>(data, offset, res);
                    memcpy(pointer_convert<char*>(result) + 1, &res, length - 1);
                    pointer_convert<char*>(result)[0] = '\xff';
                    break;
                } case TYPE_UINT8: {
                    uint8_t res = 0;
                    count = getSum<uint8_t>(data, offset, res);
                    memcpy(pointer_convert<char*>(result) + 1, &res, length - 1);
                    pointer_convert<char*>(result)[0] = '\xff';
                    break;
                } case TYPE_INT16: {
                    int16_t res = 0;
                    count = getSum<int16_t>(data, offset, res);
                    memcpy(pointer_convert<char*>(result) + 1, &res, length - 1);
                    pointer_convert<char*>(result)[0] = '\xff';
                    break;
                } case TYPE_UINT16: {
                    uint16_t res = 0;
                    count = getSum<uint16_t>(data, offset, res);
                    memcpy(pointer_convert<char*>(result) + 1, &res, length - 1);
                    pointer_convert<char*>(result)[0] = '\xff';
                    break;
                } case TYPE_INT32: {
                    int32_t res = 0;
                    count = getSum<int32_t>(data, offset, res);
                    memcpy(pointer_convert<char*>(result) + 1, &res, length - 1);
                    pointer_convert<char*>(result)[0] = '\xff';
                    break;
                } case TYPE_UINT32: {
                    uint32_t res = 0;
                    count = getSum<uint32_t>(data, offset, res);
                    memcpy(pointer_convert<char*>(result) + 1, &res, length - 1);
                    pointer_convert<char*>(result)[0] = '\xff';
                    break;
                } case TYPE_INT64: {
                    int64_t res = 0;
                    count = getSum<int64_t>(data, offset, res);
                    memcpy(pointer_convert<char*>(result) + 1, &res, length - 1);
                    pointer_convert<char*>(result)[0] = '\xff';
                    break;
                } case TYPE_UINT64: {
                    uint64_t res = 0;
                    count = getSum<uint64_t>(data, offset, res);
                    memcpy(pointer_convert<char*>(result) + 1, &res, length - 1);
                    pointer_convert<char*>(result)[0] = '\xff';
                    break;
                } case TYPE_FLOAT: {
                    float res = 0;
                    count = getSum<float>(data, offset, res);
                    memcpy(pointer_convert<char*>(result) + 1, &res, length - 1);
                    pointer_convert<char*>(result)[0] = '\xff';
                    break;
                } case TYPE_DOUBLE: {
                    double res = 0;
                    count = getSum<double>(data, offset, res);
                    memcpy(pointer_convert<char*>(result) + 1, &res, length - 1);
                    pointer_convert<char*>(result)[0] = '\xff';
                    break;
                } default: 
                    return 1;
            }
            if (!count) memset(result, 0x00, length);
            return 0;
        }
        int avg(const std::vector<void*>& data, const uint64 offset, 
                const uint64 type, const uint64 length, void* result) const {
            uint64 count;
            switch (type) {
                case TYPE_INT8: {
                    int8_t res = 0;
                    count = getAvg<int8_t>(data, offset, res);
                    memcpy(pointer_convert<char*>(result) + 1, &res, length);
                    pointer_convert<char*>(result)[0] = '\xff';
                    break;
                } case TYPE_UINT8: {
                    uint8_t res = 0;
                    count = getAvg<uint8_t>(data, offset, res);
                    memcpy(pointer_convert<char*>(result) + 1, &res, length);
                    pointer_convert<char*>(result)[0] = '\xff';
                    break;
                } case TYPE_INT16: {
                    int16_t res = 0;
                    count = getAvg<int16_t>(data, offset, res);
                    memcpy(pointer_convert<char*>(result) + 1, &res, length);
                    pointer_convert<char*>(result)[0] = '\xff';
                    break;
                } case TYPE_UINT16: {
                    uint16_t res = 0;
                    count = getAvg<uint16_t>(data, offset, res);
                    memcpy(pointer_convert<char*>(result) + 1, &res, length);
                    pointer_convert<char*>(result)[0] = '\xff';
                    break;
                } case TYPE_INT32: {
                    int32_t res = 0;
                    count = getAvg<int32_t>(data, offset, res);
                    memcpy(pointer_convert<char*>(result) + 1, &res, length);
                    pointer_convert<char*>(result)[0] = '\xff';
                    break;
                } case TYPE_UINT32: {
                    uint32_t res = 0;
                    count = getAvg<uint32_t>(data, offset, res);
                    memcpy(pointer_convert<char*>(result) + 1, &res, length);
                    pointer_convert<char*>(result)[0] = '\xff';
                    break;
                } case TYPE_INT64: {
                    int64_t res = 0;
                    count = getAvg<int64_t>(data, offset, res);
                    memcpy(pointer_convert<char*>(result) + 1, &res, length);
                    pointer_convert<char*>(result)[0] = '\xff';
                    break;
                } case TYPE_UINT64: {
                    uint64_t res = 0;
                    count = getAvg<uint64_t>(data, offset, res);
                    memcpy(pointer_convert<char*>(result) + 1, &res, length);
                    pointer_convert<char*>(result)[0] = '\xff';
                    break;
                } case TYPE_FLOAT: {
                    float res = 0;
                    count = getAvg<float>(data, offset, res);
                    memcpy(pointer_convert<char*>(result) + 1, &res, length);
                    pointer_convert<char*>(result)[0] = '\xff';
                    break;
                } case TYPE_DOUBLE: {
                    double res = 0;
                    count = getAvg<double>(data, offset, res);
                    memcpy(pointer_convert<char*>(result) + 1, &res, length);
                    pointer_convert<char*>(result)[0] = '\xff';
                    break;
                } default: 
                    return 1;
            }
            if (!count) memset(result, 0x00, length);
            return 0;
        }
        int max(const std::vector<void*>& data, const uint64 offset, 
                const uint64 type, const uint64 length, void* result) const {
            MinGenerator minGenerator;
            minGenerator(type, result, length);
            uint64 count;
            switch (type) {
                case TYPE_INT8: 
                    count = getMax<int8_t>(data, type, offset, length, result);
                    break;
                case TYPE_UINT8: 
                    count = getMax<uint8_t>(data, type, offset, length, result);
                    break;
                case TYPE_INT16:
                    count = getMax<int16_t>(data, type, offset, length, result);
                    break;
                case TYPE_UINT16:
                    count = getMax<uint16_t>(data, type, offset, length, result);
                    break;
                case TYPE_INT32:
                    count = getMax<int32_t>(data, type, offset, length, result);
                    break;
                case TYPE_UINT32:
                    count = getMax<uint32_t>(data, type, offset, length, result);
                    break;
                case TYPE_INT64:
                    count = getMax<int64_t>(data, type, offset, length, result);
                    break;
                case TYPE_UINT64:
                    count = getMax<uint64_t>(data, type, offset, length, result);
                    break;
                case TYPE_BOOL:
                    count = getMax<bool>(data, type, offset, length, result);
                    break;
                case TYPE_CHAR:
                    count = getMax<char>(data, type, offset, length, result);
                    break;
                case TYPE_UCHAR:
                    count = getMax<unsigned char>(data, type, offset, length, result);
                    break;
                case TYPE_FLOAT:
                    count = getMax<float>(data, type, offset, length, result);
                    break;
                case TYPE_DOUBLE:
                    count = getMax<double>(data, type, offset, length, result);
                    break;
                default:
                    return 1;
            }
            if (!count) memset(result, 0x00, length);
            return 0;
        }
        int min(const std::vector<void*>& data, const uint64 offset, 
                const uint64 type, const uint64 length, void* result) const {
            memset(result, 0x00, length);
            uint64 count;
            switch (type) {
                case TYPE_INT8: 
                    count = getMin<int8_t>(data, type, offset, length, result);
                    break;
                case TYPE_UINT8: 
                    count = getMin<uint8_t>(data, type, offset, length, result);
                    break;
                case TYPE_INT16:
                    count = getMin<int16_t>(data, type, offset, length, result);
                    break;
                case TYPE_UINT16:
                    count = getMin<uint16_t>(data, type, offset, length, result);
                    break;
                case TYPE_INT32:
                    count = getMin<int32_t>(data, type, offset, length, result);
                    break;
                case TYPE_UINT32:
                    count = getMin<uint32_t>(data, type, offset, length, result);
                    break;
                case TYPE_INT64:
                    count = getMin<int64_t>(data, type, offset, length, result);
                    break;
                case TYPE_UINT64:
                    count = getMin<uint64_t>(data, type, offset, length, result);
                    break;
                case TYPE_BOOL:
                    count = getMin<bool>(data, type, offset, length, result);
                    break;
                case TYPE_CHAR:
                    count = getMin<char>(data, type, offset, length, result);
                    break;
                case TYPE_UCHAR:
                    count = getMin<unsigned char>(data, type, offset, length, result);
                    break;
                case TYPE_FLOAT:
                    count = getMin<float>(data, type, offset, length, result);
                    break;
                case TYPE_DOUBLE:
                    count = getMin<double>(data, type, offset, length, result);
                    break;
                default:
                    return 1;
            }
            if (!count) memset(result, 0x00, length);
            return 0;
        }

    private:
        template <class T>
        uint64 getSum(const std::vector<void*>& data, const uint64 offset, T& initial) const {
            uint64 count = 0;
            for (const auto p: data) {
                // if is null, ignore
                if (pointer_convert<const char*>(p)[offset] == '\x00') continue;
                ++count;
                initial += *pointer_convert<const T*>(pointer_convert<const char*>(p) + 1 + offset);
            }
            return count;
        }
        template <class T>
        uint64 getAvg(const std::vector<void*>& data, const uint64 offset ,T& initial) const {
            uint64 count = getSum(data, offset, initial);
            if (count) initial = initial / count;
            return count;
        }
        template <class T>
        uint64 getMax(const std::vector<void*>& data, const uint64 type, const uint64 offset, const uint64 length, void* initial) const {
            Comparator comp;
            comp.type = type;
            uint64 count = 0;
            for (std::size_t i = 0; i < data.size(); ++i) {
                if (pointer_convert<const char*>(data[i])[offset] == '\x00') continue;
                ++count;
                if (comp(pointer_convert<const char*>(data[i]) + offset, initial, length) > 0)
                    memcpy(initial, pointer_convert<const char*>(data[i]) + offset, length);
            }
            return count;
        }
        template <class T>
        uint64 getMin(const std::vector<void*>& data, const uint64 type, const uint64 offset, const uint64 length, void* initial) const {
            Comparator comp;
            comp.type = type;
            uint64 count = 0;
            for (std::size_t i = 0; i < data.size(); ++i) {
                if (pointer_convert<const char*>(data[i])[offset] == '\x00') continue;
                ++count;
                if (comp(pointer_convert<const char*>(data[i]) + offset, initial, length) < 0)
                    memcpy(initial, pointer_convert<const char*>(data[i]) + offset, length);
            }
            return count;
        }
    };

    struct Comparator {
        uint64 type;
        // return 0 if a == b
        // return >=1 if a > b
        // return <=-1 if a < b
        // 1st byte of a(b) is 00 means this is null
        // null value is larger than non-null value
        // null value is equal to null
        int operator()(const void* a, const void* b, const uint64 length) const {
            char a_null_flag = pointer_convert<const char*>(a)[0];
            char b_null_flag = pointer_convert<const char*>(b)[0];
            if (a_null_flag == '\x00' && b_null_flag == '\x00') return 0;
            if (a_null_flag == '\x00' && b_null_flag != '\x00') return 1;
            if (a_null_flag != '\x00' && b_null_flag == '\x00') return -1;

            const void* a_data = pointer_convert<const char*>(a) + 1;
            const void* b_data = pointer_convert<const char*>(b) + 1;
            switch (type) {
                case 0: {
                    int8_t aa = *pointer_convert<const int8_t*>(a_data);
                    int8_t bb = *pointer_convert<const int8_t*>(b_data);
                    if (aa > bb) return 1;
                    if (aa < bb) return -1;
                    return 0;
                } case 1: {
                    uint8_t aa = *pointer_convert<const uint8_t*>(a_data);
                    uint8_t bb = *pointer_convert<const uint8_t*>(b_data);
                    if (aa > bb) return 1;
                    if (aa < bb) return -1;
                    return 0;
                } case 2: {
                    int16_t aa = *pointer_convert<const int16_t*>(a_data);
                    int16_t bb = *pointer_convert<const int16_t*>(b_data);
                    if (aa > bb) return 1;
                    if (aa < bb) return -1;
                    return 0;
                } case 3: {
                    uint16_t aa = *pointer_convert<const uint16_t*>(a_data);
                    uint16_t bb = *pointer_convert<const uint16_t*>(b_data);
                    if (aa > bb) return 1;
                    if (aa < bb) return -1;
                    return 0;
                } case 4: {
                    int32_t aa = *pointer_convert<const int32_t*>(a_data);
                    int32_t bb = *pointer_convert<const int32_t*>(b_data);
                    if (aa > bb) return 1;
                    if (aa < bb) return -1;
                    return 0;
                }
                case 5: {
                    uint32_t aa = *pointer_convert<const uint32_t*>(a_data);
                    uint32_t bb = *pointer_convert<const uint32_t*>(b_data);
                    if (aa > bb) return 1;
                    if (aa < bb) return -1;
                    return 0;
                } case 6: {
                    int64_t aa = *pointer_convert<const int64_t*>(a_data);
                    int64_t bb = *pointer_convert<const int64_t*>(b_data);
                    if (aa > bb) return 1;
                    if (aa < bb) return -1;
                    return 0;
                } case 7: {
                    uint64_t aa = *pointer_convert<const uint64_t*>(a_data);
                    uint64_t bb = *pointer_convert<const uint64_t*>(b_data);
                    if (aa > bb) return 1;
                    if (aa < bb) return -1;
                    return 0;
                } case 8:
                    return bool(*pointer_convert<const bool*>(a_data)) -
                           bool(*pointer_convert<const bool*>(b_data));
                case 9:
                case 10: {
                    // case insensive
                    static constexpr char charmap[] = {
                        '\000', '\001', '\002', '\003', '\004', '\005', '\006', '\007',
                        '\010', '\011', '\012', '\013', '\014', '\015', '\016', '\017',
                        '\020', '\021', '\022', '\023', '\024', '\025', '\026', '\027',
                        '\030', '\031', '\032', '\033', '\034', '\035', '\036', '\037',
                        '\040', '\041', '\042', '\043', '\044', '\045', '\046', '\047',
                        '\050', '\051', '\052', '\053', '\054', '\055', '\056', '\057',
                        '\060', '\061', '\062', '\063', '\064', '\065', '\066', '\067',
                        '\070', '\071', '\072', '\073', '\074', '\075', '\076', '\077',
                        '\100', '\141', '\142', '\143', '\144', '\145', '\146', '\147',
                        '\150', '\151', '\152', '\153', '\154', '\155', '\156', '\157',
                        '\160', '\161', '\162', '\163', '\164', '\165', '\166', '\167',
                        '\170', '\171', '\172', '\133', '\134', '\135', '\136', '\137',
                        '\140', '\141', '\142', '\143', '\144', '\145', '\146', '\147',
                        '\150', '\151', '\152', '\153', '\154', '\155', '\156', '\157',
                        '\160', '\161', '\162', '\163', '\164', '\165', '\166', '\167',
                        '\170', '\171', '\172', '\173', '\174', '\175', '\176', '\177',
                        '\200', '\201', '\202', '\203', '\204', '\205', '\206', '\207',
                        '\210', '\211', '\212', '\213', '\214', '\215', '\216', '\217',
                        '\220', '\221', '\222', '\223', '\224', '\225', '\226', '\227',
                        '\230', '\231', '\232', '\233', '\234', '\235', '\236', '\237',
                        '\240', '\241', '\242', '\243', '\244', '\245', '\246', '\247',
                        '\250', '\251', '\252', '\253', '\254', '\255', '\256', '\257',
                        '\260', '\261', '\262', '\263', '\264', '\265', '\266', '\267',
                        '\270', '\271', '\272', '\273', '\274', '\275', '\276', '\277',
                        '\300', '\301', '\302', '\303', '\304', '\305', '\306', '\307',
                        '\310', '\311', '\312', '\313', '\314', '\315', '\316', '\317',
                        '\320', '\321', '\322', '\323', '\324', '\325', '\326', '\327',
                        '\330', '\331', '\332', '\333', '\334', '\335', '\336', '\337',
                        '\340', '\341', '\342', '\343', '\344', '\345', '\346', '\347',
                        '\350', '\351', '\352', '\353', '\354', '\355', '\356', '\357',
                        '\360', '\361', '\362', '\363', '\364', '\365', '\366', '\367',
                        '\370', '\371', '\372', '\373', '\374', '\375', '\376', '\377',
                    }; 
                    uint64 n = length - 1;
                    if (n != 0) {
                        const unsigned char *cm =  (const unsigned char*)charmap,
                                            *us1 = (const unsigned char*)a,
                                            *us2 = (const unsigned char*)b;
                        do {
                            if (cm[*us1] != cm[*us2++])
                                return (cm[*us1] - cm[*--us2]);
                            if (*us1++ == '\0') break;
                        } while (--n != 0);
                    }
                    return 0;
                    // case sensitive
                    // return memcmp(a, b, length - 1);
                } case 11: {
                    float aa = *pointer_convert<const float*>(a_data);
                    float bb = *pointer_convert<const float*>(b_data);
                    if (aa > bb) return 1;
                    if (aa < bb) return -1;
                    return 0;
                } case 12: {
                    double aa = *pointer_convert<const double*>(a_data);
                    double bb = *pointer_convert<const double*>(b_data);
                    if (aa > bb) return 1;
                    if (aa < bb) return -1;
                    return 0;
                } default:
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
            _field_name.back() = _field_name.back().substr(0, FIELD_NAME_LENGTH);
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
        memcpy(buffer + pos, _field_name[i].data(), _field_name[i].length());
        pos += FIELD_NAME_LENGTH;
        buffer[pos] = '\x00';
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
    
    // remove auto-created primary key created by addPrimaryKey() function
    // returns 1 if primary key is rmeoved
    // returns 0 otherwise
    bool removePrimaryKey() {
        // check auto-created
        if (_field_name[_primary_key_field_id].length() != 0) return 0;
        assert(_primary_key_field_id == size() - 1);
        _field_id.pop_back();
        _offset.pop_back();
        _field_type.pop_back();
        _field_length.pop_back();
        _indexed.pop_back();
        _notnull.pop_back();
        _field_name.pop_back();
        _total_length -= sizeof(uint64) + 1;
        _primary_key_field_id = std::numeric_limits<decltype(_primary_key_field_id)>::max();
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

    std::vector<bool>& indexed() {
        return _indexed;
    }

    const std::vector<bool>& indexed() const {
        return _indexed;
    }

    const std::vector<bool>& notnull() const {
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

// TODO: move this to a seperate cc file.
constexpr Database::uint64 Database::DBFields::TYPE_UINT64;

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

const std::map< Database::uint64, std::string > Database::DBFields::datatype_name_map = {
    {  0, "TINY INT" },
    {  1, "UNSIGNED TINY INT" },
    {  2, "SMALL INT" },
    {  3, "UNSIGNED SMALL INT" },
    {  4, "INT" },
    {  5, "UNSIGNED INT" }, 
    {  6, "BIG INT" },
    {  7, "UNSIGNED BIG INT" },
    {  8, "BOOL" },
    { 10, "STRING" },
    { 11, "FLOAT" },
    { 12, "DOUBLE" }
};


#endif /* DB_FIELDS_H_ */
