/******************************************************************************
 *  Copyright (c) 2014 Jinming Hu 
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: Database
 *  Filename: common.h 
 *  Version: 1.0
 *  Author: Jinming Hu
 *  E-mail: hjm211324@gmail.com
 *  Date: Oct. 22, 2014
 *  Time: 19:01:38
 *  Description: Universal types, constants, declarations.
 *****************************************************************************/
#ifndef COMMON_H_
#define COMMON_H_

namespace Database {

using int64 = long long;
using uint64 = unsigned long long;


class DBFile;
class DBTableManager;
class DBFields;

struct RID {
    uint64 pageID;
    uint64 slotID;

    inline bool operator==(const RID& rid) {
        return pageID == rid.pageID && slotID == rid.slotID;
    }

    inline bool operator!=(const RID& rid) {
        return !(*this == rid);
    }
};

// Maybe there's some protable problem with reinterpret_cast<T>()?
// Use the functions below to replace it.
template <class T>
T pointer_convert(void* pointer) {
    return static_cast<T>(static_cast<void*>(pointer));
}

template <class T>
T pointer_convert(const void* pointer) {
    return static_cast<T>(static_cast<const void*>(pointer));
}

} // namespace Database

#endif /* COMMON_H_ */
