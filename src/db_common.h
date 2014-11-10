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
class DBBuffer;
template<class /* Comparator */> class DBIndexManager;

struct RID {
    uint64 pageID;
    uint64 slotID;

    RID(const uint64 pageid, const uint64 slotid): 
        pageID(pageid), slotID(slotid) { }

    inline bool operator==(const RID& rid) {
        return pageID == rid.pageID && slotID == rid.slotID;
    }

    inline bool operator!=(const RID& rid) {
        return !(*this == rid);
    }

    // conversion to bool
    operator bool() {
        return pageID;
    }
};

#ifdef DEBUG
std::ostream& operator<<(std::ostream& os, const RID& rid) {
    os << "(" << rid.pageID << ", " << rid.slotID << ")";
    return os;
}
#endif

// Maybe there's some protable problems with reinterpret_cast<T>()?
// Use the functions below to replace it.
template <class T, class T2>
T pointer_convert(T2* pointer) {
    return static_cast<T>(static_cast<void*>(pointer));
}

template <class T, class T2>
T pointer_convert(const T2* pointer) {
    return static_cast<T>(static_cast<const void*>(pointer));
}

} // namespace Database

#endif /* COMMON_H_ */
