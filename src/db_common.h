/******************************************************************************
 *  Copyright (c) 2014-2015 Jamis Hoo 
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: Database
 *  Filename: common.h 
 *  Version: 1.0
 *  Author: Jamis Hoo
 *  E-mail: hjm211324@gmail.com
 *  Date: Oct. 22, 2014
 *  Time: 19:01:38
 *  Description: Universal types, constants, declarations.
 *****************************************************************************/
#ifndef COMMON_H_
#define COMMON_H_

#include <boost/chrono.hpp>

namespace Database {

using int64 = long long;
using uint64 = unsigned long long;


class DBFile;
class DBTableManager;
class DBFields;
class DBBuffer;
template<class /* Comparator */> class DBIndexManager;
class DBQuery;
class DBInterface;
class AlignedOutputer;

struct RID {
    uint64 pageID;
    uint64 slotID;

    RID(const uint64 pageid, const uint64 slotid): 
        pageID(pageid), slotID(slotid) { }

    inline bool operator==(const RID& rid) const {
        return pageID == rid.pageID && slotID == rid.slotID;
    }

    inline bool operator!=(const RID& rid) const {
        return !(*this == rid);
    }

    inline bool operator<(const RID& rid) const {
        return pageID == rid.pageID? slotID < rid.slotID: pageID < rid.pageID;
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


/* 
   Platform-independent timer
   Function uniqueNumber will return current wall-clock time with an accuracy 
   of nanosecond. That is, numbers won't duplicate even in two successive 
   calls on GHz-level CPU.
  
   On most hardware platforms, system_clock doesn't support nanosecond-accuracy 
   while high_resolution_clock doesn't ensure the same epoch with system_clock. 
   The function below works as long as nanosecond-accuracy is supported. 
   For fear that nanosecond-accurary clock isn't supported by  
   either hardware or software, doubel-check is still recommended.
*/
// move these definitions to seperate cc file if multi-definition errors when compiling
const boost::chrono::system_clock::duration sys_clock_epoch = 
    boost::chrono::system_clock::now() - boost::chrono::system_clock::from_time_t(0);
const boost::chrono::high_resolution_clock::time_point hr_clock_epoch = 
    boost::chrono::high_resolution_clock::now();

template <class T = uint64>
T uniqueNumber() {
    return boost::chrono::duration_cast<boost::chrono::nanoseconds>(sys_clock_epoch).count() +
           boost::chrono::duration_cast<boost::chrono::nanoseconds>(boost::chrono::high_resolution_clock::now() - hr_clock_epoch).count();

}

} // namespace Database

#endif /* COMMON_H_ */
