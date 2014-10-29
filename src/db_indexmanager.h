/******************************************************************************
 *  Copyright (c) 2014 Jinming Hu 
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: Database
 *  Filename: db_indexmanager.h 
 *  Version: 1.0
 *  Author: Jinming Hu
 *  E-mail: hjm211324@gmail.com
 *  Date: Oct. 29, 2014
 *  Time: 19:27:26
 *  Description: Index manager, just a trial
 *****************************************************************************/
#ifndef DB_INDEXMANAGER_H_
#define DB_INDEXMANAGER_H_

#include <vector>
#include <iostream>
#include <fstream>
#include "db_common.h"

class Database::DBIndexManager {
public:

    DBIndexManager(const uint64 data_length): _data_length(data_length) { }

    ~DBIndexManager() {
        for (auto pt: _dataSet) delete[] pt;
    }

    // insert to an open index, modify B+Tree, write back to disk(or memory buffer)
    template<class COMPARATOR>
    void insertRecord(const void* data, const RID rid, COMPARATOR comparator) {
        char* buffer = new char[_data_length];
        memcpy(buffer, data, _data_length);

        std::vector<void*> tmp(_dataSet);
        std::vector<RID> tmp2(_ridSet);
        _dataSet.clear();
        _ridSet.clear();

        // put it in loop
        bool inserted = 0;
        for (uint64 i = 0; i < tmp.size(); ++i) {
            // if not inserted yet
            // comparator returns *i - *buffer
            // *i >= *buffer
            if (!inserted && comparator(tmp[i], buffer) >= 0) {
                _dataSet.push_back(buffer);
                _ridSet.push_back(rid);
                inserted = 1;
            }
            _dataSet.push_back(tmp[i]);
            _ridSet.push_back(tmp2[i]);
        }
        if (!inserted) {
            _dataSet.push_back(buffer);
            _ridSet.push_back(rid);
        }
    }

    // create an empty index file.
    bool createIndex() { }

    // open an existing index, load index file to _container
    // open at most one index at the same time
    bool openIndex() { }

    bool closeIndex() { }
    
    // remove an index file
    bool removeIndex() { }


    // remove record from an open index...
    bool removeRecord() { }
    
    // find...
    RID findRecord() { }
    std::vector<RID> findRecords() { }


    void display(std::ofstream& fout) {
        assert(_dataSet.size() == _ridSet.size());
        for (uint64 i = 0; i < _dataSet.size(); ++i) {
            std::cout << _ridSet[i].pageID << ' ' << _ridSet[i].slotID << std::endl;
            fout.seekp(i * _data_length);
            fout.write(pointer_convert<char*>(_dataSet[i]), _data_length);
        }
    }



    uint64 _data_length;
    std::vector<void*> _dataSet;
    std::vector<RID> _ridSet;
};

#endif /* DB_INDEXMANAGER_H_ */
