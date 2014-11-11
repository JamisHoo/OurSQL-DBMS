/******************************************************
 *  Copyright (c) 2014 Terranlee
 *  Author: Terranlee
 *  E_mail: ltrthu@163.com
 *  
 *  FileName: db_indexmanager.h
 *  Date:  Oct. 27, 2014 
 *  Time:  19:27:34
 *  *************************************************** */

/****************************************
 *  Structure of index file:
 *      Page0:  _num_pages,  _num_records, _page_size,  _data_length, ComparatorType
 *      Page1:  root node
 *      Page2:  BTreeNode
 *      ...
 *
 *  Structure of each node
 *      Header: _position,  _size,  _leaf
 *      Data:   _data
 *******************************************************/

#ifndef DB_INDEXMANAGER_H_
#define DB_INDEXMANAGER_H_

#include <vector>
#include <cassert>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include "db_common.h"
#include "db_fields.h"

template<class Comparator>
class Database::DBIndexManager {

    struct BTreeNode {
        
        BTreeNode() : _position(0), _size(0), _leaf(1), EntrySize(0), MaxSons(0), DataLength(0)
        { _data = nullptr; }

        // these three members are set during initialization
        uint64 EntrySize;
        uint64 MaxSons;
        uint64 DataLength;

        // position in index file
        uint64 _position;

        // number of Entries in _data
        uint64 _size;

        // is leaf node or not
        uint64 _leaf;

        // all the entries stored by _data
        char* _data;

        // copy one node to another, used when sloving the split of _root
        void copyKey(BTreeNode* newNode) {
            char* src = _data + sizeof(uint64) * 3;
            char* dst = newNode->_data + sizeof(uint64) * 3;
            memcpy(dst, src, EntrySize * _size);
            newNode->_size = _size;
            newNode->_leaf = _leaf;
        }

        // split one node into newNode
        void splitKey(BTreeNode* newNode) {
            int pos = _size / 2;
            // split _data into two 
            char* src = _data + sizeof(uint64) * 3;
            src += EntrySize * pos;
            char* dst = newNode->_data + sizeof(uint64) * 3;
            memcpy(dst, src, EntrySize * (MaxSons - pos));
            newNode->_size = _size - pos;
            newNode->_leaf = _leaf;
            _size = pos;
        }
        
        // merge newNode into this one
        void mergeKey(BTreeNode* newNode) {
            char* dst = _data + sizeof(uint64) * 3;
            dst += EntrySize * _size;
            char* src = newNode->_data;
            src += sizeof(uint64) * 3;
            memcpy(dst, src, EntrySize * newNode->_size);

            _size += newNode->_size;
            newNode->_size = 0;
        }

        // delete Entry[pos]
        void deleteKey(const uint64 pos) {
            if(pos >= _size)
                return;
            else if(pos == _size-1){
                _size--;
                return;
            }
            char* pointer = _data + sizeof(uint64) * 3;
            pointer += EntrySize * pos;
            memmove(pointer, pointer + EntrySize, EntrySize*(_size - pos));
            _size--;
        }
        
        // insert into Entry[pos]
        uint64 insertKey(const char* newEntry, const uint64 pos) {
            if(pos > _size || _size >= MaxSons)
                return 0;
            else if(pos == _size){
                char* pointer = _data + sizeof(uint64) * 3;
                pointer += EntrySize * pos;
                memcpy(pointer, newEntry, EntrySize);
                _size++;
                return _size;
            }
            char* pointer = _data + sizeof(uint64) * 3;
            pointer += EntrySize * pos;
            memmove(pointer+EntrySize, pointer, EntrySize*(_size - pos));
            memcpy(pointer, newEntry, EntrySize);
            _size++;
            return _size;
        }
        
        // search for target, return the closet position in _data
        uint64 searchKey(const char* target, DBFields::Comparator* cmp, uint64* next) {
            char* pointer = _data + sizeof(uint64) * 3;
            uint64 i = 0;
            if(_leaf == 1)
                while(i < _size && ((*cmp)(target, pointer, DataLength) > 0)) {
                    i++;
                    pointer += EntrySize;    
                }
            else
                while(i < _size-1 && ((*cmp)(target, pointer, DataLength) > 0)) {
                    i++;
                    pointer += EntrySize;
                }
            pointer += DataLength;
            *next = *(pointer_convert<uint64*>(pointer));
            return i;
        }

        // compare the key and Entry[off], return 0 if they are truely equal
        int compareKey(const char* key, const uint64 off) {
            char* pointer = _data + sizeof(uint64) * 3;
            pointer += EntrySize * off;
            return memcmp(pointer, key, DataLength);
        }

        // write key to Entry[off], rewrite if there exist a key
        void writeKey(const char* key, const uint64 off) {
            char* pointer = _data + sizeof(uint64) * 3;
            pointer += EntrySize * off;
            memcpy(pointer, key, DataLength);
        }

        // get the position of Entry[off]
        uint64 getPosition(const uint64 off) {
            char* pointer = _data + sizeof(uint64) * 3;
            pointer += EntrySize * off + DataLength;
            return *(pointer_convert<uint64*>(pointer));
        }

        // get the key of Entry[off] by char*
        char* getKey(const uint64 off) {
            char* pointer = _data  + sizeof(uint64) * 3;
            pointer += EntrySize * off;
            return pointer;
        }

    #ifdef DEBUG
        // for debug, return data of a certain record
        char* atData(const uint64 pos) {
            char* pointer = _data + sizeof(uint64) * 3;
            return pointer + EntrySize * pos;
        }

        // for debug, return the position of a certain record
        uint64 atPosition(const uint64 pos) {
            char* pointer = _data + sizeof(uint64) * 3;
            pointer += EntrySize * pos;
            pointer += sizeof(uint64);
            uint64 answer = *(pointer_convert<uint64*>(pointer));
            return answer;
        }

        // for debug, show every record in this node
        void display(uint64 type) {
            if(_leaf == 1)
                std::cout<<"leaf ";
            else
                std::cout<<"ordinary ";
            std::cout<<"node at:"<<_position<<std::endl;
            for(int i=0; i<_size; i++) {
                char* entry = atData(i);

                switch(type) {
                    case 0:
                        std::cout<<*(pointer_convert<int8_t*>(entry));
                        break;
                    case 1:
                        std::cout<<*(pointer_convert<uint8_t*>(entry));
                        break;
                    case 2:
                        std::cout<<*(pointer_convert<int16_t*>(entry));
                        break;
                    case 3:
                        std::cout<<*(pointer_convert<uint16_t*>(entry));
                        break;
                    case 4:
                        std::cout<<*(pointer_convert<int32_t*>(entry));
                        break;
                    case 5:
                        std::cout<<*(pointer_convert<uint32_t*>(entry));
                        break;
                    case 6:
                        std::cout<<*(pointer_convert<int64_t*>(entry));
                        break;
                    case 7:
                        std::cout<<*(pointer_convert<uint64_t*>(entry));
                        break;
                    case 8:
                        std::cout<<*(pointer_convert<bool*>(entry));
                        break;
                    case 9:
                    case 10:
                        std::cout<<entry<<std::endl;
                        break;
                    case 11:
                        std::cout<<*(pointer_convert<float*>(entry));
                        break;
                    case 12:
                        std::cout<<*(pointer_convert<double*>(entry));
                        break;
                    default:
                        assert(0);
                }
                std::cout<<" ";
                uint64 pos = atPosition(i);
                RID rid = DBIndexManager::decode(pos);
                std::cout<<rid.pageID<<" "<<rid.slotID<<std::endl;
            }
            std::cout<<std::endl;
        }

        void testShow(){
            for(int i=0; i<_size; i++) {
                char* entry = atData(i);
                std::cout<<*(pointer_convert<uint64*>(entry))<<std::endl;
            }
        }
    #endif

    };


public:
    DBIndexManager(const std::string& file): _file(file), _page_size(0), _num_pages(0), _num_records(0), _data_length(0)
    { _node_tracker = nullptr; }

    ~DBIndexManager() {
        if(_num_pages != 0)
            this->close();
        _fs.close();
    }

// public interface of open/close/create
    // close the btree index and write back the buffer
    // return 0 if close successfully, return 1 when error
    bool close() {
        if(!isopen()) return 1;

        writeNumbers();
        finalize();

        _fs.close();
        if(isopen()) return 1;
        _num_pages = 0;
        return 0;
    }

    // open index file, return _num_pages if open successfully
    // return 0 when error
    uint64 open() {
        // fail if file not accessiable
        if(!accessible()) return 0;
        if(isopen()) return _num_pages;

        _fs.open(_file, std::fstream::in| std::fstream::out| std::fstream::binary);
        if(!isopen()) return 0;

        // open successfully, load data from the first page
        _fs.seekg(0);
        char buffer[sizeof(uint64) * 5];
        _fs.read(buffer, sizeof(uint64) * 5);
        _num_pages = *(pointer_convert<uint64*>(buffer));
        _write_to = _num_pages;
        _num_records = *(pointer_convert<uint64*>(buffer + sizeof(uint64)));
        _page_size = *(pointer_convert<uint64*>(buffer + sizeof(uint64) * 2));
        _data_length = *(pointer_convert<uint64*>(buffer + sizeof(uint64) * 3));
        uint64 comp_type = *(pointer_convert<uint64*>(buffer + sizeof(uint64) * 4));

        setComparatorType(comp_type);

        _entry_size = _data_length + sizeof(uint64);
        _max_sons = (_page_size - sizeof(uint64) * 3) / _entry_size;

        initialize();

        return _num_pages;
    }

    // create index file and init Page0
    // return 0 if create successfully, return 1 when error
    bool create(const uint64 page_size, const uint64 data_length, const uint64 comp_type) {
        // fail if file exists
        if(accessible()) return 1;
        _fs.open(_file, std::fstream::out| std::fstream::binary);
        if(!isopen()) return 1;

        _page_size = page_size;
        _data_length = data_length;
        uint64 _comp_type = comp_type;

        // write page0 after creating
        _write_to = 2;
        char buffer[_page_size];
        uint64 numPages = 2;
        uint64 numReocrds = 0;
        memset(buffer, 0xdd, _page_size);
        memcpy(buffer, &numPages, sizeof(numPages));
        memcpy(buffer + sizeof(numPages), &numReocrds, sizeof(_num_records));
        memcpy(buffer + sizeof(numPages) * 2, &_page_size, sizeof(_page_size));
        memcpy(buffer + sizeof(numPages) * 3, &_data_length, sizeof(_data_length));
        memcpy(buffer + sizeof(numPages) * 4, &_comp_type, sizeof(_comp_type));
        writePage(0, buffer);

        // write page1(root) after create
        BTreeNode root;
        root._data = new char[_page_size];
        root._position = 1;
        writeNode(1, &root);
        delete[] root._data;

        _fs.close();
        return 0;
    }

// public interface of IndexManager operation

// Warning: search/remove/insert are non-entrant
//          use these carefully if you implement a multiple thread database

    // return RID(0,0) if not found
    RID searchRecord(const char* key) {
        bool answer = locateRecord(key);
        bool checkLast = (_level[_lev_track]._offset >= _node_tracker->_size);
        if(answer && !checkLast){               // key exists in this index file
            uint64 which = _level[_lev_track]._block;
            uint64 off = _level[_lev_track]._offset;
            getBuffer(which);
            uint64 pos = _node_tracker->getPosition(off);
            return decode(pos);
        }
        else{
            return RID(0,0);
        }
    }

    // return an empty vector if not found
    std::vector<RID> searchRecords(const char* key) {
        std::vector<RID> ridVector;
        RID first = searchRecord(key);
        if(first.pageID == 0 && first.slotID == 0 )     // return an empty vector
            return ridVector;

        else{
            ridVector.push_back(first);
            // continue push RID with the same key
            uint64 off = _level[_lev_track]._offset;
            while(true){
                if(off >= _node_tracker->_size - 1){    // find the proper node
                    bool answer = findNextNode();
                    if(answer){
                        off = _level[_lev_track]._offset;
                        continue;
                    }
                    else
                        break;
                }

                off++;                                  // 
                int answer = _node_tracker->compareKey(key, off);
                if(answer == 0){
                    uint64 pos = _node_tracker->getPosition(off);
                    ridVector.push_back(decode(pos));
                }
                else
                    break;
            }
        }
        return ridVector;
    }

    // search for lower <= key < upper
    // return an empty vector if not found
    std::vector<RID> rangeQuery(const char* lower, const char* upper) {
        std::vector<RID> ridVector;
        int answer = _comparator(lower, upper, _data_length);
        if(answer > 0)
            return ridVector;

        RID first = searchRecord(lower);
        if(first.pageID == 0 && first.slotID == 0)
            return ridVector;

        else{
            ridVector.push_back(first);
            // continue push RID with key within range
            uint64 off = _level[_lev_track]._offset;
            while(true){
                if(off == _node_tracker->_size - 1){
                    bool answer = findNextNode();
                    if(answer){
                        off = _level[_lev_track]._offset;
                        char* currentKey = _node_tracker->getKey(off);
                        int answer = _comparator(currentKey, upper, _data_length);
                        if(answer < 0){
                            uint64 pos = _node_tracker->getPosition(off);
                            ridVector.push_back(decode(pos));
                        }
                        continue;
                    }
                    else
                        break;
                }

                off++;
                char* currentKey = _node_tracker->getKey(off);
                int answer = _comparator(currentKey, upper, _data_length);
                if(answer < 0){
                    uint64 pos = _node_tracker->getPosition(off);
                    ridVector.push_back(decode(pos));
                }
                else
                    break;
            }
        }
        return ridVector;
    }

    // insert a record into the btree
    // return false if error
    // ifPrimary: is this key a primary key(cannot insert twice)?
    bool insertRecord(const char* key, const RID rid, const bool ifPrimary) {
        bool answer = locateRecord(key);
        bool checkLast = (_level[_lev_track]._offset >= _node_tracker->_size);
        if(answer && !checkLast && ifPrimary){
            #ifdef DEBUG
                //std::cout<<"primary key already exist"<<std::endl;
            #endif
            return false;
        }
        else{
            uint64 position = encode(rid);
            char entry[_data_length + sizeof(uint64)];
            memcpy(entry, key, _data_length);
            memcpy(entry + _data_length, &position, sizeof(uint64));
            _node_tracker->insertKey(entry, _level[_lev_track]._offset);
            setDirty(_node_tracker);
            _num_records++;
            if(_node_tracker->_size >= _max_sons)
                solveNodeSplit();
            else
                solveChangeGreater(key);
            return true;
        }

    }

    // remove all records where index.key == key
    // return true if success, else return false
    // use this function if you want to implement a multimap
    bool removeRecords(const char* key) {
        bool answer = false;
        while(true){
            bool temp = removeRecord(key);
            if(temp == false)
                return answer;
            else
                answer = true;
        }
    }

    // remove the first record if index.key == key 
    // don't check the RID
    bool removeRecord(const char* key) {
        bool answer = locateRecord(key);
        bool checkLast = (_level[_lev_track]._offset >= _node_tracker->_size);
        if(answer && !checkLast){
            uint64 off = _level[_lev_track]._offset;
            _node_tracker->deleteKey(off);
            setDirty(_node_tracker);                // delete the key and set dirty
            _num_records--;

            if(off == _node_tracker->_size){        // delete the last record in this node, may lead to change of parent nodes
                uint64 tempLev = _lev_track;
                uint64 tempBlk = _level[_lev_track]._block;
                char* keyUpdate = _node_tracker->getKey(off - 1);
                solveChangeSmaller(keyUpdate);
                _lev_track = tempLev;
                getBuffer(tempBlk);
            }
            if(_node_tracker->_size < _max_sons / 2){       // node size break through the lower bound
                solveNodeMerge();
            }
            return true;
        }
        else{
            #ifdef DEBUG
                std::cout<<"key doesn't exist"<<std::endl;
            #endif
            return false;
        }
    }

    // remove only one record if index.key == key && index.rid == rid
    bool removeRecord(const char* key, const RID rid) {
        bool answer = locateRecord(key);
        bool checkLast = (_level[_lev_track]._offset >= _node_tracker->_size);
        if(answer && !checkLast){
            bool findRID = false;
            // find the record if index.key == key && index.rid == rid
            while(true){
                uint64 off = _level[_lev_track]._offset;
                if(_node_tracker->compareKey(key, off) != 0)    // if index.key != key, failed
                    break;

                uint64 pos = _node_tracker->getPosition(off);
                RID indexRID = decode(pos);
                if(indexRID == rid){                            // if index.rid == rid, success
                    findRID = true;
                    break;
                }
                else{                                           // if index.rid != rid, find next record
                    if(_level[_lev_track]._offset < _node_tracker->_size - 1)
                        _level[_lev_track]._offset++;
                    else{
                        bool way = findNextNode();
                        if(way == false)                        // continue to search next node, failed if it comes to the last node
                            break;
                    }
                }
            }       // end of while(true)

            if(findRID){                                // if find succeed, delete it
                uint64 off = _level[_lev_track]._offset;
                _node_tracker->deleteKey(off);
                setDirty(_node_tracker);                // delete the key and set dirty
                _num_records--;

                if(off == _node_tracker->_size){        // delete the last record in this node, may lead to change of parent nodes
                    uint64 tempLev = _lev_track;
                    uint64 tempBlk = _level[_lev_track]._block;
                    char* keyUpdate = _node_tracker->getKey(off - 1);
                    solveChangeSmaller(keyUpdate);
                    _lev_track = tempLev;
                    getBuffer(tempBlk);
                }
                if(_node_tracker->_size < _max_sons / 2){       // node size break through the lower bound
                    solveNodeMerge();
                }
                return true;
            }           // end of if(findRID)
            return false;
        }               // end of if(answer && !checkLast)
        else{           
            #ifdef DEBUG
                std::cout<<"key doesn't exist"<<std::endl;
            #endif
            return false;
        }
    }

    // remove this index file
    // returns 0 if succeed, 1 otherwise
    bool remove() {
        // close before remove
        this->close();
        return std::remove(_file.data());
    }

    // traverse all records
    // callback function is like: void func(const char* key, const RID rid)
    template<class CALLBACKFUNC>
    void traverseRecords(CALLBACKFUNC func) {
        _lev_track = 0;
        _node_tracker = &_root;
        _level[_lev_track]._offset = 0;
        _level[_lev_track]._block = 1;
        findFirstNode();
        while(true){
            for(int i=0; i<_node_tracker->_size; i++){
                char* key = _node_tracker->getKey(i);
                uint64 pos = _node_tracker->getPosition(i);
                func(key, decode(pos));
            }
            bool answer = findNextNode();
            if(!answer)
                break;
        }
    }

    uint64 getNumRecords() {
        return _num_records;
    }

    #ifdef DEBUG
    // show every node in this btree
    void show() {
        for(int i=1; i<_num_pages; i++) {
            getBuffer(i);
            _node_tracker->display(_comparator.type);
        }
    }

    #endif

private:
    // private support of create/open/close the index file
    // initialize during open index
    // TODO: memory need to be set to 0?
    void initialize() {
        // initialize buffer and root
        initBuffer();
        _root._data = new char[_page_size];
        _root.EntrySize = _entry_size;
        _root.MaxSons = _max_sons;
        _root.DataLength = _data_length;

        memset(_root._data, 0, _page_size);
        readNode(1, &_root);
    }

    // finalize during close index
    void finalize() {
        closeBuffer();
        writeNode(1, &_root);
        delete[] _root._data;
    }

    // find the first leaf node according to current node
    // the results are stored in _level and _node_tracker pointed to the node
    void findFirstNode() {
        // add these if want to find the global first node
        /*
            _lev_track = 0;
            _node_tracker = &_root;
            _level[_lev_track]._block = 1;
            _level[_lev_track]._offset = 0;
        */
        if(_node_tracker->_leaf == 1)
            return;

        uint64 nextLevel = _node_tracker->getPosition(_level[_lev_track]._offset);
        _lev_track++;
        _level[_lev_track]._block = nextLevel;
        getBuffer(nextLevel);
        while(_node_tracker->_leaf == 0) {
            _level[_lev_track++]._offset = 0;
            uint64 next = _node_tracker->getPosition(0);
            _level[_lev_track]._block = next;
            getBuffer(next);
        }
        _level[_lev_track]._offset = 0;
    }

    // find the last record in index file according to _comparator
    void findLastNode() {
        _lev_track = 0;
        _node_tracker = &_root;
        _level[_lev_track]._block = 1;
        while(_node_tracker->_leaf == 0) {
            uint64 off = _node_tracker->_size - 1;
            uint64 next = _node_tracker->getPosition(off);
            _level[_lev_track++]._offset = off;
            _level[_lev_track]._block = next;
            getBuffer(next);
        }
        _level[_lev_track]._offset = _node_tracker->_size - 1;
    }

    // find next record according to the current one
    // return false if there is no next node to find
    bool findNextNode() {
        if(_lev_track == 0)
            return false;

        // search up to the common parent
        uint64 off, pos;
        while(true) {
            _lev_track--;
            pos = _level[_lev_track]._block;
            off = _level[_lev_track]._offset;
            getBuffer(pos);
            if(_node_tracker->_size > (off + 1))
                break;
            if(_lev_track == 0)
                return false;
        }
        _level[_lev_track]._offset = off + 1;

        // search down and find the next node
        findFirstNode();
        return true;
    }

// private buffer operations
    // TODO: memory need to be set to 0?
    void initBuffer() {
        for(int i=0; i<BUFFER_SIZE; i++) {
            _buffer[i]._node._data = new char[_page_size];
            memset(_buffer[i]._node._data, 0 , _page_size);
            _buffer[i]._node._position = 0;
            _buffer[i]._dirty = false;

            _buffer[i]._node.MaxSons = _max_sons;
            _buffer[i]._node.EntrySize = _entry_size;
            _buffer[i]._node.DataLength = _data_length;
        }
    }

    // close buffer: release memory and write back pages
    void closeBuffer() {
        for(int i=0; i<BUFFER_SIZE; i++){
            if(_buffer[i]._dirty == true)
                writeNode(_buffer[i]._node._position, &(_buffer[i]._node));
        }
        for(int i=0; i<BUFFER_SIZE; i++) {
            delete[] _buffer[i]._node._data;
            _buffer[i]._dirty = false;
        }
    }

    // set the buffer pointed by tracker to be dirty
    void setDirty(BTreeNode* tracker) {
        if(tracker == &_root)
            return;
        uint64 pos = tracker->_position;
        for(int i=0; i<BUFFER_SIZE; i++)
            if(_buffer[i]._node._position == pos){
                _buffer[i]._dirty = true;
                return;
            }
    }

    // clear one buffer for new BTreeNode
    int clearBuffer() {
        static int i = -1;
        while(true) {
            i = (i + 1) % BUFFER_SIZE;
            if(&(_buffer[i]._node) != _node_tracker)
                break;
        }
        if(_buffer[i]._dirty == true){
            writeNode(_buffer[i]._node._position, &(_buffer[i]._node));
            _buffer[i]._dirty = false;
        }
        return i;
    }

    int loadBuffer(uint64 pos) {
        int i = clearBuffer();
        readNode(pos, &(_buffer[i]._node));
        return i;
    }

    // get node through buffer
    // _node_tracker will point to the node you want
    void getBuffer(uint64 pos) {
        if(pos == 1){
            _node_tracker = &_root;
        }
        else{
            for(int i=0; i<BUFFER_SIZE; i++)
                if(_buffer[i]._node._position == pos) {
                    _node_tracker = &(_buffer[i]._node);
                    return;
                }
                int position = loadBuffer(pos);
                _node_tracker = &(_buffer[position]._node);
        }
    }

// private functions to support btree
    // slove split of root, btree height will increase
    void solveRootSplit(const char* leftChild, const char* rightChild, const uint64 rightPos) {
        // save left child first
        int pos = clearBuffer();
        _node_tracker->copyKey(&(_buffer[pos]._node));
        _buffer[pos]._node._position = _write_to;
        _buffer[pos]._dirty = true;
        char tempBuffer[_data_length + sizeof(uint64)];
        memcpy(tempBuffer, leftChild, _data_length);
        memcpy(tempBuffer+_data_length, &_write_to, sizeof(uint64));
        _write_to++;

        // update new root;
        _root._size = 0;
        _root.insertKey(tempBuffer, 0);
        memcpy(tempBuffer, rightChild, _data_length);
        memcpy(tempBuffer+_data_length, &rightPos, sizeof(uint64));
        _root.insertKey(tempBuffer, 1);
        _root._leaf = 0;
    }

    // slove split of ordinary node
    void solveNodeSplit() {
        if(_node_tracker->_size < _max_sons)
            return;

        char* leftChild = _node_tracker->getKey(_max_sons/2 - 1);
        char* rightChild = _node_tracker->getKey(_max_sons - 1);

        int pos = clearBuffer();
        _node_tracker->splitKey(&(_buffer[pos]._node));
        setDirty(_node_tracker);
        // new node _buffer[pos] will write to indexFile[_write_to]
        _buffer[pos]._node._position = _write_to;
        _write_to++;
        _buffer[pos]._dirty = true;

        if(_lev_track == 0){
            solveRootSplit(leftChild, rightChild, _buffer[pos]._node._position);
            return;
        }

        // not the root node, recursively solve the split problem
        _lev_track--;
        uint64 blk = _level[_lev_track]._block;
        uint64 off = _level[_lev_track]._offset;
        getBuffer(blk);
        _node_tracker->writeKey(leftChild, off);

        char tempBuffer[_data_length + sizeof(uint64)];
        memcpy(tempBuffer, rightChild, _data_length);
        memcpy(tempBuffer + _data_length, &(_buffer[pos]._node._position), sizeof(uint64));
        _node_tracker->insertKey(tempBuffer, off+1);
        setDirty(_node_tracker);
        solveNodeSplit();
    }

    // solve merge of root, btree height will decrease
    void solveRootMerge() {
        _node_tracker = &_root;             // in this condition, root has two children
        uint64 posMain = _node_tracker->getPosition(0);
        uint64 posSub = _node_tracker->getPosition(1);

        getBuffer(posMain);                 // copy the left one to root
        _node_tracker->copyKey(&_root);
        _node_tracker->_size = 0;
        setDirty(_node_tracker);

        getBuffer(posSub);                  // copy right one to root
        _root.mergeKey(_node_tracker);
        _node_tracker->_size = 0;
        setDirty(_node_tracker);
    }

    // check if left brother node can lend an entry
    int checkLeft() {
        uint64 off = _level[_lev_track]._offset;
        if(off == 0)        // no left brother
            return 0;
        uint64 leftPos = _node_tracker->getPosition(off - 1);
        getBuffer(leftPos);
        if(_node_tracker->_size > _max_sons / 2)
            return 1;       // left brother can lend
        return 0;
    }

    // check if right brother node can lend an entry
    int checkRight() {
        uint64 off = _level[_lev_track]._offset;
        if(off == _node_tracker->_size-1)       // no right brother
            return 0;
        uint64 rightPos = _node_tracker->getPosition(off + 1);
        getBuffer(rightPos);
        if(_node_tracker->_size > _max_sons / 2)
            return 2;                           // right brother can lend
        return 0;
    }

    // current: node need merge, upper: the upper level node of current
    void lendNode(uint64 upper, uint64 current, int which) {
        char buffer[_entry_size];
        if(which == 1){         // lend from left, _node_tracker points to the left brother
            uint64 off = _node_tracker->_size - 1;
            char* src = _node_tracker->getKey(off);
            memcpy(buffer, src, _entry_size);       //  get the entry from left brother
            _node_tracker->_size--;
            char update[_data_length];
            src = _node_tracker->getKey(off - 1);
            memcpy(update, src, _data_length);
            setDirty(_node_tracker);
            
            getBuffer(upper);                       // update the keys of father node
            uint64 offUpper = _level[_lev_track]._offset - 1;
            _node_tracker->writeKey(update, offUpper);
            setDirty(_node_tracker);

            getBuffer(current);                     // insert entry into current node
            _node_tracker->insertKey(buffer, 0);
            setDirty(_node_tracker);
        }
        else{                   // lend from right, _node_tracker points to the right brother
            char* src = _node_tracker->getKey(0);
            memcpy(buffer, src, _entry_size);       // get the entry from right brother
            _node_tracker->deleteKey(0);
            setDirty(_node_tracker);

            getBuffer(current);                     // insert entry into current node
            uint64 currIns = _node_tracker->_size;
            _node_tracker->insertKey(buffer, currIns);
            setDirty(_node_tracker);

            getBuffer(upper);                       // update the keys of father node
            uint64 offupper = _level[_lev_track]._offset;
            _node_tracker->writeKey(buffer, offupper);
            setDirty(_node_tracker);
        }
    }

    // if cannot lend entry, then merge
    void mergeNode(uint64 upper, uint64 current) {
        getBuffer(upper);
        uint64 off = _level[_lev_track]._offset;
        if(off != _node_tracker->_size-1) {     // merge to right node if possible
            uint64 posRight = _node_tracker->getPosition(off + 1);
            getBuffer(posRight);
            char buffer[_data_length];
            uint64 lastPos = _node_tracker->_size - 1;
            char* src = _node_tracker->getKey(lastPos);     // do preparation, reserve the max entry
            memcpy(buffer, src, _data_length);

            BTreeNode* temp = _node_tracker;                // merge
            getBuffer(current);
            _node_tracker->mergeKey(temp);
            setDirty(temp);
            setDirty(_node_tracker);

            getBuffer(upper);                               // use max entry to update parent node
            _node_tracker->writeKey(buffer, off);
            _node_tracker->deleteKey(off + 1);
            setDirty(_node_tracker);
        }
        else{                                   // merge to left node
            uint64 posLeft = _node_tracker->getPosition(off - 1);
            getBuffer(current);
            BTreeNode* temp = _node_tracker;                // similar to merge from right
            getBuffer(posLeft);
            _node_tracker->mergeKey(temp);
            setDirty(_node_tracker);
            setDirty(temp);

            char buffer[_data_length];
            uint64 lastPos = _node_tracker->_size - 1;
            char* src = _node_tracker->getKey(lastPos);
            memcpy(buffer, src, _data_length);

            getBuffer(upper);
            _node_tracker->writeKey(buffer, off - 1);
            _node_tracker->deleteKey(off);
            setDirty(_node_tracker);
        }
    }

    // main entrance when node size break through the lower bound
    void solveNodeMerge() {
        if(_node_tracker->_size >= _max_sons / 2)
            return;
        
        if(_lev_track == 0)
            return;

        uint64 current = _node_tracker->_position;

        _lev_track--;
        uint64 blk = _level[_lev_track]._block;
        getBuffer(blk);
        int answer = checkLeft();           // check if left or right is possible to lend
        if(answer == 0){
            getBuffer(blk);
            answer = checkRight();
        }
        if(answer == 1 || answer == 2) {    // lend node is not recursive, just return
            lendNode(blk, current, answer);
        }
        else {                              // merge node is recursive
            bool way = true;
            if(_lev_track == 0){
                getBuffer(blk);             // get the root node, if size == 2, then height - 1
                if(_node_tracker->_size == 2){
                    solveRootMerge();
                    way = false;
                }
            }
            if(way){
                mergeNode(blk, current);
                // recursively slove the node merge problem
                solveNodeMerge();
            }
        }
    }

    // solve the max son is greater than before
    void solveChangeGreater(const char* key) {
        if((_level[_lev_track]._offset + 1) < _node_tracker->_size)
            return;
        if(_lev_track == 0)
            return;

        _lev_track--;
        uint64 blk = _level[_lev_track]._block;
        uint64 off = _level[_lev_track]._offset;
        getBuffer(blk);
        _node_tracker->writeKey(key, off);
        setDirty(_node_tracker);

        // recursively solve the max sons problem
        solveChangeGreater(key);
    }

    // solve the max son is smaller than before
    void solveChangeSmaller(const char* key) {
        if((_level[_lev_track]._offset + 1 < _node_tracker->_size))
            return;
        if(_lev_track == 0)
            return;

        _lev_track--;
        uint64 blk = _level[_lev_track]._block;
        uint64 off = _level[_lev_track]._offset;
        getBuffer(blk);
        _node_tracker->writeKey(key, off);
        setDirty(_node_tracker);

        // recursively solve the small sons problem
        solveChangeSmaller(key);
    }
    
    // find the most suitable place for key and check if this key exist
    // return true if this key exist, return false if not exist
    bool locateRecord(const char* key) {
        _lev_track = 0;
        _node_tracker = &_root; 
        _level[_lev_track]._block = 1;
        while(_node_tracker->_leaf == 0) {
            uint64 next;
            uint64 off = _node_tracker->searchKey(key, &_comparator, &next);
            _level[_lev_track++]._offset = off;
            _level[_lev_track]._block = next;
            getBuffer(next);
        }
        uint64 next;
        uint64 off = _node_tracker->searchKey(key, &_comparator, &next);
        _level[_lev_track]._offset = off;
        // check if it is exactly the same to this entry?
        // or it is just a proper position for insertion?
        if(_node_tracker->compareKey(key, off) == 0)
            return true;
        else
            return false;
    }

// some useful tools
    // change pageID and  slotID to a uint64
    inline static uint64 encode(RID rid) {
        return (rid.pageID<<32) + rid.slotID;
    }

    // change uint64 to pageID and slotID
    inline static RID decode(uint64 position) {
        return RID(position >> 32, (position << 32) >> 32);
    }

    void displayNumPages() {
        std::cout<<"_num_pages:"<<_num_pages<<" _write_to:"<<_write_to<<std::endl;
    }

    void basicInformation() {
        std::cout<<"_page_size:"<<_page_size<<" _data_length:"<<_data_length<<std::endl;
    }

// private basic in/out functions
    // read and write a node to index
    // sizeof(uint64)*3 of _data will be determinated when it is write back
    void writeNode(const uint64 position, BTreeNode* src) {
        memcpy(src->_data, &(src->_position), sizeof(uint64) * 3);
        _fs.seekp(_page_size * position);
        _fs.write(src->_data, _page_size);
    }

    void readNode(const uint64 position, BTreeNode* dst) {
        _fs.seekg(_page_size * position);
        _fs.read(dst->_data, _page_size);
        memcpy(&(dst->_position), dst->_data, sizeof(uint64) * 3);
    }

    // read and write page using a char buffer, use this in create
    void writePage(const uint64 position, char* buffer) {
        _fs.seekp(_page_size * position);
        _fs.write(buffer, _page_size);
    }

    // change _num_pages of index file in page0
    void writeNumbers() {
        if(_write_to > _num_pages) {
            _num_pages = _write_to;
            char buffer[sizeof(_num_pages)];
            memcpy(buffer, &_num_pages, sizeof(_num_pages));
            _fs.seekp(0);
            _fs.write(buffer, sizeof(_num_pages));
        }
        char buffer[sizeof(uint64)];
        memcpy(buffer, &_num_records, sizeof(_num_records));
        _fs.seekp(sizeof(_num_records));
        _fs.write(buffer, sizeof(_num_records));
    }
    
    void setComparatorType(const uint64 type) {
        _comparator.type = type;
    }

    // check whether the file is accessible
    bool accessible() const {
        std::ifstream fin(_file);
        return fin.good();
    }
    // returns 1 if file is opened, 0 otherwise
    bool isopen() const { return _fs.is_open(); }


// members of IndexManager
    static constexpr uint64 MAX_LEVEL = 8;
    static constexpr uint64 BUFFER_SIZE = 16;

    // track different level of BTreeNode during search
    // record the position and offset of every node from root to the target
    struct Level {
        uint64 _block;
        uint64 _offset;
    };
    Level _level[MAX_LEVEL];

    // a tracker of the current level
    uint64 _lev_track;

    // each buffer node contains a header and data area
    struct BufferNode {
        BTreeNode _node;
        bool _dirty;
    };
    BufferNode _buffer[BUFFER_SIZE];

    BTreeNode _root;

    // a tracker of the current BTreeNode you are operating
    BTreeNode* _node_tracker;

    uint64 _num_pages;
    uint64 _write_to;
    uint64 _page_size;
    uint64 _data_length;
    uint64 _num_records;

    // these are equal to EntrySize and MaxSons in BTreeNode
    // initialize when create an index file
    uint64 _entry_size;
    uint64 _max_sons;

    DBFields::Comparator _comparator;

    std::string _file;
    std::fstream _fs;
};

#endif /* DB_INDEXMANAGER_H_ */
