/******************************************************
 *  Copyright (c) 2014 Terranlee
 *  Author: Terranlee
 *  E_mail: ltrthu@163.com
 *  
 *  FileName: db_btree.h
 *  Date:  Oct. 27, 2014 
 *  Time:  19:27:34
 *  *************************************************** */

/****************************************
 *  Structure of index file:
 *      Page0:  _num_pages,  _page_size,  _data_length
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
#include <iostream>
#include <fstream>
#include "db_common.h"

template<class Comparator>
class Database::DBIndexManager {
#ifdef DEBUG
public:
#endif

    struct BTreeNode {
        static int EntrySize;
        static int MaxSons;
        static uint64 DataLength;

        BTreeNode() : _position(0), _size(0), _leaf(1) 
        { _data = nullptr; }

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

        // split one node into two
        void splitKey(BTreeNode* newNode) {
            int pos = _size / 2;
            // split _data into two 
            char* src = _data + sizeof(uint64)*3;
            src += EntrySize * pos;
            char* dst = newNode->_data + sizeof(uint64)*3;
            memcpy(dst, src, EntrySize * (MaxSons - pos));
            newNode->_size = _size - pos;
            newNode->_leaf = _leaf;
            _size = pos;
        }
        
        // delete Entry[pos]
        void deleteKey(uint64 pos) {
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
        uint64 insertKey(char* newEntry, uint64 pos) {
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
            char* pointer = _data + sizeof(uint64)*3;
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
            pointer += EntrySize * i + DataLength;
            next = pointer_convert<uint64*>(pointer);
            return i;
        }

        // compare the key and Entry[off], return 0 if they are truely equal
        int compareKey(const char* key, uint64 off) {
            char* pointer = _data + sizeof(uint64)*3;
            pointer += EntrySize * off;
            return memcmp(pointer, key, DataLength);
        }

        // get the position of Entry[off]
        uint64 getPosition(uint64 off) {
            char* pointer = _data + sizeof(uint64)*3;
            pointer += EntrySize * off + DataLength;
            return *(pointer_convert<uint64*>(pointer));
        }

#ifdef DEBUG
        char* atData(uint64 pos) {
            char* pointer = _data + sizeof(uint64) * 3;
            return pointer + EntrySize * pos;
        }

        uint64 atPosition(uint64 pos) {
            char* pointer = _data + sizeof(uint64) * 3;
            pointer += EntrySize * pos;
            pointer += sizeof(uint64);
            uint64 answer = *(pointer_convert<uint64*>(pointer));
            return answer;
        }

        void display() {
            if(_leaf)
                std::cout<<"leaf ";
            else
                std::cout<<"ordinary ";
            std::cout<<"node at:"<<_position<<std::endl;
            for(int i=0; i<_size; i++) {
                char* entry = atData(i);
                std::cout<<*(pointer_convert<uint64*>(entry))<<" ";
                std::cout<<atPosition(i)<<std::endl;
            }
            std::cout<<std::endl;
        }
#endif

    };

public:

    DBIndexManager(const std::string& file): _file(file), _page_size(0), _num_pages(0), _data_length(0)
    { _node_tracker = nullptr; }

    ~DBIndexManager() {
        if(_num_pages != 0)
            this->close();
        _fs.close();
    }

    // close the btree index and write back the buffer
    // return 0 if close successfully, return 1 when error
    bool close() {
        if(!isopen()) return 1;

        writeNumPages(_write_to);
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
        char buffer[sizeof(uint64) * 3];
        _fs.read(buffer, sizeof(uint64) * 3);
        _num_pages = *(pointer_convert<uint64*>(buffer));
        _write_to = _num_pages;
        _page_size = *(pointer_convert<uint64*>(buffer + sizeof(uint64)));
        _data_length = *(pointer_convert<uint64*>(buffer + sizeof(uint64)*2));

        initialize();

        BTreeNode::EntrySize = _data_length + sizeof(uint64);
        BTreeNode::MaxSons = (_page_size - sizeof(uint64)*3) / BTreeNode::EntrySize;
        BTreeNode::DataLength = _data_length;

        return _num_pages;
    }

    // create index file and init Page0
    // return 0 if create successfully, return 1 when error
    bool create(const uint64 page_size, const uint64 data_length) {
        // fail if file exists
        if(accessible()) return 1;
        _fs.open(_file, std::fstream::out| std::fstream::binary);
        if(!isopen()) return 1;

        _page_size = page_size;
        _data_length = data_length;

        // write page0 after creating
        _write_to = 2;
        char buffer[_page_size];
        uint64 numPages = 2;
        memset(buffer, 0xdd, _page_size);
        memcpy(buffer, &numPages, sizeof(numPages));
        memcpy(buffer + sizeof(numPages), &_page_size, sizeof(_page_size));
        memcpy(buffer + sizeof(numPages)*2, &_data_length, sizeof(_data_length));
        writePage(0, buffer);

        // write page1(root) after create
        BTreeNode root;
        root._data = new char[_page_size];
        root._position = 1;
        writeNode(1, &root);
        delete[] root._data;

        _fs.close();
    }

    // initialize during open index
    // TODO: memory need to be set to 0?
    void initialize() {
        // initialize buffer and root
        initBuffer();
        _root._data = new char[_page_size];
        readNode(1, &_root);
    }

    // finalize during close index
    void finalize() {
        closeBuffer();
        writeNode(1, &_root);
        delete[] _root._data;
    }


    // buffer operations
    // TODO: memory need to be set to 0?
    void initBuffer() {
        for(int i=0; i<BUFFER_SIZE; i++) {
            _buffer[i]._node._data = new char[_page_size];
            _buffer[i]._node._position = 0;
            _buffer[i]._dirty = false;
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
                _buffer[i].dirty = true;
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

    // some private functions to support btree
    void solveNodeSplit() {

    }

    void solveRootSplit() {

    }

    // open an existing index, load index file to _container
    // open at most one index at the same time
    

    bool locateRecords(char* key) {
        _lev_track = 0;
        _node_tracker = &_root;
        _level[_lev_track]._block = 1;
        while(_node_tracker->_leaf == false) {
            uint64 next;
            uint64 off = _node_tracker->searchKey(key, &_comparator, &next);
            _level[_lev_track]._offset = off;
            _level[_lev_track]._block = next;
            getBuffer(next);
        }
        uint64 next;
        uint64 off = _node_tracker->searchKey(key, &_comparator, &next);
        _level[_lev_track]._offset = off;
        if(_node_tracker->compareKey(key, off) == 0)
            return true;
        else
            return false;
    }

    // return RID(0,0) if not found
    RID searchRecord(char* key) {
        bool answer = locateRecords(key);
        if(answer){
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

    std::vector<RID> searchRecords(char* key) {

    }

    // insert a record into the btree, return 0 if success
    // ifPrimary: is this key a primary key(cannot insert twice)?
    bool insertRecord(char* key, RID rid, bool ifPrimary) {
        bool answer = locateRecords(key);
        if(answer && ifPrimary){
            #ifdef DEBUG
                std::cout<<"primary key already exist"<<std::endl;
            #endif
            return 1;
        }
        else{
            uint64 position = encode(rid);
            char entry[_data_length + sizeof(uint64)];
            memcpy(entry, key, _data_length);
            memcpy(entry + _data_length, &position, sizeof(uint64));
            _node_tracker->insertKey(entry, _level[_lev_track].offset);
            setDirty(_node_tracker);
            solveNodeSplit();
        }

    }

    // remove an index file
    bool removeIndex() { }

    // remove record from an open index...
    bool removeRecord() { }

    inline static uint64 encode(RID rid) {
        return (rid.pageID<<32) + rid.slotID;
    }

    inline static RID decode(uint64 position) {
        return RID(position >> 32, (position << 32) >> 32);
    }

    void displayNumPages() {
        std::cout<<"_num_pages:"<<_num_pages<<" _write_to:"<<_write_to<<std::endl;
    }

    void setComparatorType(const uint64 type) {
        _comparator.type = type;
    }

    // read and write a node to index
    void writeNode(uint64 position, BTreeNode* src) {
        memcpy(src->_data, &(src->_position), sizeof(uint64)*3);
        _fs.seekp(_page_size * position);
        _fs.write(src->_data, _page_size);
    }

    void readNode(uint64 position, BTreeNode* dst) {
        _fs.seekg(_page_size * position);
        _fs.read(dst->_data, _page_size);
        memcpy(&(dst->_position), dst->_data, sizeof(uint64)*3);
    }

    // read and write page using a char buffer, use this in create
    void writePage(uint64 position, char* buffer) {
        _fs.seekp(_page_size * position);
        _fs.write(buffer, _page_size);
    }

    // change _num_pages of index file in page0
    void writeNumPages(uint64 position) {
        if(position > _num_pages) {
            _num_pages = position;
            char buffer[sizeof(_num_pages)];
            memcpy(buffer, &_num_pages, sizeof(_num_pages));
            _fs.seekp(0);
            _fs.write(buffer, sizeof(_num_pages));
        }
    }
    
    // check whether the file is accessible
    bool accessible() const {
        std::ifstream fin(_file);
        return fin.good();
    }
    // returns 1 if file is opened, 0 otherwise
    bool isopen() const { return _fs.is_open(); }



    static constexpr uint64 MAX_LEVEL = 8;
    static constexpr uint64 BUFFER_SIZE = 16;

    // track different level of BTreeNode during search
    // record the position and offset of every node from root to the target
    struct Level {
        uint64 _block;
        uint64 _offset;
    };
    Level _level[MAX_LEVEL];
    uint64 _lev_track;

    // each buffer node contains a header and data area
    struct BufferNode {
        BTreeNode _node;
        bool _dirty;
    };
    BufferNode _buffer[BUFFER_SIZE];

    BTreeNode _root;
    BTreeNode* _node_tracker;

    uint64 _num_pages;
    uint64 _write_to;
    uint64 _page_size;
    uint64 _data_length;

    DBFields::Comparator _comparator;

    std::string _file;
    std::fstream _fs;
};

// initialize the static virables 
template<class Comparator>
int Database::DBIndexManager<Comparator>::BTreeNode::MaxSons = 0;
template<class Comparator>
int Database::DBIndexManager<Comparator>::BTreeNode::EntrySize = 0;
template<class Comparator>
Database::uint64 Database::DBIndexManager<Comparator>::BTreeNode::DataLength = 0;

#endif /* DB_INDEXMANAGER_H_ */
