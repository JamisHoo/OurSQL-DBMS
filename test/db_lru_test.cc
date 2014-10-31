/******************************************************************************
 *  Copyright (c) 2014 Jinming Hu 
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: 
 *  Filename: db_lru_test.cc 
 *  Version: 1.0
 *  Author: Jinming Hu
 *  E-mail: hjm211324@gmail.com
 *  Date: Oct. 31, 2014
 *  Time: 19:37:49
 *  Description: 
 *****************************************************************************/
#include <iostream>
#include "../src/db_common.h"
#include "../src/db_buffer.h"


int main() {
    using namespace std;
    using namespace Database;

    auto callback = [](const uint64 a, const uint64 b) {
        cout << "write back buffer page " << b << " to disk page " << a << endl;
    };

    DBBuffer::LRU lru(4);

    uint64 bufferID = lru.find(1000, callback); cout << bufferID << endl;
    bufferID = lru.find(1005, callback); cout << bufferID << endl;
    lru.markDirty(1005);
    bufferID = lru.find(1003, callback); cout << bufferID << endl;
    bufferID = lru.find(1000, callback); cout << bufferID << endl;
    bufferID = lru.find(1001, callback); cout << bufferID << endl;
    bufferID = lru.find(1003, callback); cout << bufferID << endl;
    bufferID = lru.find(1004, callback); cout << bufferID << endl;
    bufferID = lru.find(1002, callback); cout << bufferID << endl;
    lru.markDirty(1002);
    
    
    lru.traverseDirtyBuffer(callback);


    cout << "Stack: " << endl;
    lru.display(cout);
    

}

