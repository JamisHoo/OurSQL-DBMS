/******************************************************************************
 *  Copyright (c) 2014 Jinming Hu 
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: 
 *  Filename: db_file_test.cc 
 *  Version: 1.0
 *  Author: Jinming Hu
 *  E-mail: hjm211324@gmail.com
 *  Date: Oct. 24, 2014
 *  Time: 08:54:22
 *  Description: 
 *****************************************************************************/
#include <iostream>
#include <fstream>
#include <cassert>
#include "../src/db_file.h"

int main() {
    using namespace std;
    using namespace Database;

    DBFile file("filename");
    // open an non-existing file, return 0 
    assert(file.open() == 0);
    
    // make 0th page
    char buffer[4096];
    uint64 page_size = 4096;
    uint64 num_pages = 4;
    memset(buffer, 0xdd, 4096);
    memcpy(buffer, &page_size, 8);
    memcpy(buffer + 8, &num_pages, 8);

    // create file
    file.create(4096, buffer);
    assert(file.accessible() == 1);

    // open file
    page_size = file.open();
    assert(page_size == 4096); 
    assert(file.pageSize() == page_size);
    assert(file.numPages() == num_pages);
    assert(file.isopen());

    // write data
    memset(buffer, 0x12, 4096);
    file.writePage(3, buffer);
    memset(buffer, 0xf0, 4096);
    file.writePage(10, buffer);

    assert(file.numPages() == 10);
    
    // read data
    char read_buffer[4096];
    file.readPage(3, read_buffer);
    memset(buffer, 0x12, 4096);
    assert(!memcmp(buffer, read_buffer, 4096));
    file.readPage(10, read_buffer);
    memset(buffer, 0xf0, 4096);
    assert(!memcmp(buffer, read_buffer, 4096));
    
    // close file
    assert(file.close() == 0);
    assert(!file.isopen());

    // remove file
    assert(file.remove() == 0);
    assert(!file.accessible());

}
