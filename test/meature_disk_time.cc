/******************************************************************************
 *  Copyright (c) 2014 Jinming Hu 
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: 
 *  Filename: meature_disk_time.cc 
 *  Version: 1.0
 *  Author: Jinming Hu
 *  E-mail: hjm211324@gmail.com
 *  Date: Nov. 05, 2014
 *  Time: 18:58:16
 *  Description: 
 *****************************************************************************/
#include <iostream>
#include <fstream>
#include <chrono>

constexpr int page_size = 4096;
constexpr int num_pages = 32580;
char buffer[page_size * num_pages];

int main() {
    using namespace std;
    
    for (int i = 0; i < page_size * num_pages; ++i)
        buffer[i] = rand();


    auto start = chrono::system_clock::now();
    
    fstream fs("output", fstream::in | fstream::out | fstream::binary);

    for (int i = 0; i < 1360000 * 2; ++i) {
        int pageID = rand() % num_pages;
        fs.seekp(page_size * pageID);
        fs.write(buffer + pageID * page_size, page_size);
    }
    fs.close();

    auto end = chrono::system_clock::now();


    auto duration = end - start;

    auto hours = chrono::duration_cast<chrono::hours>(duration);
    auto minutes = chrono::duration_cast<chrono::minutes>(duration);
    auto seconds = chrono::duration_cast<chrono::seconds>(duration);
    auto milliseconds = chrono::duration_cast<chrono::milliseconds>(duration);
    auto microseconds = chrono::duration_cast<chrono::microseconds>(duration);
    auto nanoseconds = chrono::duration_cast<chrono::nanoseconds>(duration);

    cout << hours.count() << "h. \n" 
         << minutes.count() << "m. \n"
         << seconds.count() << "s. \n"
         << milliseconds.count() << "ms. \n"
         << microseconds.count() << "us. \n"
         << nanoseconds.count() << "ns. " << endl;
}
