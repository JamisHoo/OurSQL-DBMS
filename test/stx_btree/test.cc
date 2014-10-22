/******************************************************************************
 *  Copyright (c) 2014 Jinming Hu 
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: 
 *  Filename: test.cc 
 *  Version: 1.0
 *  Author: Jinming Hu
 *  E-mail: hjm211324@gmail.com
 *  Date: Oct. 22, 2014
 *  Time: 18:08:06
 *  Description: 
 *****************************************************************************/

#include <iostream>
#include "btree_multimap"

int main(int argc, char** argv) {
    stx::btree_multimap<long, long> mymap;

    for (int i = 0; i < 300000; ++i)
       mymap.insert(std::make_pair(i, 10 * i));


    mymap.dump(std::cout);
    mymap.restore(std::cin);

    std::cout << mymap.find(231232) -> second << std::endl;







}
