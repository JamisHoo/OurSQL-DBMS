/******************************************************************************
 *  Copyright (c) 2014 Jinming Hu 
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: 
 *  Filename: db_query_test.cc 
 *  Version: 1.0
 *  Author: Jinming Hu
 *  E-mail: hjm211324@gmail.com
 *  Date: Dec. 05, 2014
 *  Time: 20:43:41
 *  Description: 
 *****************************************************************************/
#include <iostream>
#include "../src/db_query.h"

int main() {
    using namespace std;
    using namespace Database;
    
    DBQuery query;

    
    /* 
    assert(query.execute(" \t\r\ncreate\rdatabase\n__fuck123 ;") == 0);
    cout << query.db_inuse << endl;

    assert(query.execute("use __fuck123;") == 0);
    cout << query.db_inuse << endl;
    assert(query.execute("show databases;") == 0);
    assert(query.execute("DROP dataBAse     __fuck123;") == 0);
    cout << query.db_inuse << endl;
    */
    
    /*
    query.execute("create table fuck ( shit shit2 (12), shit3 shit4 Not nuLL, shit5 shit6(0), priMary key(chedan));");
    query.execute("create table create ( shit shit2 (12), shit3 shit4 Not nuLL, shit5 shit6( -3));");
    query.execute("create table fuck ( shit shit2 (12), shit3 shit4 Not nuLL, pRimary key (chedan \n), shit5 shit6( -3));");
    query.execute("Create table fuck( primary key (_ad));");
    */
    
    query.execute("Create table fuck ( shit shit );");
    query.execute("CREATE TABLE fuck (shit shit , primary key (chedan));");
    query.execute("CREATE TABLE createx (shit shit , primary key (chedan));");
} 
