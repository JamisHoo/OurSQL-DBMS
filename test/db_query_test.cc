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

    query.execute("create database student;");
    query.execute("use  student;");
    // query.execute("Create TabLE student (student_id int(10) not null, student_name varchar(7), IQ int, primary key(student_id));");
    query.execute("Create TabLE student (student_id int(10) , student_name varchar(7), IQ int);");
    query.execute("show tables;");
    query.execute("desc student;");

    query.execute("insert into student values (null, null, null), (null, 'namex', null), (null, null, 2000); ");
    query.execute("insert into student values (106, null, null), (107, 'name7', null), (-108, 'name8', null), (109, null, 109), (110, null, 1000);");
    query.execute("insert into student values (101, 'name1', 1000), (-102, 'name2', -102), (-103, 'name3', -300), (104, 'name4', true), (105, 'name5', 105);");
    
    /*
    query.execute("select * from student ;");
    query.execute("select student_id, student_name from student where student_id =101 and student_name = 'name1';");
    query.execute("select * from student where true;");
    query.execute("select student_id, student_name from student where false;");
    query.execute("select * from student where student_id = IQ;");
    query.execute("select * from student where student_id >= IQ;");
    query.execute("select * from student where student_id < IQ;");
    query.execute("select * from student where student_id != IQ;");
    query.execute("select * from student where student_id is null;");
    query.execute("select * from student where IQ is not null;");
    */
    
    query.execute("select * from student; ");
    query.execute("select * from student where student_id is null and IQ is not null;");
    query.execute("select * from student where student_id > 0 and student_name is not null and IQ >= student_id;");

    query.execute("drop table student;");
    query.execute("drop database student;");

    std::cout << std::endl;
} 
