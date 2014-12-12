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
    // this should pass
    query.execute("create table fuck ( shit smallint (12), shit3 varchar Not nuLL, shit5 char(0), priMary key(chedan));");
    // test minus filed length
    query.execute("create table createx ( shit tinyint (12), shit3 bigint Not nuLL, shit5 char( -3));");
    // test primary location
    query.execute("create table fuck ( shit bool (12), shit3 varchar Not nuLL, pRimary key (chedan \n), shit5 int( -3));");
    // test no field description
    query.execute("Create table fuck( primary key (_ad));");
    // this illegal table name
    query.execute("Create table create ( shit int );");
    // test illegal field name
    query.execute("CREATE TABLE fuck (table int , primary key (chedan));");
    // this should pass
    query.execute("CREATE TABLE createx (tablex int , primary key (chedan));");
    */
    
    // cout << query.execute("Create TabLE ints (student_id int unsigned signed not null, student_name varchar(100) not nUll, clever bool);");
    // cout << query.execute("Create TabLE ints (student_id int signed not null, student_name varchar(100) not nUll, clever bool);");
    // cout << query.execute("Create TabLE ints (student_id int unsigned not null, student_name varchar(100) not nUll, clever bool);");
    // cout << query.execute("Create TabLE ints (student_id int unsigned not null, student_name varchar(100) unsigned not nUll, clever bool);");

    cout << query.execute("create database student;");
    cout << query.execute("use  student;");
    cout << query.execute("Create TabLE student (student_id int(10) not null, student_name varchar(4), clever bool, primary key(student_id));");
    // cout << query.execute("Create TabLE student (student_id int(10) not null, student_name varchar(100), clever bool);");
    // cout << query.execute("show tables;");
    // cout << query.execute("create index on student(student_name);");
    // cout << query.execute("drop index on student(student_name);");
    // cout << query.execute("desc student;");
    // cout << query.execute("desc stu;");

    cout << query.execute("insert intostudent values(100, 'name1', true);");
    cout << query.execute("insert into student values (101, 'name1', false);");
    cout << query.execute("insert into student values (101, 'name3', false);");
    cout << query.execute("insert into student values (102, NULL, false);");
    cout << query.execute("insert into student values (102, 'name4', NULL);");

    /*
    cout << query.execute("select student_id from student where '12132' =  1;");
    cout << query.execute("select student_id, student_name from student where 1.3232=  \tstudent_name;");
    cout << query.execute("select student_id, student_name from student where student_id=student_name ;");
    cout << query.execute("select * from student where student_id = 'chedan';");
    cout << query.execute("select *, student_name from student where student_id = 100000;");
    cout << query.execute("select *  from student where student__id = 100000;");
    cout << query.execute("select student_id from student where student_id is 1000;");
    */

    cout << query.execute("select student_id, student_name from student where student_id =1;");
    cout << query.execute("select student_id, student_name from student where student_id>= 10320;");
    cout << query.execute("select student_id, student_name, clever from student ;");
    cout << query.execute("select * from student ;");
    cout << query.execute("select student_id from student where student_id is null;");
    cout << query.execute("select student_id from student where student_id is not null;");



    cout << query.execute("drop table student;");
    // cout << query.execute("show tables;");
    cout << query.execute("drop database student;");
    


    std::cout << std::endl;
} 
