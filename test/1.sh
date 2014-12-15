#!/bin/sh
###############################################################################
 #  Copyright (c) 2014 Jamis Hoo 
 #  Distributed under the MIT license 
 #  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 #  
 #  Project: 
 #  Filename: 1.sh 
 #  Version: 1.0
 #  Author: Jamis Hoo
 #  E-mail: hjm211324@gmail.com
 #  Date: Dec. 11, 2014
 #  Time: 10:47:08
 #  Description: 
###############################################################################
./a.out < dataset/create.sql        | grep 1
./a.out < dataset/book.sql          | grep 1
./a.out < dataset/customer.sql      | grep 1
./a.out < dataset/orders.sql        | grep 1
./a.out < dataset/publisher.sql     | grep 1

