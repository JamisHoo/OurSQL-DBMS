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
 *  Date: Oct. 23, 2014
 *  Time: 00:04:39
 *  Description: 
 *****************************************************************************/

#include <iostream>
#include <tuple>

struct Base {
    virtual ~Base() { }
    virtual int size() const = 0;
    Base 
};

template <class T>
struct Value: Base {
    T data;
    explicit Value(T const& d): data(d) { }
    virtual int size() const {
        return sizeof(T);
    }
};


int main() {
    using namespace std;
    int a;

    cin >> a;

    Base* p;

    if (a == 0) {
        p = new Value<int>(2);
    } else if (a == 1) {
        p = new Value<double>(2.20);
    } else if (a == 2) {
        p = new Value<char>('a');
    }

    cout << p -> size() << endl;

}

    
