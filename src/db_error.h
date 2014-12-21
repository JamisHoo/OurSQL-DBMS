/******************************************************************************
 *  Copyright (c) 2014 Jamis Hoo 
 *  Distributed under the MIT license 
 *  (See accompanying file LICENSE or copy at http://opensource.org/licenses/MIT)
 *  
 *  Project: Database
 *  Filename: db_error.h 
 *  Version: 1.0
 *  Author: Jamis Hoo
 *  E-mail: hjm211324@gmail.com
 *  Date: Dec. 20, 2014
 *  Time: 01:08:13
 *  Description: All kinds of errors
 *****************************************************************************/
#ifndef DB_ERROR_H_
#define DB_ERROR_H_

namespace Database{
namespace DBError {

// base class of all
struct Error {
    virtual std::string getInfo() const { return "Error: "; }
    std::string quoted(const std::string& str) const { return '\"' + str + '\"'; }
};

template <class T>
struct DBNotExists: T {
    DBNotExists(const std::string& db_n): T(db_n) { }
    virtual std::string getInfo() const {
        return T::getInfo() + "Database not exists. ";
    }
};
template <class T>
struct DBNotOpened: T {
    template <class ...Para>
    DBNotOpened(const Para&... p): T(p...) { }
    virtual std::string getInfo() const {
        return T::getInfo() + "No database is opened. ";
    }
};
template <class T>
struct DuplicateFieldName: T {
    std::string field_name;
    template <class ...Para>
    DuplicateFieldName(const std::string& fn, const Para&... p): T(p...), field_name(fn) { }
    virtual std::string getInfo() const {
        if (field_name.length()) 
            return T::getInfo() + "Duplicate field name " + T::quoted(field_name) + ". ";
        else 
            return T::getInfo() + "Duplicate field name. ";
    }
};
template <class T>
struct InvalidPrimaryKey: T {
    std::string field_name;
    template <class ...Para>
    InvalidPrimaryKey(const std::string& fn, const Para&... p): T(p...), field_name(fn) { }
    virtual std::string getInfo() const {
        return T::getInfo() + "Invalid primary key field name " + T::quoted(field_name) + ". ";
    }
};
template <class T>
struct ForeignKeyFailed: T {
    template <class ...Para>
    ForeignKeyFailed(const Para&... p): T(p...) { }
    virtual std::string getInfo() const {
        return T::getInfo() + "Foreign key constraint failed. ";
    }
};
template <class T>
struct InvalidFieldName: T {
    std::string field_name;
    template <class ...Para>
    InvalidFieldName(const std::string& fn, const Para&... p): T(p...), field_name(fn) { }
    virtual std::string getInfo() const {
        return T::getInfo() + "Invalid field name " + T::quoted(field_name) + ". ";
    }
};
template <class T>
struct OpenTableFailed: T {
    std::string table_name;
    template <class ...Para>
    OpenTableFailed(const std::string& tn, const Para&... p): T(p...), table_name(tn) { }
    virtual std::string getInfo() const {
        return T::getInfo() + "Failed when opening table " + T::quoted(table_name) + ". ";
    }
};
template <class T>
struct LiteralParseFailed: T {
    std::string literal;
    template <class ...Para>
    LiteralParseFailed(const std::string& l, const Para&... p): T(p...), literal(l) { }
    virtual std::string getInfo() const {
        return T::getInfo() + "Failed when parsing literal " + T::quoted(literal) + ". ";
    }
};
template <class T>
struct LiteralOutOfRange: T {
    std::string literal;
    template <class ...Para>
    LiteralOutOfRange(const std::string& l, const Para&... p): T(p...), literal(l) { }
    virtual std::string getInfo() const {
        return T::getInfo() + "Literal " + T::quoted(literal) + " out of range. ";
    }
};
template <class T>
struct CheckConstraintFailed: T {
    template <class ...Para>
    CheckConstraintFailed(const Para&... p): T(p...) { }
    virtual std::string getInfo() const {
        return T::getInfo() + "Check constraint failed. ";
    }
};
template <class T>
struct ReferencedNotExists: ForeignKeyFailed<T> {
    std::string refed_table_name;
    std::string value;
    template <class ...Para>
    ReferencedNotExists(const std::string v, const std::string rtn, const Para&... p): 
        ForeignKeyFailed<T>(p...), refed_table_name(rtn), value(v) { }
    virtual std::string getInfo() const {
        return ForeignKeyFailed<T>::getInfo() + "Value " + ForeignKeyFailed<T>::quoted(value) + 
               " not exists in referenced table " + ForeignKeyFailed<T>::quoted(refed_table_name) + ". ";
    }
};
template <class T>
struct RecordReferenced: ForeignKeyFailed<T> {
    std::string refing_table_name;
    template <class ...Para>
    RecordReferenced(const std::string& rtn, const Para&... p):
        ForeignKeyFailed<T>(p...), refing_table_name(rtn) { }
    virtual std::string getInfo() const {
        return ForeignKeyFailed<T>::getInfo() + "Record is referenced in table " + 
               ForeignKeyFailed<T>::quoted(refing_table_name) + ". ";
    }
};
template <class T>
struct NotNullExpected: T {
    template <class ...Para>
    NotNullExpected(const Para&... p): T(p...) { }
    virtual std::string getInfo() const {
        return T::getInfo() + "Not null expected. ";
    }
};
template <class T>
struct DuplicatePrimaryKey: T {
    template <class ...Para>
    DuplicatePrimaryKey(const Para&... p): T(p...) { }
    virtual std::string getInfo() const {
        return T::getInfo() + "Duplicate primary key. ";
    }
};
template <class T>
struct InvalidCondition: T {
    template <class ...Para>
    InvalidCondition(const Para&... p): T(p...) { }
};
    template <class T>
    struct InvalidConditionOperator: InvalidCondition<T> {
        std::string op;
        template <class ...Para>
        InvalidConditionOperator(const std::string& o, const Para&... p): InvalidCondition<T>(p...), op(o) { }
        virtual std::string getInfo() const {
            return InvalidCondition<T>::getInfo() + "Invalid operator " + 
                   InvalidCondition<T>::quoted(op) + " in condition clause. ";
        }
    };
    template <class T>
    struct InvalidConditionOperand: InvalidCondition<T> {
        std::string operand;
        template <class ...Para>
        InvalidConditionOperand(const std::string& o, const Para&... p): InvalidCondition<T>(p...), operand(o) { }
        virtual std::string getInfo() const {
            return InvalidCondition<T>::getInfo() + "Invalid operand " + 
                   InvalidCondition<T>::quoted(operand) + " in condition clause. ";
        }
    };


struct ParseFailed: Error {
    virtual std::string getInfo() const {
        return Error::getInfo() + "Parse failed. ";
    }
};

struct CreateDBFailed: Error {
    std::string db_name;
    CreateDBFailed(const std::string& db_n): db_name(db_n) { }
    virtual std::string getInfo() const {
        return Error::getInfo() + "Failed when creating database " + quoted(db_name) + ". ";
    }
};
    struct PathExisted: CreateDBFailed {
        PathExisted(const std::string& db_n): CreateDBFailed(db_n) { }
        virtual std::string getInfo() const {
            return CreateDBFailed::getInfo() + "Path have already existed. ";
        }
    };

struct DropDBFailed: Error {
    std::string db_name;
    DropDBFailed(const std::string& db_n): db_name(db_n) { }
    virtual std::string getInfo() const {
        return Error::getInfo() + "Failed when dropping database " + quoted(db_name) + ". ";
    }
};
    struct RemoveDBFailed: DropDBFailed {
        RemoveDBFailed(const std::string& db_n): DropDBFailed(db_n) { }
        virtual std::string getInfo() const {
            return DropDBFailed::getInfo() + "Remove directory failed. ";
        }
    };

struct UseDBFailed: Error {
    std::string db_name;
    UseDBFailed(const std::string& db_n): db_name(db_n) { }
    virtual std::string getInfo() const {
        return Error::getInfo() + "Failed when opening database " + quoted(db_name) + ". ";
    }
};
    
struct CreateTableFailed: Error {
    std::string table_name;
    CreateTableFailed(const std::string& tbn): table_name(tbn) { }
    virtual std::string getInfo() const {
        return Error::getInfo() + "Failed when creating table " + quoted(table_name) + ". ";
    }
};
    struct FieldNameTooLong: CreateTableFailed {
        std::string field_name;
        uint64 max_length;
        FieldNameTooLong(const std::string& fn, const uint64 max_l, const std::string& tbn): 
            CreateTableFailed(tbn), field_name(fn), max_length(max_l) { }
        virtual std::string getInfo() const {
            return CreateTableFailed::getInfo() + "Field name " + quoted(field_name) + " of length " + 
                std::to_string(field_name.length()) + " exceeds length limitation " + std::to_string(max_length) + ". ";
        }
    };
    struct UnsupportedType: CreateTableFailed {
        std::string type;
        bool is_signed, is_unsigned;
        UnsupportedType(const std::string& t, const bool isu, const bool iss, const std::string& tbn): 
            CreateTableFailed(tbn), type(t), is_signed(iss), is_unsigned(isu) { }
        virtual std::string getInfo() const {
            return CreateTableFailed::getInfo() + "Upsupported type " + 
                   (is_signed? "signed ": "") + (is_unsigned? "unsigned ": "") +
                   type + ". ";
        }
    };
    struct FieldLengthRequired: CreateTableFailed {
        std::string field_name, type;
        FieldLengthRequired(const std::string& fn, const std::string& t, const std::string& tbn):
            CreateTableFailed(tbn), field_name(fn), type(t) { }
        virtual std::string getInfo() const {
            return CreateTableFailed::getInfo() + "Field " + quoted(field_name) + 
                   " of type " + quoted(type) + " requires an explicit length. ";
        }
    };
    struct PrimaryKeyRequired: ForeignKeyFailed<CreateTableFailed> {
        std::string table_name;
        std::string field_name;
        PrimaryKeyRequired(const std::string& tn, const std::string& fn, const std::string& tbn):
            ForeignKeyFailed(tbn), table_name(tn), field_name(fn) { }
        virtual std::string getInfo() const {
            return ForeignKeyFailed::getInfo() + "Field " + quoted(table_name + '.' + field_name) + 
                   " is not primary key. Primay key expected. ";
        }
    };
    struct TypesDismatch: ForeignKeyFailed<CreateTableFailed> {
        std::string field_name;
        std::string foreign_table_name;
        std::string foreign_field_name;
        TypesDismatch(const std::string& fn, const std::string& ftn, const std::string& ffn, const std::string& tn):
            ForeignKeyFailed(tn), field_name(fn), foreign_table_name(ftn), foreign_field_name(ffn) { }
        virtual std::string getInfo() const {
            return ForeignKeyFailed::getInfo() + "Type of field " + 
                   quoted(field_name) + " dismatches with " + 
                   quoted(foreign_table_name + '.' + foreign_field_name) + ". ";
        }
    };
    struct LengthsDismatch: ForeignKeyFailed<CreateTableFailed> {
        std::string field_name;
        std::string foreign_table_name;
        std::string foreign_field_name;
        LengthsDismatch(const std::string& fn, const std::string& ftn, const std::string& ffn, const std::string& tn):
            ForeignKeyFailed(tn), field_name(fn), foreign_table_name(ftn), foreign_field_name(ffn) { }
        virtual std::string getInfo() const {
            return ForeignKeyFailed::getInfo() + "Length of field " + 
                   quoted(field_name) + " dismatches with " + 
                   quoted(foreign_table_name + '.' + foreign_field_name) + ". ";
        }
    };

struct ShowTablesFailed: Error {
    virtual std::string getInfo() const {
        return Error::getInfo() + "Failed when showing tables. ";
    }
};

struct DropTableFailed: Error {
    std::string table_name;
    DropTableFailed(const std::string& tn): table_name(tn) { }
    virtual std::string getInfo() const { 
        return Error::getInfo() + "Failed when dropping table " + quoted(table_name) + ". ";
    }
};
    struct TableReferenced: ForeignKeyFailed<DropTableFailed> {
        std::string referencing_table_name;
        TableReferenced(const std::string& refed_n, const std::string& refing_n):
            ForeignKeyFailed(refed_n), referencing_table_name(refing_n) { }
        virtual std::string getInfo() const {
            return ForeignKeyFailed::getInfo() + "Table is referenced by table " + 
                   quoted(referencing_table_name) + ". ";
        }
    };
    struct RemoveTableFailed: DropTableFailed {
        RemoveTableFailed(const std::string& tbn): DropTableFailed(tbn) { }
        virtual std::string getInfo() const {
            return DropTableFailed::getInfo() + "Remove files failed. ";
        }
    };

struct CreateIndexFailed: Error {
    std::string table_name, field_name;
    CreateIndexFailed(const std::string& tn, const std::string& fn): table_name(tn), field_name(fn) { }
    virtual std::string getInfo() const {
        return Error::getInfo() + "Failed when creating index on " + quoted(table_name + '.' + field_name) + ". ";
    }
};

struct DropIndexFailed: Error {
    std::string table_name, field_name;
    DropIndexFailed(const std::string& tn, const std::string& fn): table_name(tn), field_name(fn) { }
    virtual std::string getInfo() const {
        return Error::getInfo() + "Failed when dropping index on " + quoted(table_name + '.' + field_name) + ". ";
    }
};

struct InsertRecordFailed: Error {
    std::string table_name;
    std::vector<std::string> values;
    InsertRecordFailed(const std::string& tn, const std::vector<std::string>& vs): table_name(tn), values(vs) { }
    virtual std::string getInfo() const {
        std::string tp;
        for (const auto& v: values) tp += v + ',';
        return Error::getInfo() + "Failed when inserting tuple (" + tp.substr(0, tp.length() - 1) + 
               ") into table " + quoted(table_name) + ". ";
    }
};
    struct WrongTupleSize: InsertRecordFailed {
        uint64 expected_size;
        WrongTupleSize(const std::string& tn, const std::vector<std::string>& vs, const uint64 es):
            InsertRecordFailed(tn, vs), expected_size(es) { } 
        virtual std::string getInfo() const {
            return InsertRecordFailed::getInfo() + "Wrong tuple size " + 
                   std::to_string(values.size()) + ", " +
                   std::to_string(expected_size) + " expected. ";
        }
    };
    
struct SimpleSelectFailed: Error {
    std::string table_name;
    SimpleSelectFailed(const std::string& tn): table_name(tn) { }
    virtual std::string getInfo() const {
        return Error::getInfo() + "Failed when selecting from " + quoted(table_name) + ". ";
    }
};
    struct AggregateFailed: SimpleSelectFailed {
        std::string function, field_name;
        AggregateFailed(const std::string& f, const std::string& fn, const std::string& tn):
            SimpleSelectFailed(tn), function(f), field_name(fn) { }
        virtual std::string getInfo() const {
            return SimpleSelectFailed::getInfo() + "Invalid aggregate funtion " + quoted(function) + " applied to " + quoted(field_name) + ". ";
        }
    };
    struct BothGroupAndOrder: SimpleSelectFailed {
        BothGroupAndOrder(const std::string& tn): SimpleSelectFailed(tn) { }
        virtual std::string getInfo() const {
            return SimpleSelectFailed::getInfo() + "Using both GROUP BY and ORDER BY in a single query is not supported for now. ";
        }
    };
    
struct DeleteRecordFailed: Error {
    std::string table_name;
    DeleteRecordFailed(const std::string& tn): table_name(tn) { }
    virtual std::string getInfo() const {
        return Error::getInfo() + "Failed when deleting from " + quoted(table_name) + ". "; 
    }
};

struct UpdateRecordFailed: Error {
    std::string table_name;
    UpdateRecordFailed(const std::string& tn): table_name(tn) { }
    virtual std::string getInfo() const {
        return Error::getInfo() + "Failed when updating table " + quoted(table_name) + ". ";
    }
};


}
}

#endif /* DB_ERROR_H_ */
