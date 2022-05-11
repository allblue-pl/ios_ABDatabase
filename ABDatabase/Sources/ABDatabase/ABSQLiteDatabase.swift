//
//  ABSqlLiteDatabase.swift
//  Alta Associations
//
//  Created by Jakub Zolcik on 21/03/2021.
//

import Foundation
import SQLite3

public class ABSQLiteDatabase
{
    
    private var db: OpaquePointer?
    private let queue = DispatchQueue(label: "ABSQLiteDatabase.queue", attributes: .concurrent)
    private var transaction_Autocommit = true
    
    
    public func close() {
        if (self.db != nil) {
            sqlite3_close(self.db)
            self.db = nil
        }
    }
    
    public func open() -> Bool {
        var db: OpaquePointer? = nil
    
        let fileUrl = try! FileManager.default
            .url(for: .applicationSupportDirectory, in: .userDomainMask, appropriateFor: nil, create: true)
            .appendingPathComponent("ab-database.sqlite")
        
        guard sqlite3_open(fileUrl.path, &db) == SQLITE_OK else {
            sqlite3_close(db)
            return false
        }
        
        if db == nil {
            return false
        }
        
        self.db = db
        return true
    }
    
    public func transaction_Finish(_ commit: Bool) throws {
        try self.queue.sync {
            if self.db == nil {
                throw ABSQLiteDatabaseError.databaseNotOpened
            }
            
            if !commit {
                try self.transaction_Rollback()
            }
            
//            print("Transaction Finish")

            if sqlite3_exec(self.db, "COMMIT", nil, nil, nil) != SQLITE_OK && commit {
                throw ABSQLiteDatabaseError.cannotCommit
            }
            
            self.transaction_Autocommit = true
        }
    }
    
    public func transaction_IsAutocommit() throws -> Bool {
        self.queue.sync {
//            print("Transaction Check " + (self.transaction_Autocommit ? "Yes" : "No"))
            
            return self.transaction_Autocommit
        }
    }
    
    public func transaction_Rollback() throws {
        try self.queue.sync {
            if self.db == nil {
                throw ABSQLiteDatabaseError.databaseNotOpened
            }
            
//            print("Transaction Rollback")
            
            if sqlite3_exec(self.db, "ROLLBACK", nil, nil, nil) != SQLITE_OK {
                throw ABSQLiteDatabaseError.cannotRollback
            }
        }
    }
    
    public func transaction_Start() throws {
        try self.queue.sync {
            if self.db == nil {
                throw ABSQLiteDatabaseError.databaseNotOpened
            }
            
//            print("Transaction Start " + (try self.transaction_IsAutocommit() ? "Yes" : "No"))
            
            if sqlite3_exec(self.db, "BEGIN TRANSACTION", nil, nil, nil) != SQLITE_OK {
                throw ABSQLiteDatabaseError.cannotBeginTransaction
            }
            
            self.transaction_Autocommit = false
        }
    }
    
    public func query_Execute(_ query: String) throws {
        try self.queue.sync {
            guard let queryStatement = try self.rawQuery(query) else { return }
            
            if sqlite3_step(queryStatement) != SQLITE_DONE {
                let error = String(cString: sqlite3_errmsg(self.db))
                
                sqlite3_finalize(queryStatement)
                throw ABSQLiteDatabaseError.cannotExecute(error)
            }
            
            sqlite3_finalize(queryStatement)
        }
    }
    
    public func query_Select(_ query: String, _ columnTypes: [ColumnType]) throws -> [[AnyObject]] {
        try self.queue.sync {
            guard let queryStatement = try self.rawQuery(query) else {
                return []
            }
            
            let columnsCount = columnTypes.count
            var rows = [[AnyObject]]()
            while (sqlite3_step(queryStatement) == SQLITE_ROW) {
                var row = [AnyObject]()
                for i in 0..<columnsCount {
                    if sqlite3_column_type(queryStatement, Int32(i)) == SQLITE_NULL {
                        row.append(NSNull())
                    } else if columnTypes[i] == .double {
                        row.append(sqlite3_column_double(queryStatement, Int32(i))as AnyObject)
                    } else if columnTypes[i] == .int {
                        row.append(sqlite3_column_int(queryStatement, Int32(i)) as AnyObject)
                    } else if (columnTypes[i] == .text) {
                        guard let columnValue_Result = sqlite3_column_text(queryStatement, Int32(i)) else {
//                            print("Something to do")
                            row.append("" as AnyObject)
                            continue
                        }
                        
                        row.append(String(cString: columnValue_Result) as AnyObject)
                    }
                }
                rows.append(row)
            }
            sqlite3_finalize(queryStatement)
            
            return rows
        }
    }
    
    
    private func error(_ message: String) {
        print("ABSQLiteDatabase Error -> \(message)")
    }
    
    private func rawQuery(_ query:String) throws -> OpaquePointer? {
        if self.db == nil {
            throw ABSQLiteDatabaseError.databaseNotOpened
        }
        
        var queryStatement: OpaquePointer?
        
        if sqlite3_prepare_v2(db, query, -1, &queryStatement, nil) != SQLITE_OK {
            let error = String(cString: sqlite3_errmsg(self.db))
            throw ABSQLiteDatabaseError.cannotPrepare(error)
        }
        
        return queryStatement
    }
    
    public enum ColumnType {
        case double
        case int
        case text
    }
    
}


public enum ABSQLiteDatabaseError: Error {
    case cannotBeginTransaction
    case cannotCommit
    case cannotExecute(_ message: String)
    case cannotPrepare(_ message: String)
    case cannotRollback
    case databaseNotOpened
}
