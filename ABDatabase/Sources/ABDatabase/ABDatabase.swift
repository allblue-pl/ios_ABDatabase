//
//  ABSqlLiteDatabase.swift
//  Alta Associations
//
//  Created by Jakub Zolcik on 21/03/2021.
//

import Foundation
import SQLite3

public class ABDatabase {
    
    private var db: OpaquePointer
    private let queue: DispatchQueue
    
    private var transaction_CurrentId: Int?
    private var transaction_NextId: Int
    
    
    init() throws {
        var test = TestClass()
        
        queue = DispatchQueue(label: "ABDatabase.queue", attributes: .concurrent)
    
        transaction_CurrentId = nil
        transaction_NextId = 0
        
        var dbRef: OpaquePointer?
        
        let fileUrl = try! FileManager.default
            .url(for: .applicationSupportDirectory, in: .userDomainMask, appropriateFor: nil, create: true)
            .appendingPathComponent("ab-database.sqlite")
        
        guard sqlite3_open(fileUrl.path, &dbRef) == SQLITE_OK else {
            sqlite3_close(dbRef)
            throw ABDatabaseError.cannotOpenDatabase
        }
        
        guard let dbRef else {
            throw ABDatabaseError.cannotOpenDatabase
        }
        
        db = dbRef
    }
    
    
    public func close() {
        sqlite3_close(db)
    }
    
    public func transaction_Finish(transactionId: Int?, _ commit: Bool, execute onError: @escaping (_ error: ABDatabaseError) -> Void, execute onResult: @escaping () -> Void) {
        queue.sync {
            guard transaction_CurrentId != nil else {
                onError(ABDatabaseError.noTransactionInProgress)
                return
            }
            
            var error = validateTransactionId(transactionId)
            if let error {
                onError(error)
                return
            }
            
            if !commit {
                if sqlite3_exec(self.db, "ROLLBACK", nil, nil, nil) != SQLITE_OK {
                    onError(ABDatabaseError.cannotRollback)
                    return
                }
            }

            if sqlite3_exec(self.db, "COMMIT", nil, nil, nil) != SQLITE_OK && commit {
                onError(ABDatabaseError.cannotCommit)
                return
            }
            
            transaction_CurrentId = nil
            onResult()
        }
    }
    
    public func transaction_IsAutocommit(execute onError: @escaping (_ error: ABDatabaseError) -> Void, execute onResult: @escaping (_ transactionId: Int?) -> Void) {
        queue.sync {
            var inTransaction: Bool = sqlite3_get_autocommit(db) != 0
            guard inTransaction == (transaction_CurrentId != nil) else {
                onError(ABDatabaseError.transactionIdInconsistency(transaction_CurrentId, inTransaction))
                return
            }
            
            onResult(transaction_CurrentId)
        }
    }
    
//    public func transaction_Rollback() throws {
//        try self.queue.sync {
//            if self.db == nil {
//                throw ABDatabaseError.databaseNotOpened
//            }
//            
////            print("Transaction Rollback")
//            
//            if sqlite3_exec(self.db, "ROLLBACK", nil, nil, nil) != SQLITE_OK {
//                throw ABDatabaseError.cannotRollback
//            }
//        }
//    }
    
    public func transaction_Start(execute onResult: @escaping (_ transactionId: Int) -> Void, execute onError: @escaping (_ error: ABDatabaseError) -> Void, timeout: Int = 0) {
        queue.sync {
            if let transaction_CurrentId {
                if timeout <= 0 {
                    onError(ABDatabaseError.otherTransactionAlreadyInProgress(transaction_CurrentId))
                    return
                }
                
                DispatchQueue.main.asyncAfter(deadline: .now() + (Double(timeout) / 1000.0)) {
                    self.transaction_Start(execute: onResult, execute: onError)
                }
                return
            }
            
            if sqlite3_exec(self.db, "BEGIN TRANSACTION", nil, nil, nil) != SQLITE_OK {
                onError(ABDatabaseError.cannotBeginTransaction)
                return
            }
            
            transaction_CurrentId = transaction_NextId
            transaction_NextId += 1
            
            if let transaction_CurrentId {
                onResult(transaction_CurrentId)
            } else {
                onError(ABDatabaseError.cannotBeginTransaction)
            }
        }
    }
    
    public func query_Execute(_ query: String, _ transactionId: Int?, execute onResult: @escaping () -> Void, execute onError: @escaping (_ error: ABDatabaseError) -> Void, timeout: Int = 0) {
        queue.sync {
            /* Transaction Check */
            var error = validateTransactionId(transactionId)
            if let error {
                if timeout <= 0 {
                    printError("Cannot run query: " + query)
                    onError(error)
                    return
                }
                    
                DispatchQueue.main.asyncAfter(deadline: .now() + (Double(timeout) / 1000.0)) {
                    self.query_Execute(query, transactionId, execute: onResult, execute: onError)
                }
                return
            }
            
            /* Statement Prepare */
            let queryStatement: OpaquePointer
            do {
                queryStatement = try rawQuery(query)
            } catch ABDatabaseError.cannotPrepare(let message) {
                onError(ABDatabaseError.cannotPrepare(message))
                return
            } catch (let error) {
                onError(ABDatabaseError.cannotPrepare("\(error)"))
                return
            }

            /* Query Execute */
            if sqlite3_step(queryStatement) != SQLITE_DONE {
                let error = String(cString: sqlite3_errmsg(self.db))
                
                sqlite3_finalize(queryStatement)
                onError(ABDatabaseError.cannotExecute(error))
            }
            
            sqlite3_finalize(queryStatement)
            
            onResult()
        }
    }
    
    public func query_Select(_ query: String, _ columnTypes: [SelectColumnType], _ transactionId: Int?, execute onResult: (_ rows: [[AnyObject]]) -> Void, execute onError: (_ error: ABDatabaseError) -> Void, timeout: Int = 0)  {
        queue.sync {
            /* Transaction Check */
            var error = self.validateTransactionId(transactionId)
            if let error {
                if timeout <= 0 {
                    printError("Cannot run query: " + query)
                    onError(error)
                    return
                }
                    
                DispatchQueue.main.asyncAfter(deadline: .now() + (Double(timeout) / 1000.0)) {
                    self.query_Select(query, columnTypes, transactionId, execute: onResult, execute: onError)
                }
                return
            }
            
            /* Statement Prepare */
            let queryStatement: OpaquePointer
            do {
                queryStatement = try rawQuery(query)
            } catch ABDatabaseError.cannotPrepare(let message) {
                onError(ABDatabaseError.cannotPrepare(message))
                return
            } catch (let error) {
                onError(ABDatabaseError.cannotPrepare("\(error)"))
                return
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
    
    
    private func printError(_ message: String) {
        print("ABDatabase Error -> \(message)")
    }
    
    private func rawQuery(_ query:String) throws -> OpaquePointer {
        var queryStatement: OpaquePointer?
        
        if sqlite3_prepare_v2(db, query, -1, &queryStatement, nil) != SQLITE_OK {
            let error = String(cString: sqlite3_errmsg(self.db))
            throw ABDatabaseError.cannotPrepare(error)
        }
        
        if let queryStatement {
            return queryStatement
        }
        
        throw ABDatabaseError.cannotPrepare("Cannot get query statement.")
    }
    
    private func validateTransactionId(_ transactionId: Int?) -> ABDatabaseError? {
        guard transaction_CurrentId != nil else {
            if transactionId == nil {
                return nil
            }
            
            return ABDatabaseError.wrongTransactionId(transaction_CurrentId, transactionId)
        }
        
        guard transactionId != nil else {
            return ABDatabaseError.wrongTransactionId(transaction_CurrentId, transactionId)
        }
        
        guard transaction_CurrentId == transactionId else {
            return ABDatabaseError.wrongTransactionId(transaction_CurrentId, transactionId)
        }
        
        return nil
    }
    
    
    
    
}


public enum ColumnType {
    case double
    case int
    case text
}

public enum ABDatabaseError: Error {
    case cannotBeginTransaction
    case cannotCommit
    case cannotExecute(_ message: String)
    case cannotOpenDatabase
    case cannotPrepare(_ message: String)
    case cannotRollback
    case databaseNotOpened
    case noTransactionInProgress
    case otherTransactionAlreadyInProgress(_ currentTransactionId: Int)
    case transactionIdInconsistency(_ currentTransactionId: Int?, _ inTransaction: Bool)
    case wrongTransactionId(_ currentTransactionId: Int?, _ transactionId: Int?)
}
