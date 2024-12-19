
import Foundation
import SQLite3

public class ABDatabase {
    
    static private var instance: ABDatabase? = nil
    static private var lock = NSLock()
    static private var queue = DispatchQueue(label: "ABDatabase.queue", attributes: .concurrent)
    
    
    static public func getInstance() throws -> ABDatabase {
        ABDatabase.lock.lock()
        
        if let instance = ABDatabase.instance {
            ABDatabase.lock.unlock()
            return instance
        }
        
        let instance = try ABDatabase()
        ABDatabase.instance = instance
        
        return instance
    }
    
    
    private var db: OpaquePointer?
    
    private var transaction_CurrentId: Int?
    private var transaction_NextId: Int
    
    
    public func close() {
        sqlite3_close(db)
    }
    
    public func getTableColumnInfos(_ tableName: String, transactionId: Int? = nil, timeout: Int = 0, onResult: @escaping (_ columnInfos: [ColumnInfo]) -> Void, onError: @escaping (_ error: ABDatabaseError) -> Void) {
        ABDatabase.queue.sync {
            /* Transaction Check */
            var error = validateTransactionId(transactionId)
            if let error {
                if timeout <= 0 {
                    onError(error)
                    return
                }
                    
                DispatchQueue.main.asyncAfter(deadline: .now() + (Double(timeout) / 1000.0)) {
                    self.getTableColumnInfos(tableName, transactionId: transactionId, onResult: onResult, onError: onError)
                }
                return
            }
            
            /* Statement Prepare */
            let query = "PRAGMA TABLE_INFO('\(tableName)')"
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
            
            /* Run Query */
            var columnInfos = [ColumnInfo]()
            while (sqlite3_step(queryStatement) == SQLITE_ROW) {
                columnInfos.append(ColumnInfo(
                    name: getStringFromColumn(queryStatement, 1),
                    type: getStringFromColumn(queryStatement, 2),
                    notNull: (sqlite3_column_int(queryStatement, Int32(3)) as Int32) != 0)
                )
            }
            
            onResult(columnInfos)
        }
    }
    
    public func getTableNames(transactionId: Int? = nil, timeout: Int = 0, onResult: @escaping (_ tableNames: [String]) -> Void, onError: @escaping (_ error: ABDatabaseError) -> Void) {
        ABDatabase.queue.sync {
            /* Transaction Check */
            var error = validateTransactionId(transactionId)
            if let error {
                if timeout <= 0 {
                    onError(error)
                    return
                }
                    
                DispatchQueue.main.asyncAfter(deadline: .now() + (Double(timeout) / 1000.0)) {
                    self.getTableNames(transactionId: transactionId, onResult: onResult, onError: onError)
                }
                return
            }
            
            /* Statement Prepare */
            let query = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
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
            
            /* Run Query */
            var tableNames = [String]()
            while (sqlite3_step(queryStatement) == SQLITE_ROW) {
                tableNames.append(getStringFromColumn(queryStatement, 0))
            }
            
            onResult(tableNames)
        }
    }
    
    public func transaction_Finish(_ transactionId: Int, _ commit: Bool, onResult: @escaping () -> Void, onError: @escaping (_ error: ABDatabaseError) -> Void) {
        ABDatabase.queue.sync {
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
    
    public func transaction_IsAutocommit(onResult: @escaping (_ transactionId: Int?) -> Void, onError: @escaping (_ error: ABDatabaseError) -> Void) {
        ABDatabase.queue.sync {
            var inTransaction: Bool = sqlite3_get_autocommit(db) != 0
            guard inTransaction != (transaction_CurrentId != nil) else {
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
    
    public func transaction_Start(onResult: @escaping (_ transactionId: Int) -> Void, onError: @escaping (_ error: ABDatabaseError) -> Void, timeout: Int = 0) {
        ABDatabase.queue.sync {
            if let transaction_CurrentId {
                if timeout <= 0 {
                    onError(ABDatabaseError.otherTransactionAlreadyInProgress(transaction_CurrentId))
                    return
                }
                
                DispatchQueue.main.asyncAfter(deadline: .now() + (Double(timeout) / 1000.0)) {
                    self.transaction_Start(onResult: onResult, onError: onError)
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
    
    public func query_Execute(_ query: String, transactionId: Int? = nil, onResult: @escaping () -> Void, onError: @escaping (_ error: ABDatabaseError) -> Void, timeout: Int = 0) {
        ABDatabase.queue.sync {
            /* Transaction Check */
            var error = validateTransactionId(transactionId)
            if let error {
                if timeout <= 0 {
                    printError("Cannot run query: " + query)
                    onError(error)
                    return
                }
                    
                DispatchQueue.main.asyncAfter(deadline: .now() + (Double(timeout) / 1000.0)) {
                    self.query_Execute(query, transactionId: transactionId, onResult: onResult, onError: onError)
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
                return
            }
            
            do {
                sqlite3_finalize(queryStatement)
            } catch (let error) {
                onError(ABDatabaseError.cannotFinalize("\(error)"))
                return
            }
            
            onResult()
        }
    }
    
    public func query_Select(_ query: String, _ columnTypes: [SelectColumnType], transactionId: Int? = nil, onResult: @escaping (_ rows: [[AnyObject]]) -> Void, onError: @escaping (_ error: ABDatabaseError) -> Void, timeout: Int = 0)  {
        ABDatabase.queue.sync {
            /* Transaction Check */
            var error = self.validateTransactionId(transactionId)
            if let error {
                if timeout <= 0 {
                    printError("Cannot run query: " + query)
                    onError(error)
                    return
                }
                    
                DispatchQueue.main.asyncAfter(deadline: .now() + (Double(timeout) / 1000.0)) {
                    self.query_Select(query, columnTypes, transactionId: transactionId, onResult: onResult, onError: onError)
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
                    } else if columnTypes[i] == SelectColumnType.Bool {
                        row.append((sqlite3_column_int(queryStatement, Int32(i)) != 0) as AnyObject)
                    } else if columnTypes[i] == SelectColumnType.Float {
                        row.append(sqlite3_column_double(queryStatement, Int32(i)) as AnyObject)
                    } else if columnTypes[i] == SelectColumnType.Int {
                        row.append(sqlite3_column_int(queryStatement, Int32(i)) as AnyObject)
                    } else if columnTypes[i] == SelectColumnType.JSON {
                        guard let columnValue_Result = sqlite3_column_text(queryStatement, Int32(i)) else {
                            printError("Cannot parse row json.")
                            row.append(NSNull())
                            continue
                        }
                        let json_Str: String = String(cString: columnValue_Result)
                        let json_Data: Data? = json_Str.data(using: .utf8)
                        if let json_Data {
                            let json_Parsed = try? JSONSerialization.jsonObject(with: json_Data)
                            if let json = json_Parsed as? [String: AnyObject] {
                                row.append(json as AnyObject)
                                continue
                            }
                        }
                        
                        printError("Cannot parse row json: " + json_Str)
                        row.append(NSNull())
                    } else if columnTypes[i] == SelectColumnType.Long {
                        row.append(sqlite3_column_int64(queryStatement, Int32(i)) as AnyObject)
                    } else if (columnTypes[i] == SelectColumnType.String) {
                        row.append(self.getStringFromColumn(queryStatement, i) as AnyObject)
                    }
                }
                rows.append(row)
            }
            sqlite3_finalize(queryStatement)
            
            onResult(rows)
        }
    }
    
    
    private init() throws {
        transaction_CurrentId = nil
        transaction_NextId = 0
        
        let fileUrl = try! FileManager.default
            .url(for: .applicationSupportDirectory, in: .userDomainMask, appropriateFor: nil, create: true)
            .appendingPathComponent("ab-database.sqlite")
        
        guard sqlite3_open(fileUrl.path, &db) == SQLITE_OK else {
            sqlite3_close(db)
            throw ABDatabaseError.cannotOpenDatabase
        }
        
        guard let db else {
            throw ABDatabaseError.cannotOpenDatabase
        }
    }
    
    private func getStringFromColumn(_ queryStatement: OpaquePointer, _ index: Int) -> String {
        guard let columnValue_Result = sqlite3_column_text(queryStatement, Int32(index)) else {
            printError("Cannot get string from row.")
            return ""
        }
        
        return String(cString: columnValue_Result)
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


//public enum ColumnType {
//    case double
//    case int
//    case text
//}

public struct ColumnInfo {
    public let name: String
    public let type: String
    public let notNull: Bool
}

public enum ABDatabaseError: Error {
    case cannotBeginTransaction
    case cannotCommit
    case cannotExecute(_ message: String)
    case cannotFinalize(_ message: String)
    case cannotOpenDatabase
    case cannotPrepare(_ message: String)
    case cannotRollback
    case databaseNotOpened
    case noTransactionInProgress
    case otherTransactionAlreadyInProgress(_ currentTransactionId: Int)
    case transactionIdInconsistency(_ currentTransactionId: Int?, _ inTransaction: Bool)
    case wrongTransactionId(_ currentTransactionId: Int?, _ transactionId: Int?)
}
