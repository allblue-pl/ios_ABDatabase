//
//  ABDatabase.swift
//  Alta Associations
//
//  Created by Jakub Zolcik on 21/03/2021.
//

import ABNative
import Foundation

public class ABDatabase {
    
    public var db: ABSQLiteDatabase =  ABSQLiteDatabase()
    public var nativeActions: ABNativeActionsSet = ABNativeActionsSet()
    
    public init() {
        if (!self.db.open()) {
            self.error("Cannot open database.")
            return
        }
        
        self.nativeActions
            .addNative("GetTableColumnInfos", ABNativeAction(callFn: { (args: [String: AnyObject]) in
                guard let tableName = args["tableName"] as? String else {
                    return ABNativeAction.jsonArgsError()
                }
                
                var result = [String: AnyObject]()
                
                do {
                    var columnTypes = [ABSQLiteDatabase.ColumnType]()
                    columnTypes.append(.int)
                    columnTypes.append(.text)
                    columnTypes.append(.text)
                    columnTypes.append(.int)

                    let rows_DB = try self.db.query_Select("PRAGMA TABLE_INFO('\(tableName)')", columnTypes)
                    var columnInfos = [[String: AnyObject]]()
                    for row_DB in rows_DB {
                        var columnInfo = [String: AnyObject]()
                        columnInfo["name"] = row_DB[1] as AnyObject
                        columnInfo["type"] = row_DB[2] as AnyObject
                        columnInfo["notNull"] = ((row_DB[3] as? Int? ?? 0) == 0 ? false : true) as AnyObject
                        
                        columnInfos.append(columnInfo)
                    }
                    
                    result["columnInfos"] = columnInfos as AnyObject?
                    result["error"] = NSNull()
                } catch {
                    result["columnInfos"] = NSNull()
                    result["error"] = "\(error)" as AnyObject?
                }
                    
                return result
            }))
            .addNative("GetTableNames", ABNativeAction(callFn: { (args: [String: AnyObject]) in
                var result = [String: AnyObject]()
                
                do {
                    var columnNames: [ABSQLiteDatabase.ColumnType] = [ .text ]
                    let rows = try self.db.query_Select("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'", columnNames)
                    
                    var tableNames = [String]()
                    for row in rows {
                        guard let tableName = row[0] as? String else {
                            continue
                        }
                        
                        tableNames.append(tableName)
                    }
                    
                    result["tableNames"] = tableNames as AnyObject?
                    result["error"] = NSNull()
                } catch {
                    result["tableNames"] = NSNull()
                    result["error"] = "\(error)" as AnyObject?
                }
                
                return result
            }))
            .addNative("Transaction_Finish", ABNativeAction(callFn: { (args: [String: AnyObject]) in
                print("Transaction_Finish")
                
                var result = [String: AnyObject]()
                
                do {
                    guard let commit = args["commit"] as? Bool else {
                        throw ABNativeActionError.cannotParseJSON
                    }
                    
                    try self.db.transaction_Finish(commit)
                    
                    self.db.transaction_CurrentId = nil
                    
                    result["error"] = NSNull()
                } catch {
                    result["error"] = "\(error)" as AnyObject?
                }
                
                return result
            }))
            .addNative("Transaction_IsAutocommit", ABNativeAction(callFn: { (args: [String: AnyObject]) in
                print("Transaction_IsAutocommit")
                
                var result = [String: AnyObject]()
                
                do {
                    result["transactionId"] = self.db.transaction_CurrentId == nil ? NSNull() : (self.db.transaction_CurrentId as AnyObject?)
                    result["error"] = NSNull()
                } catch {
                    result["transactionId"] = NSNull() as AnyObject?
                    result["error"] = "\(error)" as AnyObject?
                }
                
                return result
            }))
            .addNative("Transaction_Start", ABNativeAction(callFn: { (args: [String: AnyObject]) in
                print("Transaction_Start")
                
                var result = [String: AnyObject]()
                do {
                    try self.db.transaction_Start()
                    
                    self.db.transaction_CurrentId = self.db.transaction_NextId
                    self.db.transaction_NextId += 1
                    
                    result["transactionId"] = self.db.transaction_CurrentId as AnyObject?
                    result["error"] = NSNull()
                } catch {
                    result["transactionId"] = NSNull()
                    result["error"] = "\(error)" as AnyObject?
                }
                
                return result
            }))
            .addNative("Query_Execute", ABNativeAction(callFn: { (args: [String: AnyObject]) in
                var result = [String: AnyObject]()
                
                do {
                    guard let query = args["query"] as? String else {
                        throw ABNativeActionError.cannotParseJSON
                    }
                    
                    try self.db.query_Execute(query)

                    result["error"] = NSNull()
                } catch {
                    result["error"] = "\(error)" as AnyObject?
                }
                
                return result
            }))
            .addNative("Query_Select", ABNativeAction(callFn: { (args: [String: AnyObject]) in
                var result = [String: AnyObject]()
                
                do {
                    guard let query = args["query"] as? String,
                          let columnTypes = args["columnTypes"] as? [String] else {
                        throw ABNativeActionError.cannotParseJSON
                    }
                    
                    var columnTypes_DB = [ABSQLiteDatabase.ColumnType]()
                    for columnType in columnTypes {
                        if columnType == "Bool" {
                            columnTypes_DB.append(.int)
                        } else if columnType == "Float" {
                            columnTypes_DB.append(.double)
                        } else if columnType == "Id" {
                            columnTypes_DB.append(.text)
                        } else if columnType == "Int" {
                            columnTypes_DB.append(.int)
                        } else if columnType == "Long" {
                            columnTypes_DB.append(.text)
                        } else if columnType == "String" {
                            columnTypes_DB.append(.text)
                        } else if columnType == "Time" {
                            columnTypes_DB.append(.text)
                        } else {
                            throw ABDatabaseError.unknownColumnType(columnType)
                        }
                    }
                    
                    let rows_DB = try self.db.query_Select(query, columnTypes_DB)
                    var rows = [AnyObject]()
                    for row_DB in rows_DB {
                        var row = [AnyObject]()
                        for i in 0..<row_DB.count {
                            if (row_DB[i] is NSNull) {
                                row.append(NSNull())
                            } else if (columnTypes[i] == "Bool") {
                                row.append(Bool((row_DB[i] as? Int32) ?? 0 > 0) as AnyObject)
                            } else if (columnTypes[i] == "Float") {
                                row.append((row_DB[i] as? Double) as AnyObject)
                            } else if (columnTypes[i] == "Id") {
                                row.append(Int64((row_DB[i] as? String) ?? "0") as AnyObject)
                            } else if (columnTypes[i] == "Int") {
                                row.append((row_DB[i] as? Int32) as AnyObject)
                            } else if (columnTypes[i] == "Long") {
                                row.append(Int64((row_DB[i] as? String) ?? "0") as AnyObject)
                            } else if (columnTypes[i] == "String") {
                                row.append((row_DB[i] as? String) as AnyObject)
                            } else if (columnTypes[i] == "Time") {
                                row.append(Int64((row_DB[i] as? String) ?? "0") as AnyObject)
                            } else {
                                throw ABDatabaseError.unknownColumnType(columnTypes[i])
                            }
                        }
                        rows.append(row as AnyObject)
                    }
                    
                    result["rows"] = rows as AnyObject?
                    result["error"] = NSNull()
                } catch {
                    result["rows"] = NSNull() as AnyObject?
                    result["error"] = "\(error)" as AnyObject?
                }
                
                return result
            }))
    }
    
    
    public func error(_ message: String) {
        print("ABDatabase Error -> \(message)")
    }
    
}


public enum ABDatabaseError: Error {
    case unknownColumnType(_ columnType: String)
}
