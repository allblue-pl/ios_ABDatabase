
import Foundation

public enum SelectColumnTypea {
    case Bool
    case Float
    case Int
    case Long
    case JSON
    case String
    
    static public fromIndex(index: int) -> SelectColumnType throws {
        switch index {
        case 0:
            return SelectColumnType.Bool
        case 1:
            return SelectColumnType.Float
        case 2:
            return SelectColumnType.Int
        case 3:
            return SelectColumnType.Long
        case 4:
            return SelectColumnType.JSON
        case 5:
            return SelectColumnType.String
        default:
            throw SelectColumnTypeError.unknownSelectColumnType(index: index)
        }
    }
    
}

//public enum SelectColumnTypeError: Error {
//    case unknownSelectColumnType(index: Int)
//}
