package db

import (
	"GO-Redis/data"
	"context"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

func setString(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) data.RedisData {
	cmdKey := strings.ToLower(string(cmd[1]))
	if len(cmd) < 3 {
		return data.MakeErrorData("error: commands is invalid")
	}

	// check option params
	var err error
	var nx, xx, get, ex, px, keepttl, exat bool
	var exval, exatval int64
	var millisecPx int64

	// parse flags
	for i := 3; i < len(cmd); i++ {
		switch strings.ToLower(string(cmd[i])) {
		case "nx": // The operation sets the given key-value pair only if the key does not exist
			nx = true
		case "xx": // The operation sets the given key-value pair only if the key already exists
			xx = true
		case "get": // Returns the old value of this key when the operation is complete
			get = true
		case "keepttl": // Keep the original expiration time (TTL) unchanged when setting a new value
			keepttl = true
		case "exat": // Expires the specified key as a Unix timestamp.
			exat = true
			i++
			if i >= len(cmd) {
				return data.MakeErrorData("error: commands is invalid")
			}
			exatval, err = strconv.ParseInt(string(cmd[1]), 10, 64)
			if err != nil {
				return data.MakeErrorData("ERROR value is not integer or out of range")
			}
		case "ex": // Will specify the expiration time of the key in seconds
			ex = true
			i++
			if i >= len(cmd) {
				return data.MakeErrorData("error: commands is invalid")
			}
			exval, err = strconv.ParseInt(string(cmd[i]), 10, 64)
			if err != nil {
				return data.MakeErrorData("ERR value is not integer or out of range")
			}
		case "px":
			px = true
			i++
			millisecPx, err = strconv.ParseInt(string(cmd[i]), 10, 64)
			if err != nil {
				return data.MakeErrorData("ERROR value is not integer or out of range")
			}
		default:
			return data.MakeErrorData("Error unsupported option: " + string(cmd[i]))
		}
	}

	if (nx && xx) || (ex && keepttl) || (ex && px) || (ex && exat) || (px && exat) {
		return data.MakeErrorData("error: commands is invalid")
	}

	// will very likely change value, lock the db methods for atomic manipulations
	db.locks.Lock(cmdKey)
	defer db.locks.UnLock(cmdKey)

	// check old value stored in key
	var res data.RedisData
	oldVal, oldOK := db.db.Get(cmdKey)

	// check if the value is string
	var oldTypeVal []byte
	var typeOK bool
	if oldOK {
		oldTypeVal, typeOK = oldVal.([]byte)
		if !typeOK {
			return data.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
	}

	// set key and check if it satisfies nx or xx condition
	// return the set result if the get command is not given
	if nx || xx {
		if nx {
			if !oldOK {
				db.db.Set(cmdKey, cmd[2])
			} else {
				res = data.MakeBulkData(nil)
			}
		} else {
			if oldOK {
				db.db.Set(cmdKey, cmd[2])
				res = data.MakeStringData("OK")
			} else {
				res = data.MakeBulkData(nil)
			}
		}
	} else {
		db.db.Set(cmdKey, cmd[2])
		res = data.MakeStringData("OK")
	}

	// If a get command offered, return GET result
	if get {
		if !oldOK {
			res = data.MakeBulkData(nil)
		} else {
			res = data.MakeBulkData(oldTypeVal)
		}
	}

	// delete old ttl if it is not preserved
	if !keepttl {
		db.DeleteTTL(cmdKey)
	}

	// set ttl
	if ex {
		db.SetTTL(cmdKey, time.Now().Unix()+exval)
	}
	if px {
		db.SetTTL(cmdKey, time.Now().Unix()+millisecPx/1000)
	}
	if exat {
		db.SetTTL(cmdKey, exatval)
	}

	return res
}

// getString 获取key值
func getString(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) data.RedisData {
	if strings.ToLower(string(cmd[0])) != "get" {
		log.Printf("getString func: cmdName != get")
		return data.MakeErrorData("Server Error")
	}

	if len(cmd) != 2 {
		return data.MakeErrorData("error: commands is invalid")
	}

	key := string(cmd[1])

	db.locks.RLock(key)
	defer db.locks.RUnLock(key)

	val, ok := db.db.Get(key)
	if !ok {
		return data.MakeBulkData(nil)
	}

	byteVal, ok := val.([]byte)
	if !ok {
		return data.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	return data.MakeBulkData(byteVal)
}

// getRangeString Get the substring of the string stored in the specified key.
// The intercept range of the string is determined by the start and end offsets (including start and end).
func getRangeString(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) data.RedisData {
	if strings.ToLower(string(cmd[0])) != "getrange" {
		log.Printf("getRangeString func: cmdName != getrange")
		return data.MakeErrorData("Server error")
	}

	if len(cmd) != 4 {
		return data.MakeErrorData("error: commands is invalid")
	}

	key := string(cmd[1])

	db.locks.RLock(key)
	defer db.locks.RUnLock(key)

	val, ok := db.db.Get(key)
	if !ok {
		return data.MakeBulkData(nil)
	}

	// Determine if val is of type []byte
	byteVal, ok := val.([]byte)
	if !ok {
		return data.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	start, err := strconv.Atoi(string(cmd[2]))
	if err != nil {
		return data.MakeErrorData("error: commands is invalid")
	}

	end, err := strconv.Atoi(string(cmd[3]))
	if err != nil {
		return data.MakeErrorData("error: commands is invalid")
	}

	if start < 0 {
		start = len(byteVal) + start
	}

	if end < 0 {
		end = len(byteVal) + end
	}
	end = end + 1

	if start > end || start >= len(byteVal) || end < 0 {
		return data.MakeErrorData("error: commands is invalid")
	}

	if start < 0 {
		start = 0
	}

	return data.MakeBulkData(byteVal[start:end])
}

// setRangeString Overwrites the value of the string stored by the given key with the specified string, starting at offset
func setRangeString(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) data.RedisData {
	if strings.ToLower(string(cmd[0])) != "setrange" {
		log.Printf("setRangeString func: cmdName != setrange")
		return data.MakeErrorData("Server Error")
	}

	if len(cmd) != 4 {
		return data.MakeErrorData("error: commands is invalid")
	}

	offset, err := strconv.Atoi(string(cmd[2]))
	if err != nil || offset < 0 {
		return data.MakeErrorData("error: offset is not a integer or less than 0")
	}

	key := string(cmd[1])

	db.locks.Lock(key)
	defer db.locks.UnLock(key)

	var oldVal []byte
	var newVal []byte

	val, ok := db.db.Get(key)
	if !ok {
		oldVal = make([]byte, 0)
	} else {
		oldVal, ok = val.([]byte)
		if !ok {
			return data.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
	}

	if offset > len(oldVal) {
		newVal = oldVal
		for i := 0; i < offset-len(oldVal); i++ {
			newVal = append(newVal, byte(0))
		}
		newVal = append(newVal, cmd[3]...)
	} else {
		newVal = oldVal[:offset]
		newVal = append(newVal, cmd[3]...)
	}

	db.db.Set(key, newVal)

	return data.MakeIntData(int64(len(newVal)))
}

// mGetString 返回所有一个或多个给定key的值
func mGetString(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) data.RedisData {
	if strings.ToLower(string(cmd[0])) != "mget" {
		log.Printf("mGetString func: cmdName != mget")
		return data.MakeErrorData("Server Error")
	}

	if len(cmd) < 2 {
		return data.MakeErrorData("error: commands is invalid")
	}

	res := make([]data.RedisData, 0)

	for i := 1; i < len(cmd); i++ {
		key := string(cmd[i])

		db.locks.RLock(key)
		val, ok := db.db.Get(key)
		db.locks.RUnLock(key)

		if !ok {
			res = append(res, data.MakeBulkData(nil))
		} else {
			byteVal, ok := val.([]byte)
			if !ok {
				res = append(res, data.MakeBulkData(nil))
			} else {
				res = append(res, data.MakeBulkData(byteVal))
			}
		}
	}

	return data.MakeArrayData(res)
}

// mSetString Setting one or more key-value pairs at the same time
func mSetString(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) data.RedisData {
	if strings.ToLower(string(cmd[0])) != "mset" {
		log.Printf("mGetString func: cmdName != mset")
		return data.MakeErrorData("Server Error")
	}

	if len(cmd) < 3 || len(cmd)&1 != 1 {
		return data.MakeErrorData("error: commands is invalid")
	}

	keys := make([]string, 0)
	vals := make([][]byte, 0)

	for i := 1; i < len(cmd); i += 2 {
		keys = append(keys, string(cmd[i]))
		vals = append(vals, cmd[i+1])
	}

	// lock all keys for atomicity (降低并发性)
	db.locks.LockMulti(keys)
	defer db.locks.UnLockMulti(keys)

	for i := 0; i < len(keys); i++ {
		db.DeleteTTL(keys[i])
		db.db.Set(keys[i], vals[i])
	}

	return data.MakeStringData("OK")
}

// setExString 为指定的key设置值及其过期时间，如果key已经存在，SETEX命令将会替换旧的值
func setExString(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) data.RedisData {
	if strings.ToLower(string(cmd[0])) != "setex" {
		log.Printf("setExString func: cmdName != setex")
		return data.MakeErrorData("Server Error")
	}

	if len(cmd) != 4 {
		return data.MakeErrorData("error: commands is invalid")
	}

	ex, err := strconv.ParseInt(string(cmd[2]), 10, 64)
	if err != nil {
		return data.MakeErrorData("error: commands is invalid")
	}

	ttl := time.Now().Unix() + ex
	key := string(cmd[1])
	val := cmd[3]

	db.locks.Lock(key)
	defer db.locks.UnLock(key)

	db.db.Set(key, val)
	db.SetTTL(key, ttl)

	return data.MakeStringData("OK")
}

// setNxString 在指定的key不存在时，为key设置指定的值
func setNxString(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) data.RedisData {
	if strings.ToLower(string(cmd[0])) != "setnx" {
		log.Printf("setNxString func: cmdName != setnx")
		return data.MakeErrorData("Server Error")
	}

	if len(cmd) != 3 {
		return data.MakeErrorData("error: commands is invalid")
	}

	key := string(cmd[1])
	val := cmd[2]

	db.locks.Lock(key)
	defer db.locks.UnLock(key)

	res := db.db.SetIfNotExist(key, val)

	return data.MakeIntData(int64(res))
}

// Gets the length of the string value stored under the specified key
func strLenString(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) data.RedisData {
	if strings.ToLower(string(cmd[0])) != "strlen" {
		log.Printf("strLenString func: cmdName != strlen")
		return data.MakeErrorData("Server Error")
	}

	if len(cmd) != 2 {
		return data.MakeErrorData("error: commands is invalid")
	}

	key := string(cmd[1])

	db.locks.RLock(key)
	defer db.locks.RUnLock(key)

	val, ok := db.db.Get(key)
	if !ok {
		return data.MakeIntData(0)
	}

	typeVal, ok := val.([]byte)
	if !ok {
		return data.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	return data.MakeIntData(int64(len(typeVal)))
}

// incrString Increments the value stored under the specified key by 1 and returns the incremented value.
// If the key does not exist, it creates the key and sets its value to 1
func incrString(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) data.RedisData {
	if strings.ToLower(string(cmd[0])) != "incr" {
		log.Printf("incrString func: cmdName != incr")
		return data.MakeErrorData("Server Error")
	}

	if len(cmd) != 2 {
		return data.MakeErrorData("error: commands is invalid")
	}

	key := string(cmd[1])

	db.locks.Lock(key)
	defer db.locks.UnLock(key)

	val, ok := db.db.Get(key)
	if !ok {
		db.db.Set(key, []byte("1"))
		return data.MakeIntData(1)
	}

	typeVal, ok := val.([]byte)
	if !ok {
		return data.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	intVal, err := strconv.ParseInt(string(typeVal), 10, 64)
	if err != nil {
		return data.MakeErrorData("value is not an integer")
	}

	intVal++

	db.db.Set(key, []byte(strconv.FormatInt(intVal, 10)))

	return data.MakeIntData(intVal)
}

// incrByString Adds the number stored in key to the specified incremental value
// If the key does not exist, the value of the key is initialized to 0 before executing the INCRBY command
func incrByString(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) data.RedisData {
	if strings.ToLower(string(cmd[0])) != "incrby" {
		log.Printf("incrByString func: cmdName != incrby")
		return data.MakeErrorData("Server Error")
	}

	if len(cmd) != 3 {
		return data.MakeErrorData("error: commands is invalid")
	}

	key := string(cmd[1])

	incr, err := strconv.ParseInt(string(cmd[2]), 10, 64)
	if err != nil {
		return data.MakeErrorData("commands invalid: increment value is not an integer")
	}

	db.locks.Lock(key)
	defer db.locks.UnLock(key)

	val, ok := db.db.Get(key)
	if !ok {
		db.db.Set(key, []byte(strconv.FormatInt(incr, 10)))
		return data.MakeIntData(incr)
	}

	typeVal, ok := val.([]byte)
	if !ok {
		return data.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	intVal, err := strconv.ParseInt(string(typeVal), 10, 64)
	if err != nil {
		return data.MakeErrorData("value is not an integer")
	}

	intVal += incr

	db.db.Set(key, []byte(strconv.FormatInt(intVal, 10)))

	return data.MakeIntData(intVal)
}

// decrString Subtracts the number stored in key by the specified incremental value
// If the key does not exist, the value of the key is initialized to 0 before the DECRBY command is executed
func decrString(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) data.RedisData {
	if strings.ToLower(string(cmd[0])) != "decr" {
		log.Printf("decrString func: cmdName != decr")
		return data.MakeErrorData("Server Error")
	}

	if len(cmd) != 2 {
		return data.MakeErrorData("error: commands is invalid")
	}

	key := string(cmd[1])

	db.locks.Lock(key)
	defer db.locks.UnLock(key)

	val, ok := db.db.Get(key)
	if !ok {
		db.db.Set(key, []byte("-1"))
		return data.MakeIntData(-1)
	}

	typeVal, ok := val.([]byte)
	if !ok {
		return data.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	intVal, err := strconv.ParseInt(string(typeVal), 10, 64)
	if err != nil {
		return data.MakeErrorData("value is not an integer")
	}

	intVal--

	db.db.Set(key, []byte(strconv.FormatInt(intVal, 10)))

	return data.MakeIntData(intVal)
}

// decrByString 将key所储存的值减去指定的减量值
// 如果key不存在，那么key的值会被先初始化为0，然后再执行decrby操作
func decrByString(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) data.RedisData {
	if strings.ToLower(string(cmd[0])) != "decrby" {
		log.Printf("decrByString func: cmdName != decrby")
		return data.MakeErrorData("Server Error")
	}

	if len(cmd) != 3 {
		return data.MakeErrorData("error: commands is invalid")
	}

	key := string(cmd[1])

	dec, err := strconv.ParseInt(string(cmd[2]), 10, 64)
	if err != nil {
		return data.MakeErrorData("commands invalid: increment value is not an integer")
	}

	db.locks.Lock(key)
	defer db.locks.UnLock(key)

	val, ok := db.db.Get(key)
	if !ok {
		db.db.Set(key, []byte(strconv.FormatInt(-dec, 10)))
		return data.MakeIntData(-dec)
	}

	typeVal, ok := val.([]byte)
	if !ok {
		return data.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	intVal, err := strconv.ParseInt(string(typeVal), 10, 64)
	if err != nil {
		return data.MakeErrorData("value is not an integer")
	}

	intVal -= dec

	db.db.Set(key, []byte(strconv.FormatInt(intVal, 10)))

	return data.MakeIntData(intVal)
}

func incrByFloatString(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) data.RedisData {
	if strings.ToLower(string(cmd[0])) != "incrbyfloat" {
		log.Printf("incrByFloatString func: cmdName != incrbyfloat")
		return data.MakeErrorData("Server Error")
	}

	if len(cmd) != 3 {
		return data.MakeErrorData("error: commands is invalid")
	}

	key := string(cmd[1])

	inc, err := strconv.ParseFloat(string(cmd[2]), 64)
	if err != nil {
		return data.MakeErrorData("commands invalid: increment value is not an float")
	}

	db.locks.Lock(key)
	defer db.locks.UnLock(key)

	val, ok := db.db.Get(key)
	if !ok {
		db.db.Set(key, []byte(strconv.FormatFloat(inc, 'f', -1, 64)))
		return data.MakeBulkData([]byte(strconv.FormatFloat(inc, 'f', -1, 64)))
	}

	typeVal, ok := val.([]byte)
	if !ok {
		return data.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	floatVal, err := strconv.ParseFloat(string(typeVal), 64)
	if err != nil {
		return data.MakeErrorData("value is not an float")
	}

	floatVal += inc

	db.db.Set(key, []byte(strconv.FormatFloat(floatVal, 'f', -1, 64)))

	return data.MakeBulkData([]byte(strconv.FormatFloat(floatVal, 'f', -1, 64)))
}

func appendString(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) data.RedisData {
	if strings.ToLower(string(cmd[0])) != "append" {
		log.Printf("appendString func: cmdName != append")
		return data.MakeErrorData("Server Error")
	}

	if len(cmd) != 3 {
		return data.MakeErrorData("error: commands is invalid")
	}

	key := string(cmd[1])
	val := cmd[2]

	db.locks.Lock(key)
	defer db.locks.UnLock(key)

	oldVal, ok := db.db.Get(key)
	if !ok {
		db.db.Set(key, val)
		return data.MakeIntData(int64(len(val)))
	}

	typeVal, ok := oldVal.([]byte)
	if !ok {
		return data.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	newVal := append(typeVal, val...)
	db.db.Set(key, newVal)

	return data.MakeIntData(int64(len(newVal)))
}

func RegisterStringCommands() {
	RegisterCommand("set", setString)
	RegisterCommand("get", getString)
	RegisterCommand("getrange", getRangeString)
	RegisterCommand("setrange", setRangeString)
	RegisterCommand("mget", mGetString)
	RegisterCommand("mset", mSetString)
	RegisterCommand("setex", setExString)
	RegisterCommand("setnx", setNxString)
	RegisterCommand("strlen", strLenString)
	RegisterCommand("incr", incrString)
	RegisterCommand("incrby", incrByString)
	RegisterCommand("decr", decrString)
	RegisterCommand("decrby", decrByString)
	RegisterCommand("incrbyfloat", incrByFloatString)
	RegisterCommand("append", appendString)
}
