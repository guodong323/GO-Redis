package db

import (
	"GO-Redis/Data"
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

// implements the keys commands of redis

func RegisterKeyCommands() {
	RegisterCommand("ping", pingKeys)
	RegisterCommand("del", deleteKey)
	RegisterCommand("exists", existsKey)
	RegisterCommand("keys", keysKey)
	RegisterCommand("expire", expireKey)
	RegisterCommand("persist", persistKey)
	RegisterCommand("ttl", ttlKey)
	//RegisterCommand("type", typeKey)
	RegisterCommand("rename", renameKey)
}

func deleteKey(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) Data.RedisData {
	cmdName := string(cmd[0])
	if strings.ToLower(cmdName) != "delete" {
		log.Printf("deleteKey Function: cmdName is not delete")
		return Data.MakeErrorData("protocol Error: cmdName is not delete")
	}
	// if already expires
	if db.CheckTTL(string(cmd[1])) {
		return Data.MakeIntData(int64(0))
	}
	// record the number of delete keys
	count := 0
	for _, key := range cmd[1:] {
		cur := string(key)
		db.locks.Lock(cur)
		ok := db.db.Delete(cur)
		if ok {
			count += 1
		}
		db.ttlKeys.Delete(cur)
		db.locks.UnLock(cur)
	}
	return Data.MakeIntData(int64(count))
}

func existsKey(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) Data.RedisData {
	cmdName := string(cmd[0])
	if strings.ToLower(cmdName) != "exists" || len(cmd) < 2 {
		log.Println("existsKey Function: cmdName is not exists or command args number is invalid")
		return Data.MakeErrorData("Protocol error: cmdName is not exists")
	}
	count := 0
	var key string
	for _, keyByte := range cmd[1:] {
		key = string(keyByte)
		if db.CheckTTL(key) {
			db.locks.RLock(key)
			if _, OK := db.db.Get(key); OK {
				count++
			}
			db.locks.RUnLock(key)
		}
	}
	return Data.MakeIntData(int64(count))
}

func keysKey(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) Data.RedisData {
	if strings.ToLower(string(cmd[0])) != "keys" || len(cmd) != 2 {
		log.Printf("keysKey Function: cmdName is not keys or cmd length is not 2")
		return Data.MakeErrorData(fmt.Sprintf("error: keys function get invalid command %s %s", string(cmd[0]), string(cmd[1])))
	}
	res := make([]Data.RedisData, 0)
	allKeys := db.db.Keys()
	pattern := string(cmd[1])
	for _, key := range allKeys {
		if db.CheckTTL(key) {
			if PattenMatch(pattern, key) {
				res = append(res, Data.MakeBulkData([]byte(key)))
			}
		}
	}
	return Data.MakeArrayData(res)
}

// expireKey 续约等操作的实现
func expireKey(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) Data.RedisData {
	cmdName := string(cmd[0])
	if strings.ToLower(cmdName) != "expire" || len(cmd) < 3 || len(cmd) > 4 {
		log.Printf("expireKey Function: cmdName is not expire or command args number is invalid")
		return Data.MakeErrorData("error: cmdName is not expire or command args number is invalid")
	}

	value, err := strconv.ParseInt(string(cmd[2]), 10, 64)
	if err != nil {
		log.Printf("expireKey Function: cmd[2] %s is not int", string(cmd[2]))
		return Data.MakeErrorData(fmt.Sprintf("error: %s is not int", string(cmd[2])))
	}
	ttl := time.Now().Unix() + value
	var op string
	if len(cmd) == 4 {
		op = strings.ToLower(string(cmd[3]))
	}
	key := string(cmd[1])
	if !db.CheckTTL(key) {
		return Data.MakeIntData(int64(0))
	}
	db.locks.Lock(key)
	defer db.locks.UnLock(key)

	var res bool
	switch op {
	case "nx":
		if _, OK := db.ttlKeys.Get(key); !OK {
			res = db.SetTTL(key, ttl)
		}
	case "xx":
		if _, OK := db.ttlKeys.Get(key); !OK {
			res = db.SetTTL(key, ttl)
		}
	case "gt":
		if v, OK := db.ttlKeys.Get(key); OK && ttl > v.(*TTLInfo).value {
			res = db.SetTTL(key, ttl)
		}
	case "lt":
		if v, OK := db.ttlKeys.Get(key); OK && ttl < v.(*TTLInfo).value {
			res = db.SetTTL(key, ttl)
		}
	default:
		if op != "" {
			log.Printf("expireKey Function: opt %s is not nx, xx, gt or lt", op)
			return Data.MakeErrorData(fmt.Sprintf(fmt.Sprintf("ERROR Unsupported option %s, except nx, xx, gt, lt", op)))
		}
		res = db.SetTTL(key, ttl)
	}
	var data int
	if res {
		data = 1
	} else {
		data = 0
	}
	return Data.MakeIntData(int64(data))
}

// persisKey 删除kv的ttl，而不删除kv，从而使得其永久化
func persistKey(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) Data.RedisData {
	cmdName := string(cmd[0])
	if strings.ToLower(cmdName) != "persist" || len(cmd) != 2 {
		log.Printf("persistKey Function: cmdName is not persist or command args number is invalid")
		return Data.MakeErrorData("error: cmdName is not persist or command args number is invalid")
	}
	key := string(cmd[1])
	if !db.CheckTTL(key) {
		return Data.MakeIntData(int64(0))
	}
	db.locks.Lock(key)
	defer db.locks.UnLock(key)

	res := db.DeleteTTL(key)
	var data int
	if res {
		data = 1
	} else {
		data = 0
	}
	return Data.MakeIntData(int64(data))
}

// ttlKey 获取指定键的剩余生存时间（TTL）的 "ttl" 命令
func ttlKey(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) Data.RedisData {
	cmdName := string(cmd[0])
	if strings.ToLower(cmdName) != "ttl" || len(cmd) != 2 {
		log.Printf("ttlKey Function: cmdName is not ttl or command args number is invalid")
		return Data.MakeErrorData("error: cmdName is not ttl or command args number is invalid")
	}
	key := string(cmd[1])
	if !db.CheckTTL(key) {
		return Data.MakeIntData(int64(0))
	}
	db.locks.RLock(key)
	defer db.locks.RUnLock(key)

	if _, OK := db.db.Get(key); !OK {
		return Data.MakeIntData(int64(-2))
	}
	now := time.Now().Unix()
	ttl, OK := db.ttlKeys.Get(key)
	if !OK {
		return Data.MakeIntData(int64(-1))
	}
	var res int64
	res = ttl.(*TTLInfo).value - now
	return Data.MakeIntData(res)
}

//func typeKey(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) Data.RedisData {
//	cmdName := string(cmd[0])
//	if strings.ToLower(cmdName) != "type" || len(cmd) != 2 {
//		log.Printf("typeKey Function: cmdName is not type or command args number is invalid")
//		return Data.MakeErrorData("error: cmdName is not type or command args number is invalid")
//	}
//	key := string(cmd[1])
//	if !db.CheckTTL(key) {
//		return Data.MakeBulkData([]byte("none"))
//	}
//	db.locks.RLock(key)
//	defer db.locks.RUnLock(key)
//	v, OK := db.db.Get(key)
//	if !OK {
//		return Data.MakeStringData("none")
//	}
//	//
//
//	return Data.MakeErrorData("unknown error: server error")
//}

func renameKey(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) Data.RedisData {
	cmdName := string(cmd[0])
	if strings.ToLower(cmdName) != "rename" || len(cmd) != 3 {
		log.Printf("renameKey Function: cmdName is not rename or command args number is invalid")
		return Data.MakeErrorData("error: cmdName is not rename or command args number is invalid")
	}
	oldName, newName := string(cmd[1]), string(cmd[2])
	if db.CheckTTL(oldName) {
		return Data.MakeErrorData(fmt.Sprintf("error: %s not exist", oldName))
	}
	db.locks.LockMulti([]string{oldName, newName})
	defer db.locks.UnLockMulti([]string{oldName, newName})

	oldValue, OK := db.db.Get(oldName)
	if !OK {
		return Data.MakeErrorData(fmt.Sprintf("error: %s not exist", oldName))
	}
	oldTTL, OK := db.ttlKeys.Get(oldName)
	if !OK {
		return Data.MakeErrorData(fmt.Sprintf("error: %s not exist", oldTTL))
	}
	// 先删除老的
	db.db.Delete(oldName)
	db.ttlKeys.Delete(oldName)

	// 再删除新的
	db.db.Delete(newName)
	db.ttlKeys.Delete(newName)

	// 再设置新的
	db.db.Set(newName, oldValue)
	db.ttlKeys.Set(newName, oldTTL)

	return Data.MakeStringData("OK")
}

func pingKeys(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) Data.RedisData {
	if len(cmd) > 2 {
		return Data.MakeErrorData("error: wrong number of arguments for 'ping' command")
	}
	// default reply
	if len(cmd) == 1 {
		return Data.MakeStringData("PONG")
	}
	return Data.MakeBulkData(cmd[1])
}

// PattenMatch matches a string with a wildcard pattern.
// It supports following cases:
// - h?llo matches hello, hallo and hxllo
// - h*llo matches hllo and heeeello
// - h[ae]llo matches hello and hallo, but not hillo
// - h[^e]llo matches hallo, hbllo, ... but not hello
// - h[a-b]llo matches hallo and hbllo
// - Use \ to escape special characters if you want to match them verbatim.
func PattenMatch(pattern, src string) bool {
	patLen, srcLen := len(pattern), len(src)

	if patLen == 0 {
		return srcLen == 0
	}

	if srcLen == 0 {
		for i := 0; i < patLen; i++ {
			if pattern[i] != '*' {
				return false
			}
		}
		return true
	}

	patPos, srcPos := 0, 0
	for patPos < patLen {
		switch pattern[patPos] {
		case '*':
			for patPos < patLen && pattern[patPos] == '*' {
				patPos++
			}
			if patPos == patLen {
				return true
			}
			for srcPos < srcLen {
				for srcPos < srcLen && src[srcPos] != pattern[patPos] {
					srcPos++
				}
				if PattenMatch(pattern[patPos+1:], src[srcPos+1:]) {
					return true
				} else {
					srcPos++
				}
			}
			return false
		case '?':
			srcPos++
			break
		case '[':
			var not, match, closePat bool
			patPos++
			// '[' must match a ']' character, otherwise it's wrong pattern and return false
			if patPos == patLen {
				return false
			}
			if pattern[patPos] == '^' {
				not = true
				patPos++
			}
			for patPos < patLen {
				if pattern[patPos] == '\\' {
					patPos++
					if patPos == patLen { // pattern syntax error
						return false
					}
					if pattern[patPos] == src[srcPos] {
						match = true
					}
				} else if pattern[patPos] == ']' {
					closePat = true
					break
				} else if pattern[patPos] == '-' {
					if patPos+1 == patLen || pattern[patPos+1] == ']' { //  wrong pattern syntax
						return false
					}
					start, end := pattern[patPos-1], pattern[patPos+1]
					if src[srcPos] >= start && src[srcPos] <= end {
						match = true
					}
					patPos++
				} else {
					if pattern[patPos] == src[srcPos] {
						match = true
					}
				}
				patPos++
			}
			if !closePat {
				return false
			}
			if not {
				match = !match
			}
			if !match {
				return false
			}
			srcPos++
			break
		case '\\':
			//	escape special character in pattern and fall through to default to handle
			if patPos+1 < patLen {
				patPos++
			} else {
				return false
			}
			//	fall into default
		default:
			if pattern[patPos] != src[srcPos] {
				return false
			}
			srcPos++
			break
		}
		patPos++
		// When src has been consumed, pattern must be consumed to the end or only contains '*' in last
		if srcPos >= srcLen {
			for patPos < patLen && pattern[patPos] == '*' {
				patPos++
			}
			break
		}
	}
	return patPos == patLen && srcPos == srcLen
}
