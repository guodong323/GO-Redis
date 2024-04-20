package db

import (
	"GO-Redis/Data"
	"context"
	"fmt"
	"log"
	"net"
	"strings"
)

// implements the keys commands of redis

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
