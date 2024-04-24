package db

import (
	"GO-Redis/data"
	"bytes"
	"context"
	"fmt"
	"log"
	"math"
	"net"
	"strconv"
	"strings"
	"time"
)

func lLenList(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) data.RedisData {
	if strings.ToLower(string(cmd[0])) != "llen" {
		log.Printf("lLenList Function: cmdName is not llen")
		return data.MakeErrorData("server error")
	}

	if len(cmd) != 2 {
		return data.MakeErrorData("wrong number of arguments for 'llen' command")
	}

	key := string(cmd[1])

	db.locks.RLock(key)
	defer db.locks.RUnLock(key)

	val, ok := db.db.Get(key)
	if !ok {
		return data.MakeIntData(0)
	}

	typeVal, ok := val.(*List)
	if !ok {
		return data.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	len := int64(typeVal.Len)

	return data.MakeIntData(len)
}

func lIndexList(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) data.RedisData {
	if strings.ToLower(string(cmd[0])) != "lindex" {
		log.Printf("lIndexList Function: cmdName is not lindex")
		return data.MakeErrorData("server error")
	}

	if len(cmd) != 3 {
		return data.MakeErrorData("wrong number of arguments for 'lindex' command")
	}

	key := string(cmd[1])
	index, err := strconv.Atoi(string(cmd[2]))
	if err != nil {
		return data.MakeErrorData("index is not an integer")
	}

	if !db.CheckTTL(key) {
		return data.MakeBulkData(nil)
	}

	db.locks.RLock(key)
	defer db.locks.RUnLock(key)

	val, ok := db.db.Get(key)
	if !ok {
		return data.MakeBulkData(nil)
	}

	typeVal, ok := val.(*List)
	if !ok {
		return data.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	resNode := typeVal.Index(index)

	if resNode == nil {
		return data.MakeBulkData(nil)
	}

	return data.MakeBulkData(resNode.Val)
}

func lPosList(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) data.RedisData {
	if strings.ToLower(string(cmd[0])) != "lpos" {
		log.Printf("lPosList Function: cmdName is not lpos")
		return data.MakeErrorData("server error")
	}

	if len(cmd) < 3 || len(cmd)&1 != 1 {
		return data.MakeErrorData("wrong number of arguments for 'lpos' command")
	}

	var rank, count, maxLen, reverse bool
	var rankVal, countVal, maxLenVal int
	var key string
	var elem []byte
	var err error

	var pos int
	elem = cmd[2]

	// handle params
	for i := 3; i < len(cmd); i += 2 {
		switch strings.ToLower(string(cmd[i])) {
		case "rank":
			rank = true
			rankVal, err = strconv.Atoi(string(cmd[i+1]))
			if err != nil || rankVal == 0 {
				return data.MakeErrorData("rank value should 1,2,3... or -1,-2,-3...")
			}
		case "count":
			count = true
			countVal, err = strconv.Atoi(string(cmd[i+1]))
			if err != nil || countVal < 0 {
				return data.MakeErrorData("count value is not an positive integer")
			}
		case "maxlen":
			maxLen = true
			maxLenVal, err = strconv.Atoi(string(cmd[i+1]))
			if err != nil || maxLenVal < 0 {
				return data.MakeErrorData("maxlen value is not an positive integer")
			}
		default:
			return data.MakeErrorData(fmt.Sprintf("unsupported option %s", string(cmd[i])))
		}
	}

	if !db.CheckTTL(key) {
		return data.MakeBulkData(nil)
	}

	db.locks.RLock(key)
	defer db.locks.RUnLock(key)

	tem, ok := db.db.Get(key)
	if !ok {
		return data.MakeBulkData(nil)
	}

	list, ok := tem.(*List)
	if !ok {
		return data.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	if list.Len == 0 {
		return data.MakeBulkData(nil)
	}

	if maxLen && maxLenVal == 0 {
		maxLenVal = list.Len
	}

	// normally pos without options
	if !rank && !count && !maxLen {
		pos := list.Pos(elem)
		if pos == -1 {
			return data.MakeBulkData(nil)
		} else {
			return data.MakeIntData(int64(pos))
		}
	}

	// handle options
	var now *ListNode
	if rank {
		if rankVal > 0 {
			pos = -1
			for now = list.Head.Next; now != list.Tail; now = now.Next {
				pos++
				if bytes.Equal(now.Val, elem) {
					rankVal--
				}
				if maxLen {
					maxLenVal--
					if maxLenVal == 0 {
						break
					}
				}
				if rankVal == 0 {
					break
				}
			}
		} else {
			reverse = true
			pos = list.Len
			for now = list.Tail.Prev; now != list.Head; now = now.Prev {
				pos--
				if bytes.Equal(now.Val, elem) {
					rankVal++
				}
				if maxLen {
					maxLenVal--
					if maxLenVal == 0 {
						break
					}
				}
				if rankVal == 0 {
					break
				}
			}
		}
	} else {
		now = list.Head.Next
		pos = 0
		if maxLen {
			maxLenVal--
		}
	}

	// when rank is out of range, return nil
	if (rank && rankVal != 0) || now == list.Tail || now == list.Head {
		return data.MakeBulkData(nil)
	}

	res := make([]data.RedisData, 0)
	if !count {
		// if count is not set, return first find pos inside maxLen range
		for ; now != list.Tail; now = now.Next {
			if bytes.Equal(now.Val, elem) {
				return data.MakeIntData(int64(pos))
			}
			pos++
			if maxLen {
				if maxLenVal <= 0 {
					break
				}
				maxLenVal--
			}
		}
		return data.MakeBulkData(nil)
	} else {
		if !reverse {
			for ; now != list.Tail && countVal != 0; now = now.Next {
				if bytes.Equal(now.Val, elem) {
					res = append(res, data.MakeIntData(int64(pos)))
					countVal--
				}
				pos++
				if maxLen {
					if maxLenVal <= 0 {
						break
					}
					maxLenVal--
				}
			}
		} else {
			for ; now != list.Head && countVal != 0; now = now.Prev {
				if bytes.Equal(now.Val, elem) {
					res = append(res, data.MakeIntData(int64(pos)))
					countVal--
				}
				pos--
				if maxLen {
					if maxLenVal <= 0 {
						break
					}
					maxLenVal--
				}
			}
		}
	}

	if len(res) == 0 {
		return data.MakeBulkData(nil)
	}

	return data.MakeArrayData(res)
}

func lPopList(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) data.RedisData {
	if len(cmd) != 2 && len(cmd) != 3 {
		return data.MakeErrorData("wrong number of arguments for 'lpop' command")
	}

	var cnt int
	var err error

	if len(cmd) == 3 {
		cnt, err = strconv.Atoi(string(cmd[2]))
		if err != nil || cnt <= 0 {
			return data.MakeErrorData("count value must be a positive integer")
		}
	}

	key := string(cmd[1])
	if !db.CheckTTL(key) {
		return data.MakeBulkData(nil)
	}

	db.locks.Lock(key)
	defer db.locks.UnLock(key)

	tem, ok := db.db.Get(key)
	if !ok {
		return data.MakeBulkData(nil)
	}

	list, ok := tem.(*List)
	if !ok {
		return data.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	// remove the key when list is empty
	defer func() {
		if list.Len == 0 {
			db.db.Delete(key)
			db.DeleteTTL(key)
		}
	}()

	// if cnt is not set, return first element
	if cnt == 0 {
		e := list.LPop()
		if e == nil {
			return data.MakeBulkData(e.Val)
		}
	}

	// return cnt number elements as array
	res := make([]data.RedisData, 0)
	for i := 0; i < cnt; i++ {
		e := list.LPop()
		if e == nil {
			break
		}
		res = append(res, data.MakeBulkData(e.Val))
	}

	return data.MakeArrayData(res)
}

func rPopList(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) data.RedisData {
	if strings.ToLower(string(cmd[0])) != "rpop" {
		log.Printf("rPopList: command is not rpop")
		return data.MakeErrorData("server error")
	}
	if len(cmd) != 2 && len(cmd) != 3 {
		return data.MakeErrorData("wrong number of arguments for 'rpop' command")
	}

	var cnt int
	var err error
	if len(cmd) == 3 {
		cnt, err = strconv.Atoi(string(cmd[2]))
		if err != nil || cnt <= 0 {
			return data.MakeErrorData("count value must be a positive integer")
		}
	}

	key := string(cmd[1])
	if !db.CheckTTL(key) {
		return data.MakeBulkData(nil)
	}

	db.locks.Lock(key)
	defer db.locks.UnLock(key)

	tem, ok := db.db.Get(key)
	if !ok {
		return data.MakeBulkData(nil)
	}

	list, ok := tem.(*List)
	if !ok {
		return data.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	defer func() {
		if list.Len == 0 {
			db.db.Delete(key)
			db.DeleteTTL(key)
		}
	}()

	// if cnt is not set, return last element
	if cnt == 0 {
		e := list.RPop()
		if e == nil {
			return data.MakeBulkData(nil)
		}
		return data.MakeBulkData(e.Val)
	}

	// return cnt number elements as array
	res := make([]data.RedisData, 0)
	for i := 0; i < cnt; i++ {
		e := list.RPop()
		if e == nil {
			break
		}
		res = append(res, data.MakeBulkData(e.Val))
	}

	return data.MakeArrayData(res)
}

func lPushList(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) data.RedisData {
	if strings.ToLower(string(cmd[0])) != "lpush" {
		log.Printf("lPushList Function : cmdName is not lpush")
		return data.MakeErrorData("server error")
	}
	if len(cmd) < 3 {
		return data.MakeErrorData("wrong number of arguments for 'lpush' command")
	}

	key := string(cmd[1])
	db.CheckTTL(key)

	db.locks.Lock(key)
	defer db.locks.UnLock(key)

	var list *List
	tem, ok := db.db.Get(key)

	// no such key, create a list
	if !ok {
		list = NewList()
		db.db.Set(key, list)
	} else {
		// try to assert it as a List
		list, ok = tem.(*List)
		if !ok {
			return data.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
	}

	// push args to the list
	for i := 2; i < len(cmd); i++ {
		list.LPush(cmd[i])
	}

	// return the length of the list
	return data.MakeIntData(int64(list.Len))
}

func lPushXList(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) data.RedisData {
	if strings.ToLower(string(cmd[0])) != "lpushx" {
		log.Printf("lPushXList Function: cmdName is not lpushx")
		return data.MakeErrorData("Server Error")
	}
	if len(cmd) < 3 {
		return data.MakeErrorData("wrong number of arguments for 'lpushx' command")
	}

	key := string(cmd[1])
	db.CheckTTL(key)

	db.locks.Lock(key)
	defer db.locks.UnLock(key)

	var list *List
	tem, ok := db.db.Get(key)
	if !ok {
		return data.MakeIntData(0)
	} else {
		list, ok = tem.(*List)
		if !ok {
			return data.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
	}
	for i := 2; i < len(cmd); i++ {
		list.LPush(cmd[i])
	}
	return data.MakeIntData(int64(list.Len))
}

func rPushList(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) data.RedisData {
	if strings.ToLower(string(cmd[0])) != "rpush" {
		log.Printf("rPushList Function: cmdName is not rpush")
		return data.MakeErrorData("server error")
	}
	if len(cmd) < 3 {
		return data.MakeErrorData("wrong number of arguments for 'rpush' command")
	}

	key := string(cmd[1])
	db.CheckTTL(key)

	db.locks.Lock(key)
	defer db.locks.UnLock(key)

	var list *List
	tem, ok := db.db.Get(key)
	if !ok {
		list = NewList()
		db.db.Set(key, list)
	} else {
		list, ok = tem.(*List)
		if !ok {
			return data.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
	}
	for i := 2; i < len(cmd); i++ {
		list.RPush(cmd[i])
	}

	return data.MakeIntData(int64(list.Len))
}

func rPushXList(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) data.RedisData {
	if strings.ToLower(string(cmd[0])) != "rpushx" {
		log.Printf("rPushXList Function : cmdName is not rpushx")
		return data.MakeErrorData("server error")
	}

	if len(cmd) < 3 {
		return data.MakeErrorData("wrong number of arguments for 'rpushX' command")
	}

	key := string(cmd[1])
	db.CheckTTL(key)

	db.locks.Lock(key)
	defer db.locks.UnLock(key)

	var list *List
	tem, ok := db.db.Get(key)
	if !ok {
		return data.MakeIntData(0)
	} else {
		list, ok = tem.(*List)
		if !ok {
			return data.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
	}
	for i := 2; i < len(cmd); i++ {
		list.RPush(cmd[i])
	}

	return data.MakeIntData(int64(list.Len))
}

func lSetList(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) data.RedisData {
	if strings.ToLower(string(cmd[0])) != "lset" {
		log.Printf("lSetList Function: cmdName is not lset")
		return data.MakeErrorData("server error")
	}
	if len(cmd) != 4 {
		return data.MakeErrorData("wrong number of arguments for 'lset' command")
	}

	index, err := strconv.Atoi(string(cmd[2]))
	if err != nil {
		return data.MakeErrorData("index must be an integer")
	}

	key := string(cmd[1])

	if !db.CheckTTL(key) {
		return data.MakeErrorData("key not exist")
	}

	db.locks.Lock(key)
	defer db.locks.UnLock(key)

	tem, ok := db.db.Get(key)
	if !ok {
		return data.MakeErrorData("key not exist")
	}

	list, ok := tem.(*List)
	if !ok {
		return data.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	success := list.Set(index, cmd[3])
	if !success {
		return data.MakeErrorData("index out of range")
	}

	return data.MakeStringData("OK")
}

func lRemList(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) data.RedisData {
	if strings.ToLower(string(cmd[0])) != "lrem" {
		log.Printf("lRemList Function : cmdName is not lrem")
		return data.MakeErrorData("server error")
	}

	if len(cmd) != 4 {
		return data.MakeErrorData("wrong number of arguments for 'lrem' command")
	}

	count, err := strconv.Atoi(string(cmd[2]))
	if err != nil {
		return data.MakeErrorData("count must be an integer")
	}

	key := string(cmd[1])
	if !db.CheckTTL(key) {
		return data.MakeIntData(0)
	}

	db.locks.Lock(key)
	defer db.locks.UnLock(key)

	tem, ok := db.db.Get(key)
	if !ok {
		return data.MakeIntData(0)
	}

	list, ok := tem.(*List)
	if !ok {
		return data.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	defer func() {
		if list.Len == 0 {
			db.db.Delete(key)
			db.DeleteTTL(key)
		}
	}()

	res := list.RemoveElement(cmd[3], count)

	return data.MakeIntData(int64(res))
}

func lTrimList(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) data.RedisData {
	if strings.ToLower(string(cmd[0])) != "ltrim" {
		log.Printf("lTrimList Function : cmdName is not ltrim")
		return data.MakeErrorData("server error")
	}

	if len(cmd) != 4 {
		return data.MakeErrorData("wrong number of arguments for 'ltrim' command")
	}

	start, err1 := strconv.Atoi(string((cmd[2])))
	end, err2 := strconv.Atoi(string(cmd[3]))

	if err1 != nil || err2 != nil {
		return data.MakeErrorData("start and end must be an integer")
	}

	key := string(cmd[1])
	if !db.CheckTTL(key) {
		return data.MakeStringData("OK")
	}

	db.locks.Lock(key)
	defer db.locks.UnLock(key)

	tem, ok := db.db.Get(key)
	if !ok {
		return data.MakeStringData("OK")
	}

	list, ok := tem.(*List)
	if !ok {
		return data.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	defer func() {
		if list.Len == 0 {
			db.db.Delete(key)
			db.DeleteTTL(key)
		}
	}()

	list.Trim(start, end)

	return data.MakeStringData("OK")
}

func lRangeList(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) data.RedisData {
	if strings.ToLower(string(cmd[0])) != "lrange" {
		log.Printf("lRangeList Function : cmdName is not lrange")
		return data.MakeErrorData("serve error")
	}

	if len(cmd) != 4 {
		return data.MakeErrorData("wrong number of arguments for 'lrange' command")
	}

	start, err1 := strconv.Atoi(string(cmd[2]))
	end, err2 := strconv.Atoi(string(cmd[3]))

	if err1 != nil || err2 != nil {
		return data.MakeErrorData("index must be an integer")
	}

	key := string(cmd[1])
	if !db.CheckTTL(key) {
		return data.MakeEmptyArrayData()
	}

	db.locks.RLock(key)
	defer db.locks.RUnLock(key)

	tem, ok := db.db.Get(key)
	if !ok {
		return data.MakeEmptyArrayData()
	}

	list, ok := tem.(*List)
	if !ok {
		return data.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	temRes := list.Range(start, end)
	if temRes == nil {
		return data.MakeEmptyArrayData()
	}

	res := make([]data.RedisData, len(temRes))
	for i := 0; i < len(temRes); i++ {
		res[i] = data.MakeBulkData(temRes[i])
	}

	return data.MakeArrayData(res)
}

func lMoveList(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) data.RedisData {
	if strings.ToLower(string(cmd[0])) != "lmove" {
		log.Printf("lMoveList Function : cmdName is not lmove")
		return data.MakeErrorData("server error")
	}

	if len(cmd) != 5 {
		return data.MakeErrorData("wrong number of arguments for 'lmove' command")
	}

	src := string(cmd[1])
	des := string(cmd[2])

	srcDrc := strings.ToLower(string(cmd[3]))
	desDrc := strings.ToLower(string(cmd[4]))

	if (srcDrc != "left" && srcDrc != "right") || (desDrc != "left" && desDrc != "right") {
		return data.MakeErrorData("options must be left or right")
	}

	if !db.CheckTTL(src) {
		return data.MakeBulkData(nil)
	}

	db.CheckTTL(des)

	keys := []string{src, des}

	db.locks.LockMulti(keys)
	defer db.locks.UnLockMulti(keys)

	srcTem, ok := db.db.Get(src)
	if !ok {
		return data.MakeBulkData(nil)
	}

	srcList, ok := srcTem.(*List)
	if !ok {
		return data.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	defer func() {
		if srcList.Len == 0 {
			db.db.Delete(src)
			db.DeleteTTL(src)
		}
	}()

	if srcList.Len == 0 {
		return data.MakeBulkData(nil)
	}

	desTem, ok := db.db.Get(des)
	if !ok {
		desTem = NewList()
		db.db.Set(des, desTem)
	}

	desList, ok := desTem.(*List)
	if !ok {
		return data.MakeErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	// pop from src
	var popElem *ListNode
	if srcDrc == "left" {
		popElem = srcList.LPop()
	} else {
		popElem = srcList.RPop()
	}

	// insert to des
	if desDrc == "left" {
		desList.LPush(popElem.Val)
	} else {
		desList.RPush(popElem.Val)
	}

	return data.MakeBulkData(popElem.Val)
}

func blPopList(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) data.RedisData {
	// at least 3 args like "BLPOP" key timeout"
	if len(cmd) < 3 {
		return data.MakeErrorData("ERROR wrong number of arguments for 'blpop' command")
	}
	return bXPopList(ctx, db, cmd, "left")
}

func brPopList(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) data.RedisData {
	// at least 3 args like "BRPOP" key timeout"
	if len(cmd) < 3 {
		return data.MakeErrorData("ERROR wrong number of arguments for 'brpop' command")
	}
	return bXPopList(ctx, db, cmd, "right")
}

func bXPopList(ctx context.Context, db *DB, cmd [][]byte, direction string) data.RedisData {
	// last arg is block timeout
	timeout, err := strconv.Atoi(string(cmd[len(cmd)-1]))
	if err != nil {
		return data.MakeErrorData("ERROR timeout is not a float or out of range")
	}
	var timer *time.Timer
	if timeout == 0 {
		timer = time.NewTimer(math.MaxInt)
	} else {
		timer = time.NewTimer(time.Duration(timeout) * time.Second)
	}

	// query interval (this could be narrowed down to query more frequently but will use more CPU resource)
	ticker := time.NewTicker(100 * time.Millisecond)

	// retrieve all list Names
	keyBytes := cmd[1 : len(cmd)-1]
	keyStrings := make([]string, 0, len(keyBytes))
	for _, bStr := range keyBytes {
		keyStrings = append(keyStrings, string(bStr))
	}
	left := "left"
	right := "right"

	// block (will return inside the infinite loop)
	for {
		select {
		// time to query keys
		case <-ticker.C:
			for _, key := range keyStrings {
				// lock db actions
				db.locks.Lock(key)
				// get key
				tmp, ok := db.db.Get(key)
				// key exist
				if ok {
					// assert is List
					list, isList := tmp.(*List)
					if isList {
						// Pop from list
						var node *ListNode
						if direction == left {
							node = list.LPop()
						} else if direction == right {
							node = list.RPop()
						}
						if node != nil {
							// find a value. need to manually release the lock since we are leaving this scope
							db.locks.UnLock(key)
							return data.MakeArrayData([]data.RedisData{data.MakeStringData(key), data.MakeBulkData(node.Val)})
						}
					}
				}
				// will finally release the lock here if nothing available
				db.locks.UnLock(key)
			}
		// timeout
		case <-timer.C:
			return data.MakeBulkData(nil)
		}
	}
}
func RegisterListCommands() {
	RegisterCommand("llen", lLenList)
	RegisterCommand("lindex", lIndexList)
	RegisterCommand("lpos", lPosList)
	RegisterCommand("lpop", lPopList)
	RegisterCommand("rpop", rPopList)
	RegisterCommand("lpush", lPushList)
	RegisterCommand("lpushx", lPushXList)
	RegisterCommand("rpush", rPushList)
	RegisterCommand("rpushx", rPushXList)
	RegisterCommand("lset", lSetList)
	RegisterCommand("lrem", lRemList)
	RegisterCommand("ltrim", lTrimList)
	RegisterCommand("lrange", lRangeList)
	RegisterCommand("lmove", lMoveList)
	RegisterCommand("blpop", blPopList)
	RegisterCommand("brpop", brPopList)
}
