package db

import (
	"GO-Redis/config"
	"log"
	"time"
)

type DB struct {
	db      *ConcurrentMap
	ttlKeys *ConcurrentMap
	locks   *Locks
}

type TTLInfo struct {
	value int64
	// 在必要时发送取消TTL任务的信号
	// 如果一个键的TTL（Time-To-Live）被更新或删除
	// 与该键相关联的旧的TTL任务应当被取消以避免不必要的操作或潜在的资源冲突。
	cancel chan struct{}
}

// NewDB
// DB is the memory cache database
// All key:value pairs are stored in db
// All ttl keys are stored in ttlKeys
// locks is used to lock a key for db to ensure some atomic operations
func NewDB() *DB {
	return &DB{
		db:      NewConcurrentMap(config.Configures.ShardNumber),
		ttlKeys: NewConcurrentMap(config.Configures.ShardNumber),
		locks:   NewLocks(config.Configures.ShardNumber * 2),
	}
}

// CheckTTL check ttl keys and delete expired keys
// return false if key is expired, else true.
// Attention: Don't lock this function because it has called locks.Lock(key) for atomic deleting expired key.
// Otherwise, it will cause a deadlock.
func (db *DB) CheckTTL(key string) bool {
	ttl, ok := db.ttlKeys.Get(key)
	if !ok {
		return true
	}
	ttlTime := ttl.(*TTLInfo)
	now := time.Now().Unix()
	if ttlTime.value > now {
		return true
	}
	// if it should expire
	db.locks.Lock(key)
	defer db.locks.UnLock(key)
	db.db.Delete(key)
	db.ttlKeys.Delete(key)
	return false
}

// SetTTL set ttl for key
// return bool to check if ttl set success
// return int to check if the key is a new ttl key
// value: seconds at expire
func (db *DB) SetTTL(key string, value int64) bool {
	if _, ok := db.db.Get(key); !ok {
		log.Printf("SetTTL: key not exist")
		return false
	}
	cancel := make(chan struct{})
	// remove old TTL task if exist
	ttlInfoMap, ok := db.ttlKeys.Get(key)
	if ok {
		ttlInfo := ttlInfoMap.(*TTLInfo)
		close(ttlInfo.cancel)
		db.ttlKeys.Delete(key)
	}
	// save TTL
	db.ttlKeys.Set(key, &TTLInfo{
		value:  value,
		cancel: cancel,
	})
	// TTL check timed task
	// 启用goroutine来异步处理TTL到期
	go func() {
		select {
		// this chan fires after the ttl expire
		case <-time.After(time.Duration(value-time.Now().Unix()) * time.Second):
			db.CheckTTL(key)
			log.Printf("TTL fires")
		case <-cancel:
			log.Printf("TTL canceled")
			return
		}
	}()
	return true
}

func (db *DB) DeleteTTL(key string) bool {
	// check ttl exist
	ttlInfoMap, ok := db.ttlKeys.Get(key)
	if !ok {
		return false
	}
	// cancel timed TTL task
	ttl := ttlInfoMap.(*TTLInfo)
	close(ttl.cancel)
	// delete TTL key from concurrentMap
	return db.ttlKeys.Delete(key)
}
