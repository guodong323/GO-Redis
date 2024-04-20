package db

import (
	_ "hash/fnv"
	"log"
	"sort"
	"sync"
)

// Locks apply to a db to ensure some atomic operations
// It locks a key according to its hash value
type Locks struct {
	locks []*sync.RWMutex
}

// NewLocks
// 数据项的哈希值用于决定使用locks切片中的哪个锁，从而确保对同一个数据项的访问始终使用同一个锁
// 减少对锁的竞争
func NewLocks(size int) *Locks {
	locks := make([]*sync.RWMutex, size)
	for i := 0; i < size; i++ {
		locks[i] = &sync.RWMutex{}
	}
	return &Locks{locks: locks}
}

func (lock *Locks) GetKeyPosition(key string) int {
	position := HashKey(key)
	return position % len(lock.locks)
}

func (lock *Locks) Lock(key string) {
	position := lock.GetKeyPosition(key)
	if position == -1 {
		log.Printf("Locks Lock key %s error: pos == -1", key)
		return
	}
	lock.locks[position].Lock()
}

func (lock *Locks) UnLock(key string) {
	position := lock.GetKeyPosition(key)
	if position == -1 {
		log.Printf("Locks Lock key %s error: pos == -1", key)
		return
	}
	lock.locks[position].Unlock()
}

func (lock *Locks) RLock(key string) {
	position := lock.GetKeyPosition(key)
	if position == -1 {
		log.Printf("Locks Lock key %s error: pos == -1", key)
		return
	}
	lock.locks[position].RLock()
}

func (lock *Locks) RUnLock(key string) {
	position := lock.GetKeyPosition(key)
	if position == -1 {
		log.Printf("Locks Lock key %s error: pos == -1", key)
		return
	}
	lock.locks[position].RUnlock()
}

// sortedLockPositions 对一组键对应锁的位置，并将这些位置去重后进行排序
// 返回一个有序的位置列表，确保所有线程都按照相同的顺序获取锁，避免死锁
// 之所以用set去重，是因为有的时候不同的key通过hash到同一个锁
// 为了避免同一位置的锁被多次获取
func (lock *Locks) sortedLockPositions(keys []string) []int {
	set := make(map[int]struct{})
	for _, key := range keys {
		position := lock.GetKeyPosition(key)
		if position == -1 {
			log.Printf("Locks Lock key %s error: pos == -1", key)
			return nil
		}
		set[position] = struct{}{}
	}
	positions := make([]int, len(set))
	i := 0
	for pos := range set {
		positions[i] = pos
		i++
	}
	sort.Ints(positions)
	return positions
}

func (lock *Locks) LockMulti(keys []string) {
	positions := lock.sortedLockPositions(keys)
	if positions == nil {
		return
	}
	for _, position := range positions {
		lock.locks[position].Lock()

	}
}

func (lock *Locks) UnLockMulti(keys []string) {
	positions := lock.sortedLockPositions(keys)
	if positions == nil {
		return
	}
	for _, position := range positions {
		lock.locks[position].Unlock()
	}
}

func (lock *Locks) RLockMulti(keys []string) {
	positions := lock.sortedLockPositions(keys)
	if positions == nil {
		return
	}
	for _, position := range positions {
		lock.locks[position].RLock()
	}
}

func (lock *Locks) RUnLockMulti(keys []string) {
	positions := lock.sortedLockPositions(keys)
	if positions == nil {
		return
	}
	for _, position := range positions {
		lock.locks[position].RUnlock()
	}
}
