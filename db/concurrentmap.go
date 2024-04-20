package db

import (
	"hash/fnv"
	"sync"
	"sync/atomic"
)

const MaxSize = int(1<<31 - 1)

// shard is the object that represents a [K:V] pair in redis
type shard struct {
	item map[string]any
	rwMu *sync.RWMutex
}

// ConcurrentMap manage a table slice with multiple hashmap shards to avoid lock bottleneck
// It is threads safe by using rwLock
// It supports maximum table size = MaxSize
type ConcurrentMap struct {
	table []*shard
	size  int
	count int64
}

func NewConcurrentMap(size int) *ConcurrentMap {
	if size <= 0 || size > MaxSize {
		size = MaxSize
	}
	m := &ConcurrentMap{
		table: make([]*shard, size),
		size:  size,
		count: 0,
	}
	for i := 0; i < size; i++ {
		m.table[i] = &shard{item: make(map[string]any), rwMu: &sync.RWMutex{}}
	}
	return m
}

func (m *ConcurrentMap) getKeyPosition(key string) int {
	return HashKey(key) % m.size
}

func (m *ConcurrentMap) getShard(key string) *shard {
	return m.table[m.getKeyPosition(key)]
}

func (m *ConcurrentMap) Set(key string, value any) int {
	added := 0
	shard := m.getShard(key)
	shard.rwMu.Lock()
	defer shard.rwMu.Unlock()

	if _, OK := shard.item[key]; OK == false {
		m.count++
		added = 1
	}
	shard.item[key] = value
	return added
}

func (m *ConcurrentMap) SetIfExits(key string, value any) int {
	position := m.getKeyPosition(key)
	shard := m.table[position]
	shard.rwMu.Lock()
	defer shard.rwMu.Unlock()

	if _, OK := shard.item[key]; OK == true {
		shard.item[key] = value
		return 1
	}
	return 0
}

func (m *ConcurrentMap) SetIfNotExist(key string, value any) int {
	position := m.getKeyPosition(key)
	shard := m.table[position]
	shard.rwMu.Lock()
	defer shard.rwMu.Unlock()

	if _, OK := shard.item[key]; OK == false {
		m.count++
		shard.item[key] = value
		return 1
	}
	return 0
}

func (m *ConcurrentMap) Get(key string) (any, bool) {
	position := m.getKeyPosition(key)
	// lock shard inorder to reading
	// this ensures no write will be synchronized before this read finished
	shard := m.table[position]
	shard.rwMu.Lock()
	defer shard.rwMu.Unlock()

	value, OK := shard.item[key]
	return value, OK
}

func (m *ConcurrentMap) Delete(key string) bool {
	position := m.getKeyPosition(key)
	shard := m.table[position]
	shard.rwMu.Lock()
	defer shard.rwMu.Unlock()

	if _, OK := shard.item[key]; OK == true {
		delete(shard.item, key)
		m.count--
		return true
	} else {
		return false
	}
}

func (m *ConcurrentMap) Len() int64 {
	return atomic.LoadInt64(&m.count)
}

func (m *ConcurrentMap) Clear() {
	*m = *NewConcurrentMap(m.size)
}

// Keys return all stored keys in the concurrent map
func (m *ConcurrentMap) Keys() []string {
	keys := make([]string, m.count)
	i := 0
	for _, shard := range m.table {
		shard.rwMu.RLock()
		for key := range shard.item {
			keys[i] = key
			i++
		}
		shard.rwMu.RUnlock()
	}
	return keys
}

func (m *ConcurrentMap) KeyValues() map[string]any {
	res := make(map[string]any)
	i := 0
	for _, shard := range m.table {
		shard.rwMu.RLock()
		for key, value := range shard.item {
			res[key] = value
			i++
		}
		shard.rwMu.RUnlock()
	}
	return res
}

// HashKey hash a string to an int value using fnv32 algorithm
func HashKey(key string) int {
	fnv32 := fnv.New32()
	key = "@#&" + key + "*^%$"
	_, _ = fnv32.Write([]byte(key))
	return int(fnv32.Sum32())
}
