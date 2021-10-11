package store

import (
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v7"
	"reflect"
	"time"
)

type CacheData struct {
	Field 		string		`json:"field"`
	Value 		interface{}	`json:"value"`
	ClearTime 	time.Time 	`json:"clear_time"`
	ValType     reflect.Type
}

type CacheTimes struct {
	Field     string    `json:"dataKey"`
	Times     int       `json:"times"`
	ClearTime time.Time `json:"clear_time"`
}

type Cache interface {
	GetDataKey() string
	GetDataSize() int
	GetTimesKey() string
	GetTimesLimit() int

	Set(filed, timesField string, value interface{}) error
	Get(filed string, clear bool) (*CacheData, error)
	GetAll() ([]*CacheData, error)
	Del(filed string) error
	ValidTimes(timesField string) (error, bool)
	AddTimes(timesField string) error

	CronTaskCacheDataExpiresClear()
	CronTaskCacheTimesExpiresClear()
}

type redisCache struct {
	dataKey              string
	dataSize             int
	dataExpiresDuration  time.Duration
	dataClearQueue       *Queue
	timesKey             string
	timesLimit           int
	timesExpiresDuration time.Duration
	timesClearQueue       *Queue
}

func NewCache(key string, size, timesLimit int, dataExpiresDuration, timesExpiresDuration time.Duration) Cache {
	cache := &redisCache{
		dataKey:              key,
		dataSize:             size,
		dataExpiresDuration:  dataExpiresDuration,
		dataClearQueue:       NewQueue(size),
		timesKey:             key + "_times",
		timesLimit:           timesLimit,
		timesExpiresDuration: timesExpiresDuration,
	}
	go cache.runCacheDataExpiresClear()
	if timesLimit > 0 {
	    cache.timesClearQueue = NewQueue(size)
		go cache.runCacheTimesExpiresClear()
	}
	return cache
}

func (c *redisCache) GetDataKey() string {
	return c.dataKey
}

func (c *redisCache) GetDataSize() int {
	return c.dataSize
}

func (c *redisCache) GetTimesKey() string {
	return c.timesKey
}

func (c *redisCache) GetTimesLimit() int {
	return c.timesLimit
}

func (c *redisCache) Set(filed, timesField string, value interface{}) error {
	if value == nil {
		return nil
	}
	data := CacheData{
		Field:     filed,
		Value:     value,
		ClearTime: time.Now().Add(c.dataExpiresDuration),
	}
	jsonBts, err := json.Marshal(data)
	if err != nil {
		return err
	}
	if err := Redis().HashSet(c.dataKey, filed, jsonBts); err != nil {
		return err
	}
	if len(timesField) > 0 {
		_ = c.AddTimes(timesField)
	}
	c.dataClearQueue.Push(&Node{Value: filed})
	return nil
}

func (c *redisCache) Get(filed string, clear bool) (*CacheData, error) {
	jsonStr, err := Redis().HashGet(c.dataKey, filed)
	if err != nil {
		return nil, err
	}
	if err == redis.Nil {
		return nil, nil
	}
	if jsonStr == "" {
		return nil, nil
	}
	var data CacheData
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return nil, err
	}
	if clear {
		_ = c.Del(filed)
	}
	return &data, nil
}

func (c *redisCache) GetAll() ([]*CacheData, error) {
	vals, err := Redis().HashVals(c.dataKey)
	if err != nil {
		return nil, err
	}
	var datas []*CacheData
	for _, val := range vals {
		var data CacheData
		if err := json.Unmarshal([]byte(val), &data); err != nil {
			return nil, err
		}
		datas = append(datas, &data)
	}
	return datas, nil
}

func (c *redisCache) Del(filed string) error {
	return Redis().HashDelete(c.dataKey, filed)
}

func (c *redisCache) ValidTimes(timesField string) (error, bool) {
	if c.timesLimit <= 0 {
		return nil, true
	}
	cacheTimes, err := c.getTimes(timesField)
	if err != nil {
		return err, false
	}
	if cacheTimes == nil {
		return nil, true
	}
	if cacheTimes.Times >= c.timesLimit {
		return nil, false
	}
	return nil, true
}

func (c *redisCache) AddTimes(timesField string) error {
	if c.timesLimit <= 0 {
		return nil
	}
	cacheTimes, err := c.getTimes(timesField)
	if err != nil {
		return err
	}
	if cacheTimes == nil {
		cacheTimes = &CacheTimes{
			Field:  	timesField,
			Times:     	1,
			ClearTime: 	time.Now().Add(c.timesExpiresDuration),
		}
	} else {
		cacheTimes.Times += 1
	}
	jsonBts, err := json.Marshal(cacheTimes)
	if err != nil {
		return err
	}
	if err := Redis().HashSet(c.timesKey, timesField, jsonBts);  err != nil {
		return err
	}
	c.timesClearQueue.Push(&Node{Value: timesField})
	return nil
}

func (c *redisCache) getTimes(timesField string) (*CacheTimes, error) {
	if c.timesLimit <= 0 {
		return nil, nil
	}
	jsonStr, err := Redis().HashGet(c.timesKey, timesField)
	if err != nil {
		return nil, err
	}
	if err == redis.Nil {
		return nil, nil
	}
	if jsonStr == "" {
		return nil, nil
	}
	var cacheTimes CacheTimes
	if err := json.Unmarshal([]byte(jsonStr), &cacheTimes); err != nil {
		return nil, err
	}
	return &cacheTimes, nil
}

func (c *redisCache) getAllTimes() ([]*CacheTimes, error) {
	vals, err := Redis().HashVals(c.timesKey)
	if err != nil {
		return nil, err
	}
	var cacheTimes []*CacheTimes
	for _, val := range vals {
		var times CacheTimes
		if err := json.Unmarshal([]byte(val), &times); err != nil {
			return nil, err
		}
		cacheTimes = append(cacheTimes, &times)
	}
	return cacheTimes, nil
}

func (c *redisCache) delTimes(timesFiled string) error {
	return Redis().HashDelete(c.timesKey, timesFiled)
}

func (c *redisCache) runCacheDataExpiresClear() {
	for {
		node := c.dataClearQueue.Pop()
		if node == nil {
			time.Sleep(c.dataExpiresDuration)
			continue
		}
		data, err := c.Get(node.Value, false)
		if err != nil {
			time.Sleep(time.Minute)
			continue
		}
		if data == nil {
			time.Sleep(time.Minute)
			continue
		}
		if data.ClearTime.After(time.Now()) {
			time.Sleep(data.ClearTime.Sub(time.Now()))
		}
		_ = c.Del(node.Value)
	}
}

func (c *redisCache) runCacheTimesExpiresClear() {
	for {
		node := c.timesClearQueue.Pop()
		if node == nil {
			time.Sleep(c.timesExpiresDuration)
			continue
		}
		times, err := c.getTimes(node.Value)
		if err != nil {
			time.Sleep(time.Minute)
			continue
		}
		if times == nil {
			time.Sleep(time.Minute)
			continue
		}
		if times.ClearTime.After(time.Now()) {
			time.Sleep(times.ClearTime.Sub(time.Now()))
		}
		_ = c.delTimes(node.Value)
	}
}

func (c *redisCache) CronTaskCacheDataExpiresClear() {
	cacheDatas, err := c.GetAll()
	if err != nil {
		return
	}
	for _, cacheData := range cacheDatas {
		if cacheData.ClearTime.After(time.Now()) {
			continue
		}
		_ = c.Del(cacheData.Field)
	}
}

func (c *redisCache) CronTaskCacheTimesExpiresClear() {
	cacheTimes, err := c.getAllTimes()
	if err != nil {
		return
	}
	for _, times := range cacheTimes {
		if times.ClearTime.After(time.Now()) {
			continue
		}
		_ = c.delTimes(times.Field)
	}
}



type Node struct {
	Value string
}

func (n *Node) String() string {
	return fmt.Sprint(n.Value)
}

// NewStack returns a new stack.
func NewStack() *Stack {
	return &Stack{}
}

// Stack is a basic LIFO stack that resizes as needed.
type Stack struct {
	nodes []*Node
	count int
}

// Push adds a node to the stack.
func (s *Stack) Push(n *Node) {
	s.nodes = append(s.nodes[:s.count], n)
	s.count++
}

// Pop removes and returns a node from the stack in last to first order.
func (s *Stack) Pop() *Node {
	if s.count == 0 {
		return nil
	}
	s.count--
	return s.nodes[s.count]
}

// NewQueue returns a new requestIdQueue with the given initial dataSize.
func NewQueue(size int) *Queue {
	return &Queue{
		nodes: make([]*Node, size),
		size:  size,
	}
}

// Queue is a basic FIFO requestIdQueue based on a circular list that resizes as needed.
type Queue struct {
	nodes []*Node
	size  int
	head  int
	tail  int
	count int
}

// Push adds a node to the requestIdQueue.
func (q *Queue) Push(n *Node) {
	if q.head == q.tail && q.count > 0 {
		nodes := make([]*Node, len(q.nodes)+q.size)
		copy(nodes, q.nodes[q.head:])
		copy(nodes[len(q.nodes)-q.head:], q.nodes[:q.head])
		q.head = 0
		q.tail = len(q.nodes)
		q.nodes = nodes
	}
	q.nodes[q.tail] = n
	q.tail = (q.tail + 1) % len(q.nodes)
	q.count++
}

// Pop removes and returns a node from the requestIdQueue in first to last order.
func (q *Queue) Pop() *Node {
	if q.count == 0 {
		return nil
	}
	node := q.nodes[q.head]
	q.head = (q.head + 1) % len(q.nodes)
	q.count--
	return node
}