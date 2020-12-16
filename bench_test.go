package memcache

import (
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func benchmarkNewSet(b *testing.B, item *Item) {
	c := newDockerServer(b)
	c.SetTimeout(time.Duration(-1))
	b.SetBytes(int64(len(item.Key) + len(item.Value)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.Set(item); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

func benchmarkNewSetGet(b *testing.B, item *Item) {
	c := newDockerServer(b)
	c.SetTimeout(time.Duration(-1))
	key := item.Key
	b.SetBytes(int64(len(item.Key) + len(item.Value)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.Set(item); err != nil {
			b.Fatal(err)
		}
		if _, err := c.Get(key); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

func largeItem() *Item {
	key := strings.Repeat("f", 240)
	value := make([]byte, 1024)
	return &Item{Key: key, Value: value}
}

func smallItem() *Item {
	return &Item{Key: "foo", Value: []byte("bar")}
}

func BenchmarkNewSet(b *testing.B) {
	benchmarkNewSet(b, smallItem())
}

func BenchmarkNewSetLarge(b *testing.B) {
	benchmarkNewSet(b, largeItem())
}

func BenchmarkNewSetGet(b *testing.B) {
	benchmarkNewSetGet(b, smallItem())
}

func BenchmarkNewSetGetLarge(b *testing.B) {
	benchmarkNewSetGet(b, largeItem())
}

func benchmarkNewConcurrentSetGet(b *testing.B, item *Item, count int, opcount int) {
	mp := runtime.GOMAXPROCS(0)
	defer runtime.GOMAXPROCS(mp)
	runtime.GOMAXPROCS(count)
	c := newDockerServer(b)
	c.SetTimeout(time.Duration(-1))
	// Items are not thread safe
	items := make([]*Item, count)
	for ii := range items {
		items[ii] = &Item{Key: item.Key, Value: item.Value}
	}
	b.SetBytes(int64((len(item.Key) + len(item.Value)) * count * opcount))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(count)
		for j := 0; j < count; j++ {
			it := items[j]
			key := it.Key
			go func() {
				defer wg.Done()
				for k := 0; k < opcount; k++ {
					if err := c.Set(it); err != nil {
						b.Fatal(err)
					}
					if _, err := c.Get(key); err != nil {
						b.Fatal(err)
					}
				}
			}()
		}
		wg.Wait()
	}
	b.StopTimer()
}

func BenchmarkNewGetCacheMiss(b *testing.B) {
	key := "not"
	c := newDockerServer(b)
	c.SetTimeout(time.Duration(-1))
	c.Delete(key)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Get(key); err != ErrCacheMiss {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

func BenchmarkNewConcurrentSetGetSmall10_100(b *testing.B) {
	benchmarkNewConcurrentSetGet(b, smallItem(), 10, 100)
}

func BenchmarkNewConcurrentSetGetLarge10_100(b *testing.B) {
	benchmarkNewConcurrentSetGet(b, largeItem(), 10, 100)
}

func BenchmarkNewConcurrentSetGetSmall20_100(b *testing.B) {
	benchmarkNewConcurrentSetGet(b, smallItem(), 20, 100)
}

func BenchmarkNewConcurrentSetGetLarge20_100(b *testing.B) {
	benchmarkNewConcurrentSetGet(b, largeItem(), 20, 100)
}
