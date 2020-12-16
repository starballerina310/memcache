package memcache

import (
	"fmt"
	"github.com/rainycape/memcache"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func newDockerServerOld(tb testing.TB) *memcache.Client {
	c, err := net.Dial("tcp", "memcached:11211")
	if err != nil {
		tb.Skip(fmt.Sprintf("skipping test; no server running at %s", "memcached:11211"))
		return nil
	}
	c.Write([]byte("flush_all\r\n"))
	c.Close()
	client, err := memcache.New("memcached:11211")
	if err != nil {
		tb.Fatal(err)
		return nil
	}
	return client
}

func benchmarkOldSet(b *testing.B, item *memcache.Item) {
	c := newDockerServerOld(b)
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

func benchmarkOldSetGet(b *testing.B, item *memcache.Item) {
	c := newDockerServerOld(b)
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

func largeItemOld() *memcache.Item {
	key := strings.Repeat("f", 240)
	value := make([]byte, 1024)
	return &memcache.Item{Key: key, Value: value}
}

func smallItemOld() *memcache.Item {
	return &memcache.Item{Key: "foo", Value: []byte("bar")}
}

func BenchmarkOldSet(b *testing.B) {
	benchmarkOldSet(b, smallItemOld())
}

func BenchmarkOldSetLarge(b *testing.B) {
	benchmarkOldSet(b, largeItemOld())
}

func BenchmarkOldSetGet(b *testing.B) {
	benchmarkOldSetGet(b, smallItemOld())
}

func BenchmarkOldSetGetLarge(b *testing.B) {
	benchmarkOldSetGet(b, largeItemOld())
}

func benchmarkOldConcurrentSetGet(b *testing.B, item *memcache.Item, count int, opcount int) {
	mp := runtime.GOMAXPROCS(0)
	defer runtime.GOMAXPROCS(mp)
	runtime.GOMAXPROCS(count)
	c := newDockerServerOld(b)
	c.SetTimeout(time.Duration(-1))
	// Items are not thread safe
	items := make([]*memcache.Item, count)
	for ii := range items {
		items[ii] = &memcache.Item{Key: item.Key, Value: item.Value}
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

func BenchmarkOldGetCacheMiss(b *testing.B) {
	key := "not"
	c := newDockerServerOld(b)
	c.SetTimeout(time.Duration(-1))
	c.Delete(key)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Get(key); err != memcache.ErrCacheMiss {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

func BenchmarkOldConcurrentSetGetSmall10_100(b *testing.B) {
	benchmarkOldConcurrentSetGet(b, smallItemOld(), 10, 100)
}

func BenchmarkOldConcurrentSetGetLarge10_100(b *testing.B) {
	benchmarkOldConcurrentSetGet(b, largeItemOld(), 10, 100)
}

func BenchmarkOldConcurrentSetGetSmall20_100(b *testing.B) {
	benchmarkOldConcurrentSetGet(b, smallItemOld(), 20, 100)
}

func BenchmarkOldConcurrentSetGetLarge20_100(b *testing.B) {
	benchmarkOldConcurrentSetGet(b, largeItemOld(), 20, 100)
}
