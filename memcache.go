/*
Copyright 2011 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package memcache provides a client for the memcached cache server.
package memcache

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"
	"net"
	"sync"
	"syscall"
	"time"
)

// Similar to:
// http://code.google.com/appengine/docs/go/memcache/reference.html
var (
	// ErrCacheMiss means that a Get failed because the item wasn't present.
	ErrCacheMiss = errors.New("memcache: cache miss")

	// ErrCASConflict means that a CompareAndSwap call failed due to the
	// cached value being modified between the Get and the CompareAndSwap.
	// If the cached value was simply evicted rather than replaced,
	// ErrNotStored will be returned instead.
	ErrCASConflict = errors.New("memcache: compare-and-swap conflict")

	// ErrNotStored means that a conditional write operation (i.e. Add or
	// CompareAndSwap) failed because the condition was not satisfied.
	ErrNotStored = errors.New("memcache: item not stored")

	// ErrServer means that a server error occurred.
	ErrServerError = errors.New("memcache: server error")

	// ErrNoStats means that no statistics were available.
	ErrNoStats = errors.New("memcache: no statistics available")

	// ErrMalformedKey is returned when an invalid key is used.
	// Keys must be at maximum 250 bytes long, ASCII, and not
	// contain whitespace or control characters.
	ErrMalformedKey = errors.New("malformed: key is too long or contains invalid characters")

	// ErrNoServers is returned when no servers are configured or available.
	ErrNoServers = errors.New("memcache: no servers configured or available")

	// ErrBadMagic is returned when the magic number in a response is not valid.
	ErrBadMagic = errors.New("memcache: bad magic number in response")

	// ErrBadIncrDec is returned when performing a incr/decr on non-numeric values.
	ErrBadIncrDec = errors.New("memcache: incr or decr on non-numeric value")

	putUint16 = binary.BigEndian.PutUint16
	putUint32 = binary.BigEndian.PutUint32
	putUint64 = binary.BigEndian.PutUint64
	bUint16   = binary.BigEndian.Uint16
	bUint32   = binary.BigEndian.Uint32
	bUint64   = binary.BigEndian.Uint64
)

var (
	_writerPool = sync.Pool{
		New: func() interface{} {
			return bufio.NewWriter(nil)
		},
	}
)

// DefaultPoolTimeout is the default pool timeout
// DefaultSocketTimeout is the default socket timeout
// DefaultBufPoolSize is the default size for buffer pool
// maxIdleConnsPerAddr is the default size for connection pool
const (
	DefaultSocketTimeout = 0
	DefaultBufPoolSize   = 200000
	DefaultKeepAlive     = time.Duration(10) * time.Second
	maxIdleConnsPerAddr  = 10
)

// ==================================================================
// ConnPool
// ==================================================================
type ConnPool struct {
	pool  sync.Pool
	limit chan struct{}
}

func getConnWrapper(c *Client, addr *Addr) func() *conn {
	return func() *conn {
		cn, err := c.getNewConn(addr)
		if err != nil {
			return nil
		}
		return cn
	}
}

func newConnPool(lim int, fn func() *conn) *ConnPool {
	// init connection pool
	p := ConnPool{
		pool: sync.Pool{
			New: func() interface{} {
				return fn()
			},
		},
		limit: make(chan struct{}, lim),
	}
	// set idle connections
	for i := 0; i < lim; i++ {
		p.Put(fn())
	}
	return &p
}

func (p *ConnPool) Get() *conn {
	// total connections <= upperlimit
	select {
	case <-p.limit:
	/* ignore */
	default:
		/* ignore */
	}
	return p.pool.Get().(*conn)
}

func (p *ConnPool) Put(c *conn) {
	select {
	// total idle connetions <= limit
	case p.limit <- struct{}{}:
		p.pool.Put(c)
	default:
		c.nc.Close()
	}
}

// ==================================================================
// BufferPool
// ==================================================================
type BufPool struct {
	pool  sync.Pool
	limit chan struct{}
}

func newBufPool(lim int) *BufPool {
	p := BufPool{
		pool: sync.Pool{
			New: func() interface{} {
				// 24 is header size
				arr := make([]byte, 24, 24)
				return &arr
			},
		},
		limit: make(chan struct{}, lim),
	}
	for i := 0; i < lim; i++ {
		arr := make([]byte, 24, 24)
		p.Put(&arr)
	}
	return &p

}

func (p *BufPool) CleanBuf(b *[]byte) *[]byte {
	arr := *b
	for i, _ := range arr {
		arr[i] = byte(0)
	}
	*b = arr
	return b
}

func (p *BufPool) Get() *[]byte {
	// total buffers <= limit
	select {
	case <-p.limit:
	/* ignore */
	default:
		/* ignore */
	}
	return p.pool.Get().(*[]byte)
}

func (p *BufPool) Put(b *[]byte) {
	select {
	case p.limit <- struct{}{}:
		p.pool.Put(b)
	default:
		b = nil
	}
}

// ==================================================================
// commands
// ==================================================================
type command uint8

const (
	cmdGet = iota
	cmdSet
	cmdAdd
	cmdReplace
	cmdDelete
	cmdIncr
	cmdDecr
	cmdQuit
	cmdFlush
	cmdGetQ
	cmdNoop
	cmdVersion
	cmdGetK
	cmdGetKQ
	cmdAppend
	cmdPrepend
	cmdStat
	cmdSetQ
	cmdAddQ
	cmdReplaceQ
	cmdDeleteQ
	cmdIncrementQ
	cmdDecrementQ
	cmdQuitQ
	cmdFlushQ
	cmdAppendQ
	cmdPrependQ
)

// ==================================================================
// responses
// ==================================================================
type response uint16

const (
	respOk = iota
	respKeyNotFound
	respKeyExists
	respValueTooLarge
	respInvalidArgs
	respItemNotStored
	respInvalidIncrDecr
	respWrongVBucket
	respAuthErr
	respAuthContinue
	respUnknownCmd   = 0x81
	respOOM          = 0x82
	respNotSupported = 0x83
	respInternalErr  = 0x85
	respBusy         = 0x85
	respTemporaryErr = 0x86
)

func (r response) asError() error {
	switch r {
	case respKeyNotFound:
		return ErrCacheMiss
	case respKeyExists:
		return ErrNotStored
	case respInvalidIncrDecr:
		return ErrBadIncrDec
	case respItemNotStored:
		return ErrNotStored
	}
	return r
}

func (r response) Error() string {
	switch r {
	case respOk:
		return "Ok"
	case respKeyNotFound:
		return "key not found"
	case respKeyExists:
		return "key already exists"
	case respValueTooLarge:
		return "value too large"
	case respInvalidArgs:
		return "invalid arguments"
	case respItemNotStored:
		return "item not stored"
	case respInvalidIncrDecr:
		return "incr/decr on non-numeric value"
	case respWrongVBucket:
		return "wrong vbucket"
	case respAuthErr:
		return "auth error"
	case respAuthContinue:
		return "auth continue"
	}
	return ""
}

const (
	reqMagic  uint8 = 0x80
	respMagic uint8 = 0x81
)

// ==================================================================
// functions
// ==================================================================
func legalKey(key string) bool {
	if len(key) > 250 {
		return false
	}
	for i := 0; i < len(key); i++ {
		if key[i] <= ' ' || key[i] > 0x7e {
			return false
		}
	}
	return true
}

func poolSize() int {
	return DefaultBufPoolSize
}

// ==================================================================
// New returns a memcache client using the provided server(s)
// with equal weight. If a server is listed multiple times,
// it gets a proportional amount of weight.
func New(server ...string) (*Client, error) {
	servers, err := NewServerList(server...)
	if err != nil {
		return nil, err
	}
	return NewFromServers(servers), nil
}

func NewWithMaxIdlePerAddr(maxIdlePerAddr int, server ...string) (*Client, error) {
	servers, err := NewServerList(server...)
	if err != nil {
		return nil, err
	}
	return NewFromServersWithMaxIdlePerAddr(maxIdlePerAddr, servers), nil
}

// NewFromServers returns a new Client using the provided Servers.
func NewFromServers(servers Servers) *Client {
	cli := &Client{
		timeout:        DefaultSocketTimeout,
		maxIdlePerAddr: maxIdleConnsPerAddr,
		servers:        servers,
		bufPool:        newBufPool(poolSize()),
	}
	cli.initConnPool()
	return cli
}

func NewFromServersWithMaxIdlePerAddr(maxIdlePerAddr int, servers Servers) *Client {
	cli := &Client{
		timeout:        DefaultSocketTimeout,
		maxIdlePerAddr: maxIdlePerAddr,
		servers:        servers,
		bufPool:        newBufPool(poolSize()),
	}
	cli.initConnPool()
	return cli
}

func (cli *Client) initConnPool() {
	addrList, _ := cli.servers.Servers()
	freeconn := make(map[string]*ConnPool, len(addrList))
	for _, addr := range addrList {
		freeconn[addr.s] = newConnPool(cli.maxIdlePerAddr, getConnWrapper(cli, addr))
	}
	cli.freeconn = freeconn
	return
}

// ==================================================================
// Client is a memcache client.
// It is safe for unlocked use by multiple concurrent goroutines.
type Client struct {
	timeout        time.Duration
	maxIdlePerAddr int
	servers        Servers
	mu             sync.RWMutex
	freeconn       map[string]*ConnPool
	bufPool        *BufPool
}

// Timeout returns the socket read/write timeout. By default, it's
// DefaultSocketTimeout.
func (c *Client) Timeout() time.Duration {
	return c.timeout
}

// SetTimeout specifies the socket read/write timeout.
// If zero, DefaultSocketTimeout is used. If < 0, there's
// no timeout. This method must be called before any
// connections to the memcached server are opened.
func (c *Client) SetTimeout(timeout time.Duration) {
	if timeout == time.Duration(0) {
		timeout = DefaultSocketTimeout
	}
	c.timeout = timeout
}

// MaxIdleConnsPerAddr returns the maximum number of idle
// connections kept per server address.
func (c *Client) MaxIdleConnsPerAddr() int {
	return c.maxIdlePerAddr
}

// SetMaxIdleConnsPerAddr changes the maximum number of
// idle connections kept per server. If maxIdle < 0,
// no idle connections are kept. If maxIdle == 0,
// the default number (currently 2) is used.
func (c *Client) SetMaxIdleConnsPerAddr(maxIdle int) {
	if maxIdle == 0 {
		maxIdle = maxIdleConnsPerAddr
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.maxIdlePerAddr = maxIdle
	if maxIdle > 0 {
		c.closeIdleConns()
		freeconn := make(map[string]*ConnPool)
		if servers, serversErr := c.servers.Servers(); serversErr == nil {
			for _, addr := range servers {
				freeconn[addr.s] = newConnPool(maxIdle, getConnWrapper(c, addr))
			}
		}
		c.freeconn = freeconn
	} else {
		c.closeIdleConns()
		c.freeconn = nil
	}
}

// Close closes all currently open connections.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closeIdleConns()
	c.freeconn = nil
	c.maxIdlePerAddr = 0
	return nil
}

// ==================================================================
// Item is an item to be got or stored in a memcached server.
type Item struct {
	// Key is the Item's key (250 bytes maximum).
	Key string

	// Value is the Item's value.
	Value []byte

	// Flags are server-opaque flags whose semantics are entirely
	// up to the app.
	Flags uint32

	// Expiration is the cache expiration time, in seconds: either a relative
	// time from now (up to 1 month), or an absolute Unix epoch time.
	// Zero means the Item has no expiration time.
	Expiration int32

	// Compare and swap ID.
	casid uint64
}

// ==================================================================
// conn is a connection to a server.
type conn struct {
	nc   net.Conn
	addr *Addr
}

// condRelease releases this connection if the error pointed to by err
// is is nil (not an error) or is only a protocol level error (e.g. a
// cache miss).  The purpose is to not recycle TCP connections that
// are bad.
func (c *Client) condRelease(cn *conn, err *error) {
	switch *err {
	case nil, ErrCacheMiss, ErrCASConflict, ErrNotStored, ErrBadIncrDec:
		c.putFreeConn(cn)
	case syscall.EPIPE:
		if cn != nil && cn.nc != nil {
			cn.nc.Close()
			cn = nil
		}
	default:
		if cn != nil {
			if ne, ok := (*err).(net.Error); ok && ne.Temporary() {
				c.putFreeConn(cn)
			} else if cn.nc != nil {
				cn.nc.Close()
			}
			cn = nil
		}
	}
}

func (c *Client) closeIdleConns() {
	// please exec mu.Lock in caller
	if c.freeconn == nil {
		return
	}
	for k, v := range c.freeconn {
		if v.limit != nil {
			for len(v.limit) > 0 {
				cn := v.Get()
				if cn != nil && cn.nc != nil {
					cn.nc.Close()
					cn = nil
				}
				if cn == nil {
					break
				}
			}
			c.freeconn[k] = nil
		}
	}
}

func (c *Client) putFreeConn(cn *conn) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	freelist := c.freeconn[cn.addr.s]
	if freelist == nil {
		return
	}
	freelist.Put(cn)
}

func (c *Client) getFreeConn(addr *Addr) (*conn, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	freelist := c.freeconn[addr.s]
	if freelist == nil {
		return nil, ErrServerError
	}
	return freelist.Get(), nil
}

// ==================================================================
// ConnectTimeoutError is the error type used when it takes
// too long to connect to the desired host. This level of
// detail can generally be ignored.
type ConnectTimeoutError struct {
	Addr net.Addr
}

func (cte *ConnectTimeoutError) Error() string {
	return "memcache: connect timeout to " + cte.Addr.String()
}

func (cte *ConnectTimeoutError) Timeout() bool {
	return true
}

func (cte *ConnectTimeoutError) Temporary() bool {
	return true
}

func (c *Client) dial(addr *Addr) (net.Conn, error) {
	if c.timeout > 0 {
		conn, err := net.DialTimeout(addr.n, addr.s, c.timeout)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				return nil, &ConnectTimeoutError{addr}
			}
			return nil, err
		}
		return conn, nil
	}
	return net.Dial(addr.n, addr.s)
}

func (c *Client) getNewConn(addr *Addr) (*conn, error) {
	nc, err := c.dial(addr)
	if err != nil {
		return nil, err
	}
	cn := &conn{
		nc:   nc,
		addr: addr,
	}
	if c.timeout > 0 {
		nc.SetDeadline(time.Now().Add(c.timeout))
	}
	if _, ok := nc.(*net.TCPConn); ok {
		err = nc.(*net.TCPConn).SetKeepAlive(true)
		if err != nil {
			nc.Close()
			cn = nil
			return nil, err
		}
		err = nc.(*net.TCPConn).SetKeepAlivePeriod(DefaultKeepAlive)
		if err != nil {
			nc.Close()
			cn = nil
			return nil, err
		}
	}
	return cn, nil
}

// ==================================================================
// Get gets the item for the given key. ErrCacheMiss is returned for a
// memcache cache miss. The key must be at most 250 bytes in length.
func (c *Client) Get(key string) (*Item, error) {
	cn, err := c.sendCommand(key, cmdGet, nil, 0, nil)
	if err != nil {
		c.condRelease(cn, &err)
		return nil, err
	}
	return c.parseItemResponse(key, cn, true)
}

func (c *Client) sendCommand(key string, cmd command, value []byte, casid uint64, extras []byte) (*conn, error) {
	// check key length
	if !legalKey(key) {
		return nil, ErrMalformedKey
	}

	// get connection and send command
	addr, err := c.servers.PickServer(key)
	if err != nil {
		return nil, err
	}
	cn, err := c.getFreeConn(addr)
	if err != nil {
		return nil, err
	}
	err = c.sendConnCommand(cn, key, cmd, value, casid, extras)
	if err != nil {
		// retry once
		cn, err = c.getFreeConn(addr)
		if err != nil {
			return nil, err
		}
		err = c.sendConnCommand(cn, key, cmd, value, casid, extras)
		if err != nil {
			return nil, err
		}
		return cn, err
	}
	return cn, err
}

func (c *Client) sendConnCommand(cn *conn, key string, cmd command, value []byte, casid uint64, extras []byte) (err error) {
	// get buffer from pool
	bufp := c.bufPool.Get()
	buf := *bufp
	buf = buf[:24]
	buf[0] = reqMagic

	// put buffer into pool
	defer func() {
		*bufp = buf
		bufp = c.bufPool.CleanBuf(bufp)
		c.bufPool.Put(bufp)
	}()

	// Command (1)
	buf[1] = byte(cmd)
	kl := len(key)
	el := len(extras)

	// Key length (2-3)
	putUint16(buf[2:], uint16(kl))

	// Extras length (4)
	buf[4] = byte(el)

	// Data type (5), always zero
	// VBucket (6-7), always zero
	// Total body length (8-11)
	vl := len(value)
	bl := uint32(kl + el + vl)
	putUint32(buf[8:], bl)

	// Opaque (12-15), always zero
	// CAS (16-23)
	putUint64(buf[16:], casid)

	// Extras
	if el > 0 {
		buf = append(buf, extras...)
	}
	if kl > 0 {
		// Key itself
		buf = append(buf, stobs(key)...)
	}
	writer := _writerPool.Get().(*bufio.Writer)
	defer _writerPool.Put(writer)
	writer.Reset(cn.nc)
	if _, err = writer.Write(buf); err != nil {
		return err
	}
	if vl > 0 {
		if _, err = writer.Write(value); err != nil {
			return err
		}
	}
	if err = writer.Flush(); err != nil {
		return err
	}
	return nil
}

func (c *Client) parseResponse(rKey string, cn *conn) ([]byte, []byte, []byte, []byte, error) {
	var err error

	// get buffer from pool
	hdrp := c.bufPool.Get()
	hdr := *hdrp
	hdr = hdr[:24]

	// put buffer into pool
	defer func() {
		*hdrp = hdr
		hdrp = c.bufPool.CleanBuf(hdrp)
		c.bufPool.Put(hdrp)
	}()

	if err = readAtLeast(cn.nc, hdr, 24); err != nil {
		return nil, nil, nil, nil, err
	}
	if hdr[0] != respMagic {
		return nil, nil, nil, nil, ErrBadMagic
	}
	total := int(bUint32(hdr[8:12]))
	status := bUint16(hdr[6:8])
	if status != respOk {
		if _, err = io.CopyN(ioutil.Discard, cn.nc, int64(total)); err != nil {
			return nil, nil, nil, nil, err
		}
		if status == respInvalidArgs && !legalKey(rKey) {
			return nil, nil, nil, nil, ErrMalformedKey
		}
		return nil, nil, nil, nil, response(status).asError()
	}

	var bp int
	var bufLen int = total + len(hdr)
	var extras, key, value, valBuf []byte = nil, nil, nil, nil
	valBuf = make([]byte, bufLen, bufLen)
	if el := int(hdr[4]); el > 0 {
		extras = valBuf[0:el]
		bp += el
		if err = readAtLeast(cn.nc, extras, el); err != nil {
			return nil, nil, nil, nil, err
		}
	}
	if kl := int(bUint16(hdr[2:4])); kl > 0 {
		key = valBuf[bp : bp+kl]
		bp += kl
		if err = readAtLeast(cn.nc, key, kl); err != nil {
			return nil, nil, nil, nil, err
		}
	}
	if vl := total - bp; vl > 0 {
		value = valBuf[bp:total]
		if err = readAtLeast(cn.nc, value, vl); err != nil {
			return nil, nil, nil, nil, err
		}
	}
	copy(valBuf[total:], hdr)
	return valBuf[total:], key, extras, value, nil
}

func (c *Client) parseUintResponse(key string, cn *conn) (uint64, error) {
	_, _, _, value, err := c.parseResponse(key, cn)
	c.condRelease(cn, &err)
	if err != nil {
		return 0, err
	}
	return bUint64(value), nil
}

func (c *Client) parseItemResponse(key string, cn *conn, release bool) (*Item, error) {
	hdr, k, extras, value, err := c.parseResponse(key, cn)
	if release {
		c.condRelease(cn, &err)
	}
	if err != nil {
		return nil, err
	}
	var flags uint32
	if len(extras) > 0 {
		flags = bUint32(extras)
	}
	if key == "" && len(k) > 0 {
		key = string(k)
	}
	return &Item{
		Key:   key,
		Value: value,
		Flags: flags,
		casid: bUint64(hdr[16:24]),
	}, nil
}

// ==================================================================
// GetMulti is a batch version of Get. The returned map from keys to
// items may have fewer elements than the input slice, due to memcache
// cache misses. Each key must be at most 250 bytes in length.
// If no error is returned, the returned map will also be non-nil.
func (c *Client) GetMulti(keys []string) (map[string]*Item, error) {
	keyMap := make(map[*Addr][]string)
	for _, key := range keys {
		addr, err := c.servers.PickServer(key)
		if err != nil {
			return nil, err
		}
		keyMap[addr] = append(keyMap[addr], key)
	}

	var chs []chan *Item
	for addr, keys := range keyMap {
		ch := make(chan *Item)
		chs = append(chs, ch)
		go func(addr *Addr, keys []string, ch chan *Item) {
			defer close(ch)
			cn, err := c.getFreeConn(addr)
			defer c.condRelease(cn, &err)
			if err != nil {
				return
			}
			for _, k := range keys {
				if err = c.sendConnCommand(cn, k, cmdGetKQ, nil, 0, nil); err != nil {
					return
				}
			}
			if err = c.sendConnCommand(cn, "", cmdNoop, nil, 0, nil); err != nil {
				return
			}
			var item *Item
			for {
				item, err = c.parseItemResponse("", cn, false)
				if item == nil || item.Key == "" {
					// Noop response
					break
				}
				ch <- item
			}
		}(addr, keys, ch)
	}

	m := make(map[string]*Item)
	for _, ch := range chs {
		for item := range ch {
			m[item.Key] = item
		}
	}
	return m, nil
}

// ==================================================================
// Set writes the given item, unconditionally.
func (c *Client) Set(item *Item) error {
	return c.populateOne(cmdSet, item, 0)
}

// ==================================================================
// Add writes the given item, if no value already exists for its
// key. ErrNotStored is returned if that condition is not met.
func (c *Client) Add(item *Item) error {
	return c.populateOne(cmdAdd, item, 0)
}

// ==================================================================
// CompareAndSwap writes the given item that was previously returned
// by Get, if the value was neither modified or evicted between the
// Get and the CompareAndSwap calls. The item's Key should not change
// between calls but all other item fields may differ. ErrCASConflict
// is returned if the value was modified in between the
// calls. ErrNotStored is returned if the value was evicted in between
// the calls.
func (c *Client) CompareAndSwap(item *Item) error {
	return c.populateOne(cmdSet, item, item.casid)
}

func (c *Client) populateOne(cmd command, item *Item, casid uint64) error {
	// get buffer from pool
	extp := c.bufPool.Get()
	extras := *extp
	extras = extras[:8]

	// put buffer into pool
	defer func() {
		extp = c.bufPool.CleanBuf(extp)
		c.bufPool.Put(extp)
	}()

	putUint32(extras, item.Flags)
	putUint32(extras[4:8], uint32(item.Expiration))
	cn, err := c.sendCommand(item.Key, cmd, item.Value, casid, extras)
	if err != nil {
		c.condRelease(cn, &err)
		return err
	}
	hdr, _, _, _, err := c.parseResponse(item.Key, cn)
	if err != nil {
		c.condRelease(cn, &err)
		return err
	}
	c.putFreeConn(cn)
	item.casid = bUint64(hdr[16:24])
	return nil
}

// ==================================================================
// Delete deletes the item with the provided key. The error ErrCacheMiss is
// returned if the item didn't already exist in the cache.
func (c *Client) Delete(key string) error {
	cn, err := c.sendCommand(key, cmdDelete, nil, 0, nil)
	if err != nil {
		c.condRelease(cn, &err)
		return err
	}
	_, _, _, _, err = c.parseResponse(key, cn)
	c.condRelease(cn, &err)
	return err
}

// ==================================================================
// Increment atomically increments key by delta. The return value is
// the new value after being incremented or an error. If the value
// didn't exist in memcached the error is ErrCacheMiss. The value in
// memcached must be an decimal number, or an error will be returned.
// On 64-bit overflow, the new value wraps around.
func (c *Client) Increment(key string, delta uint64) (newValue uint64, err error) {
	return c.incrDecr(cmdIncr, key, delta)
}

// ==================================================================
// Decrement atomically decrements key by delta. The return value is
// the new value after being decremented or an error. If the value
// didn't exist in memcached the error is ErrCacheMiss. The value in
// memcached must be an decimal number, or an error will be returned.
// On underflow, the new value is capped at zero and does not wrap
// around.
func (c *Client) Decrement(key string, delta uint64) (newValue uint64, err error) {
	return c.incrDecr(cmdDecr, key, delta)
}

func (c *Client) incrDecr(cmd command, key string, delta uint64) (uint64, error) {
	// get buffer from pool
	extp := c.bufPool.Get()
	extras := *extp
	extras = extras[:20]

	// put buffer into pool
	defer func() {
		extp = c.bufPool.CleanBuf(extp)
		c.bufPool.Put(extp)
	}()

	putUint64(extras, delta)
	// Set expiration to 0xfffffff, so the command fails if the key
	// does not exist.
	for ii := 16; ii < 20; ii++ {
		extras[ii] = 0xff
	}
	cn, err := c.sendCommand(key, cmd, nil, 0, extras)
	if err != nil {
		c.condRelease(cn, &err)
		return 0, err
	}
	return c.parseUintResponse(key, cn)
}

// ==================================================================
// Flush removes all the items in the cache after expiration seconds. If
// expiration is <= 0, it removes all the items right now.
func (c *Client) Flush(expiration int) error {
	servers, err := c.servers.Servers()
	var failed []*Addr
	var errs []error
	if err != nil {
		return err
	}
	var extras []byte
	if expiration > 0 {
		extras = make([]byte, 4)
		putUint32(extras, uint32(expiration))
	}
	for _, addr := range servers {
		cn, err := c.getFreeConn(addr)
		if err != nil {
			failed = append(failed, addr)
			errs = append(errs, err)
			continue
		}
		if err = c.sendConnCommand(cn, "", cmdFlush, nil, 0, extras); err == nil {
			_, _, _, _, err = c.parseResponse("", cn)
		}
		if err != nil {
			failed = append(failed, addr)
			errs = append(errs, err)
		}
		c.condRelease(cn, &err)
	}
	if len(failed) > 0 {
		var buf bytes.Buffer
		buf.WriteString("failed to flush some servers: ")
		for ii, addr := range failed {
			if ii > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(addr.String())
			buf.WriteString(": ")
			buf.WriteString(errs[ii].Error())
		}
		return errors.New(buf.String())
	}
	return nil
}
