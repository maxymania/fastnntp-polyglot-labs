/*
Copyright (c) 2017 Simon Schmidt

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

/*
Out-of-Order HTTP-like protocol with keepalive messages/heartbeat.
Useful for use with KCP.
*/
package kcphttp

import (
	"sync"
	"net"
	"github.com/valyala/fasthttp"
	"encoding/binary"
	"time"
	"io"
	"io/ioutil"
	"github.com/valyala/batcher"
	"bufio"
	"bytes"
)

var bE = binary.BigEndian

const (
	heartBeatTimeout = time.Second*2
)

type conn struct{
	dial func() (net.Conn,error)
	nc   net.Conn
	r    *bufio.Reader
	w    *bufio.Writer
	l    sync.Mutex
	wiq  *WIQueue
	bat  batcher.Batcher
}
func (r *conn) close() {
	r.l.Lock(); defer r.l.Unlock()
	nc := r.nc
	r.nc = nil
	if nc!=nil { nc.Close() }
	r.w.Reset(ioutil.Discard)
	r.wiq.EvictAll()
}
func (r *conn) beater(){
	t := time.NewTicker(time.Second/2)
	defer t.Stop()
	var buf [8]byte
	for {
		<- t.C
		r.l.Lock()
		if r.nc==nil { r.l.Unlock() ; return } // stream was closed
		_,err := r.w.Write(buf[:])
		if err==nil { err = r.w.Flush() }
		r.l.Unlock()
		if err!=nil { r.close() ; return }
		r.wiq.EvictExpired()
	}
}
func (r *conn) reader(){
	defer r.close()
	var buf [8]byte
	var resp fasthttp.Response
	for {
		r.nc.SetReadDeadline(fasthttp.CoarseTimeNow().Add(heartBeatTimeout))
		_,err := io.ReadFull(r.r,buf[:])
		if err!=nil { break }
		u := bE.Uint64(buf[:])
		if u==0 { continue }
		wi := r.wiq.Get(u)
		r.nc.SetReadDeadline(time.Time{})
		if wi==nil {
			err = resp.Read(r.r)
		}else{
			err = wi.Resp.Read(r.r)
			wi.Err = err
			wi.WG.Done()
		}
		if err!=nil { break }
	}
}
func (r *conn) write(batch []interface{}){
	var err error
	var buf [8]byte
	r.l.Lock(); defer r.l.Unlock()
	
	if r.nc==nil {
		r.nc,err = r.dial()
		if err==nil {
			r.w.Reset(r.nc)
			r.r.Reset(r.nc)
			go r.reader()
			go r.beater()
		}
	}
	for _,elem := range batch {
		wi := elem.(*WorkItem)
		if err!=nil {
			wi.Err = err
			wi.WG.Done()
			continue
		}
		id := r.wiq.Push(wi)
		if id==0 { wi.Err = ETimeout ; wi.WG.Done(); continue }
		
		bE.PutUint64(buf[:],id)
		
		_,err = r.w.Write(buf[:])
		if err==nil { err = wi.Req.Write(r.w) }
	}
	if err==nil { err = r.w.Flush() }
	
	if err==nil { return } // Done!
	
	
	// Otherwise: error
	r.close()
}

type Client struct{
	conn
}
func NewClient(Dial func() (net.Conn,error)) *Client {
	c := &Client{conn{
		dial:Dial,
		r   :bufio.NewReader(bytes.NewReader([]byte{})),
		w   :bufio.NewWriter(ioutil.Discard),
		wiq :NewWIQueue(),
	}}
	c.bat.Func = c.write
	c.bat.Start()
	return c
}
func (c *Client) DoDeadline(req *fasthttp.Request, resp *fasthttp.Response, deadline time.Time) error {
	var wi WorkItem
	wi.Req  = req
	wi.Resp = resp
	wi.Dead = deadline
	wi.WG.Add(1)
	c.bat.Push(&wi)
	wi.WG.Wait()
	return wi.Err
}
func (c *Client) Destroy() {
	defer c.close()
	c.bat.Stop()
}

