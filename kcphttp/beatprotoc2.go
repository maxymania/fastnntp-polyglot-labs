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


package kcphttp

import (
	"sync"
	"github.com/valyala/fasthttp"
	"bufio"
	"io/ioutil"
	"io"
	"time"
	"net"
	"github.com/valyala/batcher"
)

type serverWorkItem struct{
	ctx fasthttp.RequestCtx
	rid uint64
}

var swiPool = sync.Pool{ New: func()interface{}{ return new(serverWorkItem) } }
func recycleSwi(swi *serverWorkItem) {
	swi.ctx.Request.Reset()
	swi.ctx.Response.Reset()
	swiPool.Put(swi)
}

type server struct{
	nc net.Conn
	r  *bufio.Reader
	w  *bufio.Writer
	l    sync.Mutex
	bl   sync.RWMutex
	handler func(*server,*serverWorkItem)
	batch   batcher.Batcher
	batchOn bool
}
func (s *server) batchOff() {
	s.bl.Lock(); defer s.bl.Unlock()
	if !s.batchOn { return }
	s.batchOn = false
	s.batch.Stop()
}
func (s *server) close() {
	s.l.Lock(); defer s.l.Unlock()
	nc := s.nc
	s.nc = nil
	if nc!=nil { nc.Close() }
	s.w.Reset(ioutil.Discard)
	s.batchOff()
}
func (s *server) respond(swi *serverWorkItem) {
	s.bl.RLock(); defer s.bl.RUnlock()
	if !s.batchOn { return }
	s.batch.Push(swi)
}
func (s *server) respondBatch(batch []interface{}) {
	var err error
	var buf [8]byte
	s.l.Lock(); defer s.l.Unlock()
	for _,elem := range batch {
		swi := elem.(*serverWorkItem)
		// If we had timeout, this context is wasted. Also, do not respond!
		if swi.ctx.LastTimeoutErrorResponse()!=nil { continue }
		
		bE.PutUint64(buf[:],swi.rid)
		if err==nil { _,err = s.w.Write(buf[:]) }
		if err==nil { err = swi.ctx.Response.Write(s.w) }
		recycleSwi(swi)
	}
	if err==nil { err = s.w.Flush() }
	if err==nil { return }
	
	// On Error, close the connection.
	nc := s.nc
	s.nc = nil
	if nc!=nil { nc.Close() }
	s.w.Reset(ioutil.Discard)
	s.batchOff()
}
func (s *server) reader(){
	defer s.close()
	var buf [8]byte
	for {
		s.nc.SetReadDeadline(fasthttp.CoarseTimeNow().Add(heartBeatTimeout))
		_,err := io.ReadFull(s.r,buf[:])
		if err!=nil { break }
		u := bE.Uint64(buf[:])
		if u==0 { continue } // Heart-Beat.
		s.nc.SetReadDeadline(time.Time{})
		swi := swiPool.Get().(*serverWorkItem)
		swi.rid = u
		err = swi.ctx.Request.Read(s.r)
		if err!=nil { break }
		go s.handler(s,swi)
	}
}
func (r *server) heartbeat() error {
	var buf [8]byte
	r.l.Lock(); defer r.l.Unlock()
	if r.nc==nil { return io.EOF } // stream was closed
	_,err := r.w.Write(buf[:])
	if err==nil { err = r.w.Flush() }
	return err
}
func (r *server) beater(){
	t := time.NewTicker(time.Second/2)
	defer t.Stop()
	for {
		<- t.C
		err := r.heartbeat()
		if err!=nil { r.close() ; return }
	}
}
func newHandler(hn fasthttp.RequestHandler) func(*server,*serverWorkItem) {
	return func(srv *server,swi *serverWorkItem) {
		defer srv.respond(swi)
		hn(&swi.ctx)
	}
}


type Server struct{
	handler func(*server,*serverWorkItem)
}
func NewServer(hn fasthttp.RequestHandler) *Server { return &Server{newHandler(hn)} }
func (sr *Server) Handle(c net.Conn) {
	s := &server{
		nc      :c,
		r       :bufio.NewReader(c),
		w       :bufio.NewWriter(c),
		handler :sr.handler,
	}
	s.batchOn = true
	s.batch.Func = s.respondBatch
	s.batch.Start()
	go s.reader()
	s.beater()
}

