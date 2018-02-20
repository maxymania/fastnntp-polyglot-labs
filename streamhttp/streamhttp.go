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

/* Deprecated: Considered cruft right now. */
package streamhttp

import "github.com/valyala/fasthttp"
import "github.com/byte-mug/golibs/streamprotoc"
import "sync"
import "bufio"
import "net"

import "fmt"

var footer = []byte("Footer\nFooter\nFooter\nFooter\n")

var fakeAddr = &net.TCPAddr{ IP: net.ParseIP("0.0.0.0"), Port: 64000 }

type fakeConn struct{ net.Conn }
func (f fakeConn) LocalAddr() net.Addr { return fakeAddr }
func (f fakeConn) RemoteAddr() net.Addr { return fakeAddr }

type printer struct{}
func (p printer) Printf(format string, args ...interface{}) {
	fmt.Printf(format,args...)
	fmt.Println()
}


type WrappedCtx struct{
	Ctx fasthttp.RequestCtx
}
func (wc *WrappedCtx) WriteMsg(w *bufio.Writer) error {
	err := wc.Ctx.Response.Write(w)
	w.Write(footer)
	return err
}
func (wc *WrappedCtx) ReadMsg(r *bufio.Reader) error {
	//wc.Ctx.Init2(fakeConn{},printer{},false)
	err := wc.Ctx.Request.Read(r)
	return err
}

type WrappedRequest struct{
	Req *fasthttp.Request
}
func (wc *WrappedRequest) WriteMsg(w *bufio.Writer) error {
	err := wc.Req.Write(w)
	w.Write(footer)
	return err
}
func (wc *WrappedRequest) ReadMsg(r *bufio.Reader) error {
	return wc.Req.Read(r)
}

type WrappedResponse struct{
	Resp *fasthttp.Response
}
func (wc *WrappedResponse) WriteMsg(w *bufio.Writer) error {
	err := wc.Resp.Write(w)
	w.Write(footer)
	return err
}
func (wc *WrappedResponse) ReadMsg(r *bufio.Reader) error {
	err := wc.Resp.Read(r)
	return err
}

var msgpool = sync.Pool{New:func() interface{} {
	return &streamprotoc.SendMessage{Inner:new(WrappedCtx)}
}}

func NewMessage () *streamprotoc.SendMessage {
	return msgpool.Get().(*streamprotoc.SendMessage)
}
func NewHandler (hnd fasthttp.RequestHandler) func(r *streamprotoc.SendMessage,c *streamprotoc.ServerConn) {
	return func(r *streamprotoc.SendMessage,c *streamprotoc.ServerConn){
		s := r.Inner.(*WrappedCtx)
		hnd(&s.Ctx)
		lter := s.Ctx.LastTimeoutErrorResponse()
		if lter!=nil {
			nc := new(WrappedCtx)
			lter.CopyTo(&nc.Ctx.Response)
			r.Inner = nc
		}
		defer msgpool.Put(r)
		c.WriteMessage(r)
		if lter==nil {
			s.Ctx.Request.Reset()
			s.Ctx.Response.Reset()
		}
	}
}

