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


package oohttp

import "github.com/valyala/fastrpc"
import "github.com/valyala/fasthttp"
import "bufio"
import "net"

const SniffHeader = "OOHttp"
const ProtocolVersion byte = 1

var _ fastrpc.HandlerCtx = &WrappedCtx{}

func NewHandlerCtx() fastrpc.HandlerCtx { return new(WrappedCtx) }

type WrappedCtx struct{
	Ctx fasthttp.RequestCtx
}
func (w *WrappedCtx) ConcurrencyLimitError(concurrency int) {
	w.Ctx.Error("Concurrency Error",fasthttp.StatusTooManyRequests)
}

func (w *WrappedCtx) Init(conn net.Conn, logger fasthttp.Logger) {
	// Clear request and response
	w.Ctx.Request.Reset()
	w.Ctx.Response.Reset()
	
	// Re-Init.
	w.Ctx.Init2(conn, logger,false)
}

func (w *WrappedCtx) ReadRequest(br *bufio.Reader) error {
	return w.Ctx.Request.Read(br)
}

func (w *WrappedCtx) WriteResponse(bw *bufio.Writer) error {
	return w.Ctx.Response.Write(bw)
}

func NewHandler(hnd fasthttp.RequestHandler) func(ctx fastrpc.HandlerCtx) fastrpc.HandlerCtx {
	return func(ctx fastrpc.HandlerCtx) fastrpc.HandlerCtx{
		w := ctx.(*WrappedCtx)
		hnd(&w.Ctx)
		lter := w.Ctx.LastTimeoutErrorResponse()
		if lter!=nil {
			nc := new(WrappedCtx)
			lter.CopyTo(&nc.Ctx.Response)
			return nc
		}
		return ctx
	}
}

var _ fastrpc.RequestWriter = &WrappedRequest{}

type WrappedRequest struct{
	Req *fasthttp.Request
}
func (w *WrappedRequest) WriteRequest(bw *bufio.Writer) error {
	return w.Req.Write(bw)
}

var _ fastrpc.ResponseReader = &WrappedResponse{}

type WrappedResponse struct{
	Resp *fasthttp.Response
}
func (w *WrappedResponse) ReadResponse(br *bufio.Reader) error {
	return w.Resp.Read(br)
}

type fakeResponse struct{}

func (f fakeResponse) ReadResponse(br *bufio.Reader) error {
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	return resp.Read(br)
}

var fakeResponseObj fastrpc.ResponseReader = &fakeResponse{}

func NewFakeResponse() fastrpc.ResponseReader { return fakeResponseObj }

