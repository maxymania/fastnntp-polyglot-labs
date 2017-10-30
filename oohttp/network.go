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
import "time"

func InitServer(s *fastrpc.Server, hnd fasthttp.RequestHandler) {
	s.SniffHeader = SniffHeader
	s.ProtocolVersion = ProtocolVersion
	s.NewHandlerCtx = NewHandlerCtx
	s.Handler = NewHandler(hnd)
	s.MaxBatchDelay = time.Millisecond * 20
}

type Client struct {
	Inner fastrpc.Client
}
func (c *Client) Init() {
	c.Inner.SniffHeader = SniffHeader
	c.Inner.ProtocolVersion = ProtocolVersion
	c.Inner.NewResponse = NewFakeResponse
	c.Inner.MaxBatchDelay = time.Millisecond * 20
}
func (c *Client) DoDeadline(req *fasthttp.Request, resp *fasthttp.Response, deadline time.Time) error {
	wreq := WrappedRequest{req}
	wresp := WrappedResponse{resp}
	return c.Inner.DoDeadline(&wreq,&wresp,deadline)
}
func (c *Client) PendingRequests() int {
	return c.Inner.PendingRequests()
}

