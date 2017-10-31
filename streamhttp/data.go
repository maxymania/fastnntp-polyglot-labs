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


package streamhttp

import "github.com/valyala/fasthttp"
import "github.com/byte-mug/golibs/streamprotoc"
import "net"
import "time"
import "github.com/xtaci/kcp-go"

type Client struct{
	Inner *streamprotoc.Client
}
// Dials with kcp-go
func Dial(addr string) (*Client,error) {
	conn,err := kcp.DialWithOptions(addr, nil, 10, 3)
	if err!=nil { return nil,err }
	return &Client{streamprotoc.NewClient(conn)},nil
}

// Dials with TCP
func DialTCP(addr string) (*Client,error) {
	conn,err := net.Dial("tcp",addr)
	if err!=nil { return nil,err }
	return &Client{streamprotoc.NewClient(conn)},nil
}

func (c *Client) DoDeadline(req *fasthttp.Request, resp *fasthttp.Response, deadline time.Time) error {
	wreq := WrappedRequest{req}
	wresp := WrappedResponse{resp}
	return c.Inner.DoDeadline(&wreq,&wresp,deadline)
}

type Server struct{
	streamprotoc.Server
}
func NewServer(hnd fasthttp.RequestHandler) *Server{
	s := new(Server)
	s.NewMessage = NewMessage
	s.Handler    = NewHandler(hnd)
	return s
}
func (s *Server) Serve(l net.Listener){
	for {
		c,e := l.Accept()
		if e!=nil {
			time.Sleep(time.Second)
			continue
		}
		go s.Handle(c)
	}
}
func (s *Server) ListenAndServe(addr string) error {
	l,e := kcp.ListenWithOptions(addr, nil, 10, 3)
	if e!=nil { return e }
	s.Serve(l)
	return nil
}
func (s *Server) ListenAndServeTCP(addr string) error {
	l,e := net.Listen("tcp",addr)
	if e!=nil { return e }
	s.Serve(l)
	return nil
}
