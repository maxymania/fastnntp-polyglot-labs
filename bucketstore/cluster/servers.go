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


package cluster

import "net"
import "github.com/valyala/fasthttp"
import "github.com/valyala/fastrpc"
import "github.com/maxymania/fastnntp-polyglot-labs/oohttp"
import "fmt"
import "github.com/maxymania/fastnntp-polyglot-labs/kcphttp"
import "github.com/xtaci/kcp-go"

func lasHttp (md *MetaData,f fasthttp.RequestHandler) {
	l,e := net.Listen("tcp",fmt.Sprintf(":%d",md.Port))
	if e!=nil { return }
	fasthttp.Serve(l,f)
}
func lasOO (md *MetaData,f fasthttp.RequestHandler) {
	l,e := net.Listen("tcp",fmt.Sprintf(":%d",md.Port))
	if e!=nil { return }
	s := new(fastrpc.Server)
	oohttp.InitServer(s, f)
	s.Serve(l)
}

func lasKCP (md *MetaData,f fasthttp.RequestHandler) {
	var cipher kcp.BlockCrypt
	if len(md.KCP.Salsa20Key)>0 {
		cipher,_ = kcp.NewSalsa20BlockCrypt(md.KCP.Salsa20Key)
	}
	l,e := kcp.ListenWithOptions(fmt.Sprintf(":%d",md.Port),cipher, md.KCP.DataShards, md.KCP.ParityShards)
	if e!=nil { return }
	srv := kcphttp.NewServer(f)
	tm := md.KCP.TurboMode
	for {
		c,e := l.AcceptKCP()
		if e!=nil {
			continue
		}
		if tm { c.SetNoDelay(1,40,1,1) }
		go srv.Handle(c)
	}
}

