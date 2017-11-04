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
import "github.com/maxymania/fastnntp-polyglot-labs/bucketstore/remote"
import "github.com/maxymania/fastnntp-polyglot-labs/oohttp"
import "github.com/valyala/fasthttp"
import "github.com/maxymania/fastnntp-polyglot-labs/kcphttp"
import "github.com/xtaci/kcp-go"

func dial6(s string) (net.Conn,error) { return net.Dial("tcp6",s) }

func ccHttp(md *MetaData,ip net.IP) remote.HttpClient {
	addr := (&net.TCPAddr{IP:ip,Port:int(md.Port)}).String()
	hc := &fasthttp.HostClient{Addr: addr}
	
	// If the address is an IPv6 address, Use IPv6-Dial. By default fasthttp uses IPv4 only.
	if len(ip.To4())==0 { hc.Dial = dial6 }
	return hc
}

func ccOO(md *MetaData,ip net.IP) remote.HttpClient {
	addr := (&net.TCPAddr{IP:ip,Port:int(md.Port)}).String()
	oc := &oohttp.Client{}
	oc.Inner.Addr = addr
	
	// If the address is an IPv6 address, Use IPv6-Dial. By default fastrpc uses IPv4 only, just like fasthttp.
	if len(ip.To4())==0 { oc.Inner.Dial = dial6 }
	return oc
}

func kcpDialer(md *MetaData, ip net.IP) func() (net.Conn,error) {
	addr := (&net.TCPAddr{IP:ip,Port:int(md.Port)}).String()
	return func() (net.Conn,error) {
		var cipher kcp.BlockCrypt
		if len(md.KCP.Salsa20Key)>0 {
			cipher,_ = kcp.NewSalsa20BlockCrypt(md.KCP.Salsa20Key)
		}
		ce,err := kcp.DialWithOptions(addr, cipher, md.KCP.DataShards, md.KCP.ParityShards)
		if ce!=nil && md.KCP.TurboMode { ce.SetNoDelay(1,40,1,1) }
		return ce,err
	}
}

func ccKcp(md *MetaData,ip net.IP) remote.HttpClient {
	kc := kcphttp.NewClient(kcpDialer(md,ip))
	return kc
}


