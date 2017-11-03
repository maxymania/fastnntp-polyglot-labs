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


package remote

import "time"
import "github.com/vmihailenco/msgpack"
import "github.com/valyala/fasthttp"
import "github.com/maxymania/fastnntp-polyglot/groups"

import "github.com/maxymania/fastnntp-polyglot-labs/binarix"

import "fmt"

type GhaService struct{
	Actor *groups.GroupHeadActor
}
func (g *GhaService) Handler(ctx *fasthttp.RequestCtx) {
	var numbuf [16]byte
	var idbuf [256]byte
	path := binarix.Iterator{ctx.Path()}
	path.Split('/') // leading '/'
	cmd := path.Split('/') // Command+'/'
	
	var groups [][]byte
	var inums []int64
	var id []byte
	switch string(cmd) {
	case "AdmCreateGroup":
		id = idbuf[:]
		if msgpack.Unmarshal(ctx.Request.Body(),&id)!=nil { ctx.Response.Header.Set("X-Result","4") ; return }
		ctx.Response.Header.SetBytesV("X-Result",binarix.Itoa(int64(g.Actor.AdmCreateGroup(id)),numbuf[:0]))
	case "GetDown":
		id = idbuf[:]
		if msgpack.Unmarshal(ctx.Request.Body(),&id)!=nil { ctx.Error("Error",404) ; return }
		n,e := g.Actor.GetDown(id)
		if e!=nil { ctx.Error(e.Error(),404) ; return }
		ctx.Response.Header.SetBytesV("X-Result",binarix.Itoa(n,numbuf[:0]))
	case "GroupHeadInsert":
		if msgpack.Unmarshal(ctx.Request.Body(),&groups)!=nil { ctx.Error("Error",404) ; return }
		nums,e := g.Actor.GroupHeadInsert(groups,nil)
		if e!=nil { ctx.Error(e.Error(),404) ; return }
		msgpack.NewEncoder(ctx).Encode(nums)
	case "GroupHeadRevert":
		if msgpack.Unmarshal(ctx.Request.Body(),&groups,&inums)!=nil { ctx.Error("Error",404) ; return }
		e := g.Actor.GroupHeadRevert(groups,inums)
		if e!=nil { ctx.Error(e.Error(),404) ; return }
	case "MoveDown":
		id = idbuf[:]
		if msgpack.Unmarshal(ctx.Request.Body(),&id)!=nil { ctx.Error("Error",404) ; return }
		n,e := g.Actor.MoveDown(id)
		if e!=nil { ctx.Error(e.Error(),404) ; return }
		ctx.Response.Header.SetBytesV("X-Result",binarix.Itoa(n,numbuf[:0]))
	case "UpdateDown":
		id = idbuf[:]
		var oldHigh,low,high,count int64
		if msgpack.Unmarshal(ctx.Request.Body(),&id,oldHigh,low,high,count)!=nil { ctx.Error("Error",404) ; return }
		b,e := g.Actor.UpdateDown(id, oldHigh, low, high, count)
		if e!=nil { ctx.Error(e.Error(),404) ; return }
		if b {
			ctx.Response.Header.Set("X-Result","1")
		} else {
			ctx.Response.Header.Set("X-Result","0")
		}
	default: ctx.Error("No such service",404)
	}
}

type HttpClient interface {
	DoDeadline(req *fasthttp.Request, resp *fasthttp.Response, deadline time.Time) error
}

func prep(req *fasthttp.Request) {
	req.SetHost("unknown")
	req.Header.SetMethod("POST")
}

type writer struct{
	*fasthttp.Request
}
func (w writer) Write(p []byte) (int,error) {
	w.AppendBody(p)
	return len(p),nil
}

type GhaClient struct{
	Client HttpClient
}

func (g *GhaClient) AdmCreateGroup(group []byte) int {
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	req.SetRequestURI("/AdmCreateGroup/")
	prep(req)
	msgpack.NewEncoder(writer{req}).Encode(group)
	err := g.Client.DoDeadline(req,resp,fasthttp.CoarseTimeNow().Add(time.Second*2))
	if err!=nil { return 3 }
	
	fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	
	return int(binarix.Atoi(resp.Header.Peek("X-Result")))
}

func (g *GhaClient) GetDown(group []byte) (int64, error) {
	req := fasthttp.AcquireRequest()
	prep(req)
	resp := fasthttp.AcquireResponse()
	req.SetRequestURI("/GetDown/")
	msgpack.NewEncoder(writer{req}).Encode(group)
	err := g.Client.DoDeadline(req,resp,fasthttp.CoarseTimeNow().Add(time.Second*2))
	if err!=nil { return 0,err }
	
	fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	
	if resp.StatusCode()!=200 { return 0,fmt.Errorf("Remote: %s",resp.Body()) }
	
	return binarix.Atoi(resp.Header.Peek("X-Result")),nil
}
func (g *GhaClient) MoveDown(group []byte) (int64, error) {
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	req.SetRequestURI("/MoveDown/")
	prep(req)
	msgpack.NewEncoder(writer{req}).Encode(group)
	err := g.Client.DoDeadline(req,resp,fasthttp.CoarseTimeNow().Add(time.Second*2))
	if err!=nil { return 0,err }
	
	fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	
	if resp.StatusCode()!=200 { return 0,fmt.Errorf("Remote: %s",resp.Body()) }
	
	return binarix.Atoi(resp.Header.Peek("X-Result")),nil
}
func (g *GhaClient) UpdateDown(group []byte, oldHigh, low, high, count int64) (bool, error) {
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	req.SetRequestURI("/UpdateDown/")
	prep(req)
	msgpack.NewEncoder(writer{req}).Encode(group,oldHigh, low, high, count)
	err := g.Client.DoDeadline(req,resp,fasthttp.CoarseTimeNow().Add(time.Second*2))
	if err!=nil { return false,err }
	
	fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	
	if resp.StatusCode()!=200 { return false,fmt.Errorf("Remote: %s",resp.Body()) }
	
	return binarix.Atoi(resp.Header.Peek("X-Result"))==1,nil
}
func (g *GhaClient) GroupHeadInsert(groups [][]byte, buf []int64) ([]int64, error) {
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	req.SetRequestURI("/GroupHeadInsert/")
	prep(req)
	msgpack.NewEncoder(writer{req}).Encode(groups)
	
	err := g.Client.DoDeadline(req,resp,fasthttp.CoarseTimeNow().Add(time.Second*2))
	if err!=nil { return nil,err }
	
	fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	
	if resp.StatusCode()!=200 { return nil,fmt.Errorf("Remote: %s",resp.Body()) }
	msgpack.Unmarshal(resp.Body(),&buf)
	
	return buf,nil
}
func (g *GhaClient) GroupHeadRevert(groups [][]byte, nums []int64) error {
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	req.SetRequestURI("/GroupHeadRevert/")
	prep(req)
	msgpack.NewEncoder(writer{req}).Encode(groups,nums)
	err := g.Client.DoDeadline(req,resp,fasthttp.CoarseTimeNow().Add(time.Second*2))
	if err!=nil { return err }
	
	fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	
	if resp.StatusCode()!=200 { return fmt.Errorf("Remote: %s",resp.Body()) }
	
	return nil
}

