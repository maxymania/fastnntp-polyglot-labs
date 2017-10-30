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

import "github.com/maxymania/fastnntp-polyglot-labs/bucketstore"
import "sync"
import "sync/atomic"
import "github.com/valyala/fasthttp"
import "github.com/maxymania/fastnntp-polyglot-labs/binarix"
import "time"


type BucketRouter struct{
	locals  map[string]*BucketShare
	remotes map[string]*Client
	remlock sync.Mutex
	uuids   []string
	postick int
}
func NewBucketRouter() *BucketRouter {
	return &BucketRouter{
		remotes: make(map[string]*Client),
		locals: make(map[string]*BucketShare),
	}
}
func (b *BucketRouter) AddLocal(bu *bucketstore.Bucket) {
	if _,ok := b.locals[bu.Uuid] ; ok { return }
	b.locals[bu.Uuid] = NewBucketShare(bu.Store)
	b.uuids = append(b.uuids,bu.Uuid)
}
func (b *BucketRouter) AddNode(names [][]byte,cli HttpClient) {
	b.remlock.Lock(); defer b.remlock.Unlock()
	for _,name := range names {
		sn := string(name)
		b.remotes[sn] = &Client{cli,name}
		b.uuids = append(b.uuids,sn)
	}
}

func (b *BucketRouter) apiSubmit(path binarix.Iterator,ctx *fasthttp.RequestCtx) {
	var idbuf [100]byte
	id,err := decode(path.Split('/'),idbuf[:])
	if err!=nil { ctx.Error("Bad Request",fasthttp.StatusBadRequest) ; return }
	defer id.Free()
	expire,err := time.ParseInLocation(URLDate,string(path.Split('/')),time.UTC)
	if err!=nil { ctx.Error("Bad Request",fasthttp.StatusBadRequest) ; return }
	overl := binarix.Atoi(ctx.Request.Header.Peek("X-Over"))
	headl := binarix.Atoi(ctx.Request.Header.Peek("X-Head"))
	bodyl := binarix.Atoi(ctx.Request.Header.Peek("X-Body"))
	rdata := ctx.Request.Body()
	if int64(len(rdata))!=(overl+headl+bodyl) {
		ctx.Error("Bad Request",fasthttp.StatusBadRequest)
		return
	}
	bodyf := headl+overl
	
	
	uuids := b.uuids
	if len(uuids)==0 {
		ctx.Error("Out of Storage Space",fasthttp.StatusInsufficientStorage)
		return
	}
	for i := 0 ; i<len(uuids) ; i++ {
	
		postick := b.postick
		if postick >= len(uuids) { postick = 0 }
		uuid := uuids[postick]
		b.postick = postick+1
	
		if lc := b.locals[uuid] ; lc!=nil {
			lc.Handler(ctx)
			lng := atomic.LoadInt64(&lc.spcLeft)
			if lng<(overl+headl+bodyl) {
				continue
			}
			
			err = lc.Store.Put(id.Bytes(), rdata[:overl], rdata[overl:bodyf], rdata[bodyf:], expire)
			if err==bucketstore.EExists {
				ctx.Error("Object Already Exists",fasthttp.StatusConflict)
				return
			}
			if err!=nil {
				ctx.Error("Disk Failure",fasthttp.StatusInsufficientStorage)
				return
			}
			atomic.AddInt64(&lc.spcLeft,overl+headl+bodyl) // inaccurate update.
			
			lc.Wakeup() // Let the background process do it's job
			
			ctx.SetStatusCode(fasthttp.StatusCreated)
			ctx.Response.Header.Set("X-Bucket",uuid)
			return
		}
		
		b.remlock.Lock()
		cli := b.remotes[uuid]
		b.remlock.Unlock()
		
		if cli!=nil {
			lng,err := cli.FreeStorage()
			if err!=nil { continue } // Unreachable
			if lng<(overl+headl+bodyl) {
				continue
			}
			
			err = cli.Put(id.Bytes(), rdata[:overl], rdata[overl:bodyf], rdata[bodyf:], expire)
			if err==bucketstore.EExists {
				ctx.Error("Object Already Exists",fasthttp.StatusConflict)
				return
			}
			if err!=nil {
				ctx.Error("Disk Failure",fasthttp.StatusInsufficientStorage)
				return
			}
			
			ctx.SetStatusCode(fasthttp.StatusCreated)
			ctx.Response.Header.Set("X-Bucket",uuid)
			return
		}
	}
	
	ctx.Error("404 No such Bucket!",fasthttp.StatusNotFound)
	return
}
func (b *BucketRouter) Handler(ctx *fasthttp.RequestCtx) {
	path := binarix.Iterator{ctx.Path()}
	path.Split('/') // leading '/'
	buuid := path.Split('/') // Bucket-ID+'/'
	
	switch string(buuid) {
	case "api.submit":
		b.apiSubmit(path,ctx)
		return
	}
	
	if lc := b.locals[string(buuid)] ; lc!=nil {
		lc.Handler(ctx)
		return
	}
	
	b.remlock.Lock()
	cli := b.remotes[string(buuid)]
	b.remlock.Unlock()
	
	
	if cli!=nil {
		if string(ctx.Request.Header.Peek("X-Has-Loop"))=="1" {
			ctx.Error("Loop Detected!",fasthttp.StatusLoopDetected)
			return
		}
		ctx.Request.Header.Set("X-Has-Loop","1")
		cli.client.DoDeadline(&ctx.Request,&ctx.Response,time.Now().Add(time.Second))
	} else {
		ctx.Error("404 No such Bucket!",fasthttp.StatusNotFound)
	}
}
