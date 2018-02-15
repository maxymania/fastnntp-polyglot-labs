/*
Copyright (c) 2017-2018 Simon Schmidt

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

import "github.com/maxymania/fastnntp-polyglot-labs/bucketstore/degrader"

type BucketRouter struct{
	locals  map[string]*BucketShare
	remotes map[string]*Client
	remlock sync.Mutex
	uuids   []string
	postick int
	Dmd     *degrader.DegraderMetadata
}
func NewBucketRouter() *BucketRouter {
	return &BucketRouter{
		remotes: make(map[string]*Client),
		locals: make(map[string]*BucketShare),
	}
}
func (b *BucketRouter) AddLocal(bu *bucketstore.Bucket) {
	if _,ok := b.locals[bu.Uuid] ; ok { return }
	bush := NewBucketShare(bu.Store)
	bush.degr.Dmd = b.Dmd
	b.locals[bu.Uuid] = bush
	b.uuids = append(b.uuids,bu.Uuid)
}
func (b *BucketRouter) AddNode(names [][]byte,cli HttpClient) {
	b.remlock.Lock(); defer b.remlock.Unlock()
	for _,name := range names {
		sn := string(name)
		b.remotes[sn] = &Client{cli,name,degrader.Degrader{Dmd:b.Dmd}}
		b.uuids = append(b.uuids,sn)
	}
}
func (b *BucketRouter) AddNode2(names []string,cli HttpClient) {
	b.remlock.Lock(); defer b.remlock.Unlock()
	for _,sn := range names {
		b.remotes[sn] = &Client{cli,[]byte(sn),degrader.Degrader{Dmd:b.Dmd}}
		b.uuids = append(b.uuids,sn)
	}
}
func (b *BucketRouter) Remove2(names []string) {
	b.remlock.Lock(); defer b.remlock.Unlock()
	m := make(map[string]bool,len(names))
	for _,name := range names {
		m[name]=true
		delete(b.remotes,name)
	}
	i := 0
	for _,uuid := range b.uuids {
		if m[uuid] { continue }
		b.uuids[i] = uuid
		i++
	}
	b.uuids = b.uuids[:i]
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
			
			/* Check, if our bucket had a failure recently. */
			if lc.degr.Damaged() { continue }
			
			/* Check Length. We must not exceed the available storage space. */
			lng := atomic.LoadInt64(&lc.spcLeft)
			if lng<(overl+headl+bodyl) {
				continue
			}
			
			err = lc.Store.Put(id.Bytes(), rdata[:overl], rdata[overl:bodyf], rdata[bodyf:], expire)
			if err==bucketstore.EExists {
				ctx.Error("Object Already Exists",fasthttp.StatusConflict)
				return
			}
			if err==bucketstore.EOutOfStorage {
				/* Out of storage: set the Out of Storage variable to ZERO. */
				atomic.StoreInt64(&lc.spcLeft,0)
				continue
			}
			if err==bucketstore.ETemporaryFailure {
				lc.degr.ForceFail()
				continue
			}
			if err!=nil {
				lc.degr.Fail()
				continue
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
			/* Check, if our bucket had a failure recently. */
			if cli.degr.Damaged() { continue }
			
			lng,err := cli.FreeStorage()
			if err!=nil { // Unreachable
				cli.degr.Fail()
				continue
			}
			
			if lng<(overl+headl+bodyl) {
				continue
			}
			
			err = cli.Put(id.Bytes(), rdata[:overl], rdata[overl:bodyf], rdata[bodyf:], expire)
			if err==bucketstore.EExists {
				ctx.Error("Object Already Exists",fasthttp.StatusConflict)
				return
			}
			if err==bucketstore.EOutOfStorage {
				/* We augment Out Of Storage errors by telling the degrader "broken". */
				cli.degr.ForceFail()
				continue
			}
			if err==bucketstore.ETemporaryFailure {
				cli.degr.ForceFail()
				continue
			}
			if err!=nil {
				cli.degr.Fail()
				continue
			}
			
			ctx.SetStatusCode(fasthttp.StatusCreated)
			ctx.Response.Header.Set("X-Bucket",uuid)
			return
		}
	}
	
	/*
	 * If we looped through all Nodes and then determined, that no one fits: Out of Storage.
	 */
	ctx.Error("Insufficient Storage",fasthttp.StatusInsufficientStorage)
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
