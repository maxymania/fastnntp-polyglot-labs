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
import "github.com/maxymania/fastnntp-polyglot-labs/bufferex"
import "time"
import "github.com/valyala/fasthttp"

import "github.com/maxymania/fastnntp-polyglot-labs/binarix"
import "github.com/maxymania/fastnntp-polyglot/buffer"

import "encoding/base64"
import "sync/atomic"

const (
	statusTemporaryFailure = 900+iota
	statusDiskFailure
)

var codec = base64.RawURLEncoding

func decode(buf []byte,targ []byte) (bufferex.Binary, error){
	l := codec.DecodedLen(len(buf))
	if cap(targ)>l {
		targ = targ[:l]
		n,err := codec.Decode(targ,buf)
		if err!=nil { return bufferex.Binary{},err }
		return bufferex.NewBinaryInplace(targ[:n]),nil
	}
	temp := buffer.Get(l)
	defer buffer.Put(temp)
	n,err := codec.Decode((*temp),buf)
	if err!=nil { return bufferex.Binary{},err }
	return bufferex.NewBinary((*temp)[:n]),nil
}

type BucketShare struct{
	spcLeft  int64
	Store    bucketstore.BucketStore
	signaler chan int
}
const URLDate = "20060102150405"
func NewBucketShare(s bucketstore.BucketStore) *BucketShare {
	bkt := &BucketShare{Store:s,signaler:make(chan int,1)}
	go bkt.Refresher()
	bkt.Wakeup()
	return bkt
}

func (b *BucketShare) Refresher() {
	for {
		<- b.signaler
		
		lng,err := b.Store.FreeStorage()
		if err!=nil { continue }
		atomic.StoreInt64(&b.spcLeft,lng)
	}
}
func (b *BucketShare) Wakeup() {
	select {
		case b.signaler <- 1:
		default:
	}
}
func (b *BucketShare) Handler(ctx *fasthttp.RequestCtx) {
	var idbuf [100]byte
	var numbuf [16]byte
	path := binarix.Iterator{ctx.Path()}
	path.Split('/') // leading '/'
	path.Split('/') // Bucket-ID+'/'
	
	if ctx.IsGet() {
		id,err := decode(path.Split('/'),idbuf[:])
		if err!=nil { ctx.Error("Bad Request",fasthttp.StatusBadRequest) }
		defer id.Free()
		xid := binarix.Atoi(path.Split('/'))
		var bover,bhead,bbody bufferex.Binary
		var pover,phead,pbody *bufferex.Binary
		if (xid&1)==1 { pover = &bover }
		if (xid&2)==2 { phead = &bhead }
		if (xid&4)==4 { pbody = &bbody }
		ok,e := b.Store.Get(id.Bytes(), pover, phead, pbody)
		defer bover.Free()
		defer bhead.Free()
		defer bbody.Free()
		if e!=nil { ctx.Error("Storage error "+e.Error(),fasthttp.StatusInternalServerError); return }
		if !ok { ctx.Error("Not found",fasthttp.StatusNotFound); return }
		ctx.Response.Header.SetBytesV("X-Over",binarix.Itoa(int64(len(bover.Bytes())),numbuf[:0]))
		ctx.Response.Header.SetBytesV("X-Head",binarix.Itoa(int64(len(bhead.Bytes())),numbuf[:0]))
		ctx.Response.Header.SetBytesV("X-Body",binarix.Itoa(int64(len(bbody.Bytes())),numbuf[:0]))
		ctx.Write(bover.Bytes())
		ctx.Write(bhead.Bytes())
		ctx.Write(bbody.Bytes())
		return
	}
	if ctx.IsPut() {
		id,err := decode(path.Split('/'),idbuf[:])
		if err!=nil { ctx.Error("Bad Request",fasthttp.StatusBadRequest) ; return }
		expire,err := time.ParseInLocation(URLDate,string(path.Split('/')),time.UTC)
		if err!=nil { ctx.Error("Bad Request",fasthttp.StatusBadRequest) ; return }
		defer id.Free()
		overl := binarix.Atoi(ctx.Request.Header.Peek("X-Over"))
		headl := binarix.Atoi(ctx.Request.Header.Peek("X-Head"))
		bodyl := binarix.Atoi(ctx.Request.Header.Peek("X-Body"))
		rdata := ctx.Request.Body()
		if int64(len(rdata))!=(overl+headl+bodyl) {
			ctx.Error("Bad Request",fasthttp.StatusBadRequest)
			return
		}
		bodyf := headl+overl
		lng := atomic.LoadInt64(&b.spcLeft)
		if lng<(overl+headl+bodyl) {
			ctx.Error("Out of Storage Space",fasthttp.StatusInsufficientStorage)
			return
		}
		
		err = b.Store.Put(id.Bytes(), rdata[:overl], rdata[overl:bodyf], rdata[bodyf:], expire)
		if err==bucketstore.EExists {
			ctx.Error("Object Already Exists",fasthttp.StatusConflict)
			return
		}
		if err==bucketstore.EOutOfStorage {
			ctx.Error("Insufficient Storage",fasthttp.StatusInsufficientStorage)
			return
		}
		if err==bucketstore.ETemporaryFailure {
			ctx.Error("Temporary Failure",statusTemporaryFailure)
			return
		}
		if err!=nil {
			ctx.Error("Disk Failure",statusDiskFailure)
			return
		}
		
		atomic.AddInt64(&b.spcLeft,overl+headl+bodyl) // inaccurate update.
		
		b.Wakeup() // Let the background process do it's job
		
		ctx.SetStatusCode(fasthttp.StatusCreated)
		return
	}
	if ctx.IsDelete() { // Expire
		expire,err := time.ParseInLocation(URLDate,string(path.Split('/')),time.UTC)
		if err!=nil { ctx.Error("Bad Request",fasthttp.StatusBadRequest) ; return }
		err = b.Store.Expire(expire)
		if err!=nil {
			ctx.Error("IO Error",fasthttp.StatusInternalServerError)
			return
		}
		ctx.SetStatusCode(fasthttp.StatusNoContent)
		return
	}
	if ctx.IsHead() {
		lng := atomic.LoadInt64(&b.spcLeft)
		ctx.SetStatusCode(fasthttp.StatusNoContent)
		ctx.Response.Header.SetBytesV("X-Free-Storage",binarix.Itoa(lng,numbuf[:0]))
		ctx.SetStatusCode(fasthttp.StatusNoContent)
		return
	}
	ctx.Error("Error 404",404)
}




