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

import "time"
import "github.com/valyala/fasthttp"
import "github.com/maxymania/fastnntp-polyglot-labs/bucketstore"
import "github.com/maxymania/fastnntp-polyglot/buffer"
import "github.com/maxymania/fastnntp-polyglot-labs/bufferex"
import "github.com/maxymania/fastnntp-polyglot-labs/binarix"
import "fmt"
import "github.com/maxymania/fastnntp-polyglot-labs/bucketstore/degrader"

type HttpClient interface{
	DoDeadline(req *fasthttp.Request, resp *fasthttp.Response, deadline time.Time) error
}

func required(req *fasthttp.Request) {
	req.SetHost("unknown")
}

type Client struct{
	client HttpClient
	uuid   []byte
	degr   degrader.Degrader
}
func NewClient(c HttpClient, uuid []byte) *Client {
	return &Client{c,uuid,degrader.Degrader{}}
}

func (c *Client) setUrl(req *fasthttp.Request, id []byte, date []byte) {
	encl := codec.EncodedLen(len(id))
	n := 1+len(c.uuid)+1+encl+1+len(date)
	buf := buffer.Get(n)
	defer buffer.Put(buf)
	uri := append(append(append((*buf)[:0],'/'),c.uuid...),'/')
	uri = uri[:len(uri)+encl]
	codec.Encode(uri[len(uri)-encl:],id)
	if len(date)>0 { uri = append(append(uri,'/'),date...) }
	req.SetRequestURIBytes(uri)
}

func (c *Client) Put(id, overv, head, body []byte, expire time.Time) error {
	var buf [32]byte
	var numbuf [16]byte
	
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	
	c.setUrl(req,id,expire.AppendFormat(buf[:0],URLDate))
	req.Header.SetMethod("PUT")
	
	req.Header.SetBytesV("X-Over",binarix.Itoa(int64(len(overv)),numbuf[:0]))
	req.Header.SetBytesV("X-Head",binarix.Itoa(int64(len(head )),numbuf[:0]))
	req.Header.SetBytesV("X-Body",binarix.Itoa(int64(len(body )),numbuf[:0]))
	
	req.AppendBody(overv)
	req.AppendBody(head)
	req.AppendBody(body)
	
	required(req)
	err := c.client.DoDeadline(req,resp,time.Now().Add(time.Second))
	
	if err!=nil { return err }
	
	fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	
	switch resp.StatusCode() {
	case fasthttp.StatusBadRequest:          return bucketstore.EBadRequest
	case fasthttp.StatusCreated:             return nil
	case fasthttp.StatusConflict:            return bucketstore.EExists
	case fasthttp.StatusInsufficientStorage: return bucketstore.EOutOfStorage
	case fasthttp.StatusNotFound:            return bucketstore.ENoBucket
	case statusTemporaryFailure:             return bucketstore.ETemporaryFailure
	default:                                 return bucketstore.EDiskFailure
	}
	
	panic("unreachable")
}
func (c *Client) Get(id []byte, overv, head, body *bufferex.Binary) (ok bool,err error) {
	var buf [1]byte
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	
	if overv!= nil { buf[0]|=1 }
	if head != nil { buf[0]|=2 }
	if body != nil { buf[0]|=4 }
	buf[0] += '0'
	
	c.setUrl(req,id,buf[:])
	req.Header.SetMethod("GET")
	
	required(req)
	err = c.client.DoDeadline(req,resp,time.Now().Add(time.Second))
	
	if err!=nil { return }
	
	fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	
	overl := binarix.Atoi(resp.Header.Peek("X-Over"))
	headl := binarix.Atoi(resp.Header.Peek("X-Head"))
	bodyl := binarix.Atoi(resp.Header.Peek("X-Body"))
	
	if overv!=nil && overl==0 { return }
	if head !=nil && headl==0 { return }
	if body !=nil && bodyl==0 { return }
	
	rdata := resp.Body()
	if int64(len(rdata))!=(overl+headl+bodyl) {
		return
	}
	
	if overv!=nil { *overv = bufferex.NewBinary(rdata[:overl]) }
	rdata = rdata[overl:]
	if head!=nil { *head = bufferex.NewBinary(rdata[:headl]) }
	rdata = rdata[headl:]
	if body!=nil { *body = bufferex.NewBinary(rdata) }
	
	ok = true
	
	return
}
func (c *Client) Expire(expire time.Time) error {
	var buf [32]byte
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	
	req.Header.SetMethod("DELETE")
	req.SetRequestURI(fmt.Sprintf("/%s/%s",c.uuid,expire.AppendFormat(buf[:0],URLDate)))
	
	required(req)
	err := c.client.DoDeadline(req,resp,time.Now().Add(time.Second))
	
	if err!=nil { return err }
	
	fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	
	if resp.StatusCode()!=fasthttp.StatusNoContent { return bucketstore.EDiskFailure }
	
	return nil
}
func (c *Client) FreeStorage() (int64,error) {
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	
	req.Header.SetMethod("HEAD")
	req.SetRequestURI(fmt.Sprintf("/%s",c.uuid))
	
	required(req)
	err := c.client.DoDeadline(req,resp,time.Now().Add(time.Second))
	
	if err!=nil { return 0,err }
	
	fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	
	if resp.StatusCode()!=fasthttp.StatusNoContent { return 0,bucketstore.EDiskFailure }
	
	return binarix.Atoi(resp.Header.Peek("X-Free-Storage")),nil
}

type MultiClient struct{
	client HttpClient
}
func NewMultiClient(c HttpClient) *MultiClient {
	return &MultiClient{c}
}

func (m *MultiClient) Submit(id, overv, head, body []byte, expire time.Time) (bucket bufferex.Binary,err error) {
	const prefix = "/api.submit/"
	var numbuf [16]byte
	var buf [32]byte
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	
	// Do it the inefficient way!
	req.SetRequestURI(fmt.Sprintf("/api.submit/%s/%s",codec.EncodeToString(id),expire.AppendFormat(buf[:0],URLDate)))
	
	req.Header.SetMethod("POST")
	
	req.Header.SetBytesV("X-Over",binarix.Itoa(int64(len(overv)),numbuf[:0]))
	req.Header.SetBytesV("X-Head",binarix.Itoa(int64(len(head )),numbuf[:0]))
	req.Header.SetBytesV("X-Body",binarix.Itoa(int64(len(body )),numbuf[:0]))
	
	req.AppendBody(overv)
	req.AppendBody(head)
	req.AppendBody(body)
	
	required(req)
	err = m.client.DoDeadline(req,resp,time.Now().Add(time.Second))
	
	if err!=nil { return }
	
	fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	
	switch resp.StatusCode() {
	case fasthttp.StatusBadRequest:          err = bucketstore.EBadRequest
	case fasthttp.StatusCreated:             err = nil
	case fasthttp.StatusConflict:            err = bucketstore.EExists
	case fasthttp.StatusInsufficientStorage: err = bucketstore.EOutOfStorage
	case fasthttp.StatusNotFound:            err = bucketstore.ENoBucket
	case statusTemporaryFailure:             err = bucketstore.ETemporaryFailure
	default:                                 err = bucketstore.EDiskFailure
	}
	
	if err==nil {
		bucket = bufferex.NewBinary(resp.Header.Peek("X-Bucket"))
	}
	
	return
}
func (m *MultiClient) OverPut(bucket []byte, id, overv, head, body []byte, expire time.Time) error {
	c := Client{m.client,bucket,degrader.Degrader{}}
	return c.Put(id,overv,head,body,expire)
}
func (m *MultiClient) OverGet(bucket []byte, id []byte, overv, head, body *bufferex.Binary) (ok bool,e error) {
	c := Client{m.client,bucket,degrader.Degrader{}}
	return c.Get(id,overv,head,body)
}
func (m *MultiClient) OverExpire(bucket []byte, expire time.Time) error {
	c := Client{m.client,bucket,degrader.Degrader{}}
	return c.Expire(expire)
}
func (m *MultiClient) OverFreeStorage(bucket []byte) (int64,error) {
	c := Client{m.client,bucket,degrader.Degrader{}}
	return c.FreeStorage()
}

