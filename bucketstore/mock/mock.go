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


package mock

import "github.com/maxymania/fastnntp-polyglot-labs/bucketstore"
import "github.com/maxymania/fastnntp-polyglot-labs/bufferex"
import "time"

type Buckets map[string]*bucketstore.Bucket

func (b Buckets) Add(bu *bucketstore.Bucket) {
	b[bu.Uuid] = bu
}
func (b Buckets) OverPut(bucket []byte, id, overv, head, body []byte, expire time.Time) error {
	if v := b[string(bucket)] ; v!=nil { return v.Store.Put(id,overv,head,body,expire) }
	return bucketstore.ENoBucket
}
func (b Buckets) OverGet(bucket []byte, id []byte, overv, head, body *bufferex.Binary) (ok bool,e error) {
	if v := b[string(bucket)] ; v!=nil { return v.Store.Get(id,overv,head,body) }
	return false,bucketstore.ENoBucket
}
func (b Buckets) OverExpire(bucket []byte, expire time.Time) error {
	if v := b[string(bucket)] ; v!=nil { return v.Store.Expire(expire) }
	return bucketstore.ENoBucket
}
func (b Buckets) OverFreeStorage(bucket []byte) (int64,error) {
	if v := b[string(bucket)] ; v!=nil { return v.Store.FreeStorage() }
	return 0,bucketstore.ENoBucket
}
func (b Buckets) Submit(id, overv, head, body []byte, expire time.Time) (bucket bufferex.Binary,err error) {
	err = bucketstore.ENoBucket
	size := int64(len(id)+len(overv)+len(head)+len(body))
	var free int64
	for _,bkt := range b {
		if free,err = bkt.Store.FreeStorage(); err==nil && free<=size {
			continue
		}
		err = bkt.Store.Put(id,overv,head,body,expire)
		if err==nil { bucket = bufferex.NewBinaryStr(bkt.Uuid) }
		return
	}
	return
}

