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


package bucketstore

import "errors"
import "time"
import "github.com/maxymania/fastnntp-polyglot-labs/bufferex"

var EExists = errors.New("Object exists")
var ENoBucket = errors.New("No such bucket")

// Mainly used for Network Remoting:

var EDiskFailure = errors.New("Disk Failure")
var EOutOfStorage = errors.New("Out of Storage")
var EBadRequest = errors.New("Bad Request")

// Failure handling

var ETemporaryFailure = errors.New("Temporary Failure")

type BucketStore interface{
	Put(id, overv, head, body []byte, expire time.Time) error
	Get(id []byte, overv, head, body *bufferex.Binary) (ok bool,e error)
	Expire(expire time.Time) error
	FreeStorage() (int64,error)
}

type OverStore interface{
	Submit(id, overv, head, body []byte, expire time.Time) (bucket bufferex.Binary,err error)
	OverPut(bucket []byte, id, overv, head, body []byte, expire time.Time) error
	OverGet(bucket []byte, id []byte, overv, head, body *bufferex.Binary) (ok bool,e error)
	OverExpire(bucket []byte, expire time.Time) error
	OverFreeStorage(bucket []byte) (int64,error)
}

type Config struct{
	MaxSpace int64 // Max. Storage consuption in bytes.
	MaxFiles int // Max. number of file descriptors
}

type Loader func(path string, cfg *Config) (BucketStore,error)

var Backends = make(map[string]Loader)

