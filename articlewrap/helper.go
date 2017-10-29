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


package articlewrap

import "io"
import "compress/flate"
import "bytes"
import "github.com/vmihailenco/msgpack"
import "sync"
import "github.com/maxymania/fastnntp-polyglot/buffer"
import "fmt"

var EBufferTooLarge = fmt.Errorf("E-Buffer-Too-Large")

var flatePool sync.Pool

func zunmarshal(data []byte,v... interface{}) error {
	var r io.ReadCloser
	d := bytes.NewReader(data)
	ir := flatePool.Get()
	if ir==nil {
		r  = flate.NewReader(d)
	} else {
		ir.(flate.Resetter).Reset(d,nil)
		r = ir.(io.ReadCloser)
	}
	defer flatePool.Put(r)
	e := msgpack.NewDecoder(r).Decode(v...)
	return e
}

func zdecode(data []byte) (*[]byte,[]byte,error) {
	var r io.ReadCloser
	d := bytes.NewReader(data)
	ir := flatePool.Get()
	if ir==nil {
		r  = flate.NewReader(d)
	} else {
		ir.(flate.Resetter).Reset(d,nil)
		r = ir.(io.ReadCloser)
	}
	defer flatePool.Put(r)
	
	tb := buffer.Get(1<<16)
	wegot := 0
	for {
		n,e := r.Read((*tb)[wegot:])
		wegot += n
		if e==io.EOF { break }
		if e!=nil { buffer.Put(tb) ; return nil,nil,e }
		if wegot==len(*tb) {
			ntb := buffer.Get(len(*tb)*2)
			if ntb!=nil { buffer.Put(tb) ; return nil,nil,EBufferTooLarge }
			copy(*ntb,*tb)
			buffer.Put(tb)
			tb = ntb
		}
	}
	
	return tb,(*tb)[:wegot],nil
}
