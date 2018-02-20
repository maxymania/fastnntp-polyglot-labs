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

/* Deprecated: Considered cruft right now. */
package indexdb

import "github.com/maxymania/fastnntp-polyglot/buffer"

type IndexableBit struct{
	_msgpack struct{} `msgpack:",asArray"`
	PrimaryKey    []byte
	SecondaryKeys [][]byte
}

func Prepend(buf []byte,b byte) (r Binary) {
	r = AllocBinary(len(buf)+1)
	r.Bytes()[0] = b
	copy(r.Bytes()[1:],buf)
	return r
}


type Binary struct{
	buf *[]byte
	data []byte
}

func (b Binary) Bytes() []byte { return b.data }
func (b Binary) Free() {
	buffer.Put(b.buf)
}
func AllocBinary(n int) (b Binary) {
	b.buf = buffer.Get(n)
	b.data = (*b.buf)[:n]
	return
}
func NewBinary(data []byte) (b Binary) {
	b.buf = buffer.Get(len(data))
	b.data = (*b.buf)[:len(data)]
	copy(b.data,data)
	return
}
func NewBinaryStr(data string) (b Binary) {
	b.buf = buffer.Get(len(data))
	b.data = (*b.buf)[:len(data)]
	copy(b.data,data)
	return
}
func NewBinaryInplace(data []byte) (b Binary) {
	b.data = data
	return
}
