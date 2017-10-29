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


package bufferex

import "github.com/maxymania/fastnntp-polyglot/buffer"

type Binary struct{
	buf *[]byte
	data []byte
}

func (b Binary) BufferAllocUnit() *[]byte { return b.buf }
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

