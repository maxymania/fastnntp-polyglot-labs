/*
Copyright (c) 2018 Simon Schmidt

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


package nham

import "math/rand"
import "time"
import "encoding/binary"
import "encoding/hex"
import "github.com/maxymania/fastnntp-polyglot-labs/file"

/* Temporary. */
func TimeToInt(t time.Time) int {
	return deTime(t)
}

func deTime(t time.Time) int{
	y,m,d := t.Date()
	return (y<<16)|(int(m)<<8)|d
}
func enTime(i int) time.Time {
	return time.Date(((i>>16)&0xffff),time.Month((i>>8)&0xff),i&0xff, 0,0,0,0, time.UTC)
}
func evictFile (key interface{}, value interface{}) {
	f,_ := value.(*file.File)
	if f!=nil { f.Close() }
}

var bE = binary.BigEndian

func safe_u64(b []byte) uint64 {
	var u [8]byte
	copy(u[:],b)
	return bE.Uint64(u[:])
}

type FileID [16]byte
func (a FileID) Equal(b FileID) bool {
	for i := range a {
		if a[i]!=b[i] { return false }
	}
	return true
}

func NewFileId(src rand.Source) (f FileID) {
	var d [8]byte
	x := uint64(src.Int63())
	bE.PutUint64(d[:],x)
	hex.Encode(f[:],d[:])
	return
}
func NewFileId64(src rand.Source64) (f FileID) {
	var d [8]byte
	x := src.Uint64()
	bE.PutUint64(d[:],x)
	hex.Encode(f[:],d[:])
	return
}

type Position struct{
	_msgpack struct{} `msgpack:",asArray"`
	File   FileID
	Offset int64
	Over, Head, Body int
}

