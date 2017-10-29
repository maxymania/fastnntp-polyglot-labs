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


package file

import "os"
import "sync"
import "sync/atomic"

type File struct {
	refc int32
	lock sync.Mutex
	*os.File
	name string
	flag int
	perm os.FileMode
}
func OpenFile(name string, flag int, perm os.FileMode) *File {
	return &File{name:name,flag:flag,perm:perm}
}
func (f *File) Close() error {
	if atomic.AddInt32(&f.refc,-1)!=0 { return nil }
	f.lock.Lock(); defer f.lock.Unlock()
	if f.File==nil { return nil }
	f.File.Close()
	f.File = nil
	return nil
}
func (f *File) Open() error {
	if atomic.AddInt32(&f.refc,1)!=1 { return nil }
	f.lock.Lock(); defer f.lock.Unlock()
	g,e := os.OpenFile(f.name,f.flag,f.perm)
	if e!=nil {
		atomic.AddInt32(&f.refc,-1)
		return e
	}
	f.File = g
	return nil
}

