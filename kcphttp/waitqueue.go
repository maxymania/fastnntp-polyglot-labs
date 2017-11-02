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


package kcphttp

import (
	"sync"
	"github.com/valyala/fasthttp"
	"math/rand"
	"time"
	"errors"
)

var ETimeout = errors.New("deadline timeout")

type WorkItem struct{
	WG   sync.WaitGroup
	Req  *fasthttp.Request
	Resp *fasthttp.Response
	Dead time.Time
	Err  error
}

type WIQueue struct{
	l sync.Mutex
	m map[uint64]*WorkItem
	r *rand.Rand
}
func NewWIQueue() *WIQueue {
	return &WIQueue{m:make(map[uint64]*WorkItem),r:rand.New(rand.NewSource(rand.Int63()))}
}
func (w *WIQueue) Push(wi *WorkItem) uint64 {
	w.l.Lock(); defer w.l.Unlock()
	for i := 0 ; i<16 ; i++ {
		ni := w.r.Uint64()
		if ni == 0 { continue }
		if _,ok := w.m[ni] ; ok { continue }
		w.m[ni] = wi
		return ni
	}
	return 0
}
func (w *WIQueue) Get(ni uint64) *WorkItem {
	w.l.Lock(); defer w.l.Unlock()
	wi := w.m[ni]
	delete(w.m,ni)
	return wi
}
func (w *WIQueue) EvictExpired() {
	w.l.Lock(); defer w.l.Unlock()
	t := fasthttp.CoarseTimeNow()
	for k,v := range w.m {
		if v.Dead.Before(t) {
			v.Err = ETimeout
			v.WG.Done()
			delete(w.m,k)
		}
	}
}
func (w *WIQueue) EvictAll() {
	w.l.Lock(); defer w.l.Unlock()
	for k,v := range w.m {
		{
			v.Err = ETimeout
			v.WG.Done()
			delete(w.m,k)
		}
	}
}

