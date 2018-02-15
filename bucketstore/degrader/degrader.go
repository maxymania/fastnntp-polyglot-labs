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

// A Resource damaged-state implementation for the remote interface.
package degrader

import "sync"
import "time"

type DegraderMetadata struct{
	MaxErrors  uint
	RetryAfter time.Duration
}

type Degrader struct{
	Dmd *DegraderMetadata
	
	mtx sync.Mutex
	
	errcount uint
	damaged bool
	
	waitUntil time.Time
}

func (d *Degrader) reset(){
	d.errcount = 0
	d.damaged = false
}

func (d *Degrader) Success() {
	if !d.damaged { return }
	
	d.mtx.Lock(); defer d.mtx.Unlock()
	d.reset()
}

func (d *Degrader) Fail() {
	if d.Dmd==nil { return }
	d.mtx.Lock(); defer d.mtx.Unlock()
	
	if d.damaged { return }
	
	d.errcount++
	if d.errcount <= d.Dmd.MaxErrors { return }
	
	d.damaged = true
	
	time.Now().Add(d.Dmd.RetryAfter)
}
func (d *Degrader) ForceFail() {
	if d.Dmd==nil { return }
	d.mtx.Lock(); defer d.mtx.Unlock()
	
	if d.damaged { return }
	
	d.damaged = true
	
	time.Now().Add(d.Dmd.RetryAfter)
}

func (d *Degrader) Damaged() bool {
	if !d.damaged { return false }
	
	d.mtx.Lock(); defer d.mtx.Unlock()
	
	if d.waitUntil.Before(time.Now()) {
		d.reset()
		return false
	}
	
	return true
}

