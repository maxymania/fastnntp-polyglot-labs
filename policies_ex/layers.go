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


package policies_ex

import "github.com/maxymania/fastnntp-polyglot/policies"
import "time"

type LayerElement struct {
	Xover, Head, Body policies.DeflateFunction
	
	ExpireDays int
	
	Criteria *Matcher
}

type Layer struct {
	Inner      policies.PostingPolicy
	Element    []LayerElement
	PerformAll bool
}

func (l *Layer) Decide(groups [][]byte, lines, length int64) policies.PostingDecision {
	pd := l.Inner.Decide(groups,lines,length)
	for _,e := range l.Element {
		if !e.Criteria.Match(groups,lines,length) { continue }
		
		if e.Xover!=nil { pd.CompressXover = e.Xover }
		if e.Head!=nil { pd.CompressHeader = e.Head }
		if e.Body!=nil { pd.CompressBody = e.Body }
		if e.ExpireDays>0 { pd.ExpireAt = time.Now().UTC().AddDate(0,0,e.ExpireDays) }
		
		if !l.PerformAll { break }
	}
	return pd
}

