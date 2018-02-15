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


package loader

import "github.com/maxymania/fastnntp-polyglot/policies"
import "github.com/maxymania/fastnntp-polyglot-labs/policies_ex"

type CompressionCfg struct{
	CompType string `inn:"type"`
	Level    int    `inn:"level"`
}

type LayerElemCfg struct{
	Perform  policies_ex.MatcherConfig                      `inn:"where!"`
	Adaptive map[string]*policies_ex.AdaptiveDeflatorConfig `inn:"%adaptive!"`
	Compress map[string]*CompressionCfg                     `inn:"%compress!"`
	ExpiresAfter int                                        `inn:"expire-after"`
}
func (l *LayerElemCfg) getCompressor(n string) policies.DeflateFunction {
	if l.Adaptive!=nil {
		if ac,ok := l.Adaptive[n]; ok {
			ad := new(policies_ex.AdaptiveDeflator)
			ac.Build(ad)
			return ad.Deflate
		}
	}
	if l.Compress!=nil {
		if cf,ok := l.Compress[n]; ok {
			switch cf.CompType {
			case "none": return policies.NoDeflate
			case "zopfli":  return policies_ex.ZopfliDeflate
			case "fast": return policies_ex.FastDeflate
			case "huffman": return policies_ex.HuffmanOnlyDeflate
			}
			return policies_ex.NewDeflateFunction(cf.Level)
		}
	}
	return nil
}

func (l *LayerElemCfg) CreateLayerElement() (elem policies_ex.LayerElement) {
	matcher := new(policies_ex.Matcher)
	
	l.Perform.Build(matcher)
	
	elem.Criteria = matcher
	
	elem.Xover = l.getCompressor("xover")
	elem.Head  = l.getCompressor("head")
	elem.Body  = l.getCompressor("body")
	
	elem.ExpireDays = l.ExpiresAfter
	
	return
}

type LayerCfg struct{
	PerformAll bool `inn:"incremental"`
	Elements   []LayerElemCfg `inn:"@element!"`
}


func (l *LayerCfg) CreateLayer(inner policies.PostingPolicy) *policies_ex.Layer {
	lay := new(policies_ex.Layer)
	lay.Inner = inner
	lay.PerformAll = l.PerformAll
	lay.Element = make([]policies_ex.LayerElement,len(l.Elements))
	for i,le := range l.Elements {
		lay.Element[i] = le.CreateLayerElement()
	}
	return lay
}


