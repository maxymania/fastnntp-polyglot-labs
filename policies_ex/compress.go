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

import "github.com/klauspost/compress"
import "github.com/foobaz/go-zopfli/zopfli"
import "github.com/klauspost/compress/flate"
import "bytes"
import "fmt"

/* Very fast policies.DeflateFunction (llar). */
func HuffmanOnlyDeflate(d policies.DEFLATE,data []byte) []byte {
	buf := new(bytes.Buffer)
	w,e := flate.NewWriter(buf,flate.HuffmanOnly)
	if e!=nil { panic(e) }
	w.Write(data)
	w.Close()
	return buf.Bytes()
}

/* Fast policies.DeflateFunction */
func FastDeflate(d policies.DEFLATE,data []byte) []byte {
	buf := new(bytes.Buffer)
	w,e := flate.NewWriter(buf,1)
	if e!=nil { panic(e) }
	w.Write(data)
	w.Close()
	return buf.Bytes()
}

/* High efficiency policies.DeflateFunction */
func ZopfliDeflate(d policies.DEFLATE,data []byte) []byte {
	buf := new(bytes.Buffer)
	o := zopfli.DefaultOptions()
	o.NumIterations = 15
	o.BlockSplittingLast = true
	o.BlockType = 2
	def := zopfli.NewDeflator(buf,&o)
	def.Deflate(true,data)
	return buf.Bytes()
}

/* Faster alternaticve to policies.NewDeflateFunction(level int) */
func NewDeflateFunction(level int) policies.DeflateFunction {
	if level<  -2 || 9<level { panic(fmt.Errorf("Invalid level: %d",level)) }
	return func(d policies.DEFLATE,data []byte) []byte {
		buf := new(bytes.Buffer)
		w,e := flate.NewWriter(buf,level)
		if e!=nil { panic(e) }
		w.Write(data)
		w.Close()
		return buf.Bytes()
	}
}

/*
 Adaptive Compression function constructor.
 This produces an policies.DeflateFunction,
 that first estimates the compressibility of the data,
 and then, chooses an underlying compression function, depending on
 whether the data is Non-Compressible, Compressible or Very compressible.
 */
func CreateSimpleAdaptiveDeflate(unCompressible, compressible, veryCompressible policies.DeflateFunction) policies.DeflateFunction {
	not := unCompressible.Def()
	slight := compressible.Def()
	very := veryCompressible.Def()
	return func(d policies.DEFLATE,data []byte) []byte {
		f := compress.Estimate(data)
		if f<0.1 { /* Not compressible */
			return not(d,data)
		} else if f<0.5 { /* Compressible */
			return slight(d,data)
		} else { /* Very Compressible */
			return very(d,data)
		}
		panic("unreachable")
	}
}


