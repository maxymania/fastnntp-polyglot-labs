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
import "github.com/klauspost/compress/flate"
import "bytes"

import "github.com/foobaz/go-zopfli/zopfli"
import "github.com/maxymania/fastnntp-polyglot-labs/policies_ex/format"

func estf6bsr(f float64,t []float64) int {
	b,n := 0,len(t)	/* e = b+n */
	for {
		i := (n/2)
		if i==0 { return b }
		g := t[i+b]
		if f<g {
			n = i
		} else if f>g {
			n -= i
			b += i
		} else {
			return b+i
		}
	}
	panic("unreach")
}

var est2ratio = [...]float64 {
	0.0, // 0
	0.1, // 1
	0.2, // 2
	0.3, // 3
	0.4, // 4
	0.5, // 5
	0.6, // 6
	0.7, // 7
	0.8, // 8
	0.9, // 9
}

type AdaptiveDeflatorConfig struct {
	CompressibleSize format.Range `inn:"size"`
	MaxRatio            int `inn:"ratio"`
	UseHuffmanInCase    bool `inn:"nc-use-huffman"`
	BoostRatio          format.Range `inn:"boost"`
	Zopfli struct{
		Size format.Range `inn:"size"`
		MinCompressibility float64 `inn:"compressible"`
	} `inn:"zopfli!"`
}

func (cfg *AdaptiveDeflatorConfig) Build(ad *AdaptiveDeflator) {
	ad.CompressibleSize = cfg.CompressibleSize
	ad.MaxRatio         = cfg.MaxRatio
	ad.UseHuffmanInCase = cfg.UseHuffmanInCase
	ad.BoostRatio       = cfg.BoostRatio.Min
	ad.BoostMaxRatio    = cfg.BoostRatio.Max
	ad.ZopfliMinSize    = cfg.Zopfli.Size.Min
	ad.ZopfliMaxSize    = cfg.Zopfli.Size.Max
	ad.ZopfliMinCompressibility = cfg.Zopfli.MinCompressibility
}

/* Advanced adaptive DEFLATE encoder */
type AdaptiveDeflator struct {
	// If not zero, this variable declares, how small a BLOB may be at least and how big it may be at most,
	// in order to archieve proper compression.
	CompressibleSize format.Range
	
	// The Maximum DELATE-Ratio, that might be used to compress the data.
	// This option is meant to limit the performance impact of large BLOBs.
	MaxRatio int
	
	// If true, the function will use HuffmanOnly instead of NoCompression for
	// non-compressible BLOBs.
	UseHuffmanInCase bool
	
	// If the data is compressible, add BoostRatio but don't exceed BoostMaxRatio
	BoostRatio, BoostMaxRatio int
	
	// If ZopfliMinSize <= len(data) <= ZopfliMaxSize AND len(data)!=0, then
	// the Zopfli Compression algorithm will be used instead of the regular
	// Deflate routines, thus resulting in much higher compression ratio.
	ZopfliMinSize, ZopfliMaxSize int
	
	// If the estimated compressibility is < ZopfliMinCompressibility, then
	// the Zopfli algorithm will not kick in.
	ZopfliMinCompressibility float64
}

func (ad *AdaptiveDeflator) passTooBig(data []byte) []byte {
	ratio := flate.NoCompression
	buf := new(bytes.Buffer)
	w,e := flate.NewWriter(buf,ratio)
	if e!=nil { panic(e) }
	w.Write(data)
	w.Close()
	return buf.Bytes()
}

func (ad *AdaptiveDeflator) compress(data []byte, ratio int) []byte {
	{
		useHuffmanOnly := ad.UseHuffmanInCase && (ratio==flate.NoCompression)
		exceedMaxRatio := ratio > ad.MaxRatio
		performBoost   := (ratio>0) && (ratio<ad.BoostMaxRatio) && !exceedMaxRatio
		
		if useHuffmanOnly { ratio = flate.HuffmanOnly }
		if exceedMaxRatio { ratio = ad.MaxRatio }
		if performBoost {
			ratio += ad.MaxRatio
			if ratio > ad.BoostMaxRatio { ratio = ad.BoostMaxRatio }
		}
	}
	buf := new(bytes.Buffer)
	w,e := flate.NewWriter(buf,ratio)
	if e!=nil { panic(e) }
	w.Write(data)
	w.Close()
	return buf.Bytes()
}
func (ad *AdaptiveDeflator) compressZopfli(data []byte) []byte {
	buf := new(bytes.Buffer)
	o := zopfli.DefaultOptions()
	o.NumIterations = 10
	o.BlockSplittingLast = true
	o.BlockType = 2
	def := zopfli.NewDeflator(buf,&o)
	def.Deflate(true,data)
	return buf.Bytes()
}

/* Use this method as policies.DeflateFunction closure. */
func (ad *AdaptiveDeflator) Deflate(d policies.DEFLATE,data []byte) []byte {
	l := len(data)
	f := compress.Estimate(data)
	r := estf6bsr(f,est2ratio[:])
	
	if l<ad.CompressibleSize.Min {
		return ad.passTooBig(data)
	}
	
	/* If ab.MaxCompressibleSize is ZERO, this check is disabled. */
	if ad.CompressibleSize.Max!=0 {
		if l>ad.CompressibleSize.Max {
			return ad.passTooBig(data)
		}
	}
	
	/* If ad.ZopfliMaxSize is ZERO, zopfli is deactivated. */
	if ad.ZopfliMaxSize!=0 {
		if (l!=0) && (ad.ZopfliMinSize<=l) && (l<=ad.ZopfliMaxSize) && (f>=ad.ZopfliMinCompressibility) {
			return ad.compressZopfli(data)
		}
	}
	return ad.compress(data,r)
}

