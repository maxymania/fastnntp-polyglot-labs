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


package format

import "strconv"

func splitB(str []byte,s byte) ([]byte,[]byte){
	for i,b := range str {
		if b==s {
			return str[:i],str[i+1:]
		}
	}
	return str,nil
}


type Range struct{
	Min, Max int
}
func (r *Range) UnmarshalText(text []byte) error {
	min,max := splitB(text,',')
	i,e := strconv.ParseInt(string(min),0,64)
	if e!=nil { return e }
	r.Min = int(i)
	r.Max = 0
	if len(max)!=0 {
		i,e = strconv.ParseInt(string(max),0,64)
		if e!=nil { return e }
		r.Max = int(i)
	}
	return e
}


type Range64 struct{
	Min, Max int64
}
func (r *Range64) UnmarshalText(text []byte) error {
	min,max := splitB(text,',')
	i,e := strconv.ParseInt(string(min),0,64)
	if e!=nil { return e }
	r.Min = i
	r.Max = 0
	if len(max)!=0 {
		i,e = strconv.ParseInt(string(max),0,64)
		if e!=nil { return e }
		r.Max = i
	}
	return e
}

