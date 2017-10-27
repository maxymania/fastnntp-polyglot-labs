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


package binarix

func Split(data []byte, sep byte) ([]byte,[]byte) {
	for i,b := range data {
		if b==sep { return data[:i],data[i+1:] }
	}
	return data,nil
}

func Atoi(data []byte) (v int64) {
	for _,b := range data {
		if b<'0' || '9'<b { continue }
		v = (v*10)+int64(b-'0')
	}
	return
}

func Itoa(i int64,buf []byte) []byte {
	var stack [32]byte
	
	sp := 0
	
	for {
		stack[sp] = byte('0'+(i%10))
		i/=10
		sp++
		if i==0 { break }
	}
	
	for sp>0 {
		sp--
		buf = append(buf,stack[sp])
	}
	return buf
}

type Iterator struct{
	Content []byte
}
func (i *Iterator) Split(sep byte) (res []byte) {
	res,i.Content = Split(i.Content,sep)
	return
}


