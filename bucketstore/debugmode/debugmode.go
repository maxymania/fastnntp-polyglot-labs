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


package debugmode

import "fmt"
import "github.com/maxymania/fastnntp-polyglot-labs/bucketstore"
import "github.com/maxymania/fastnntp-polyglot-labs/bufferex"

func bytes(arr *bufferex.Binary)  []byte {
	if arr==nil { return nil }
	return arr.Bytes()
}

type OverStoreDbg struct{
	bucketstore.OverStore
}
func (ov OverStoreDbg) OverGet(bucket []byte, id []byte, overv, head, body *bufferex.Binary) (ok bool, e error) {
	beg := fmt.Sprintf("OverGet(%q,%q)->",bucket,id)
	ok,e = ov.OverStore.OverGet(bucket,id,overv,head,body)
	fmt.Println(beg,ok,e,bytes(overv),bytes(head),bytes(body))
	return
}

