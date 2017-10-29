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


package bucketstore

import "github.com/maxymania/fastnntp-polyglot-labs/guido"
import "fmt"

type Bucket struct{
	Store BucketStore
	Uuid  string
}

func OpenStore(kind, path string, cfg *Config) (*Bucket,error){
	loader,ok := Backends[kind]
	if !ok { return nil,fmt.Errorf("No such backend %q",kind) }
	uid,err := guido.GetUID(path)
	if err!=nil { return nil,err }
	st,err := loader(path,cfg)
	if err!=nil { return nil,err }
	return &Bucket{st,uid.String()},nil
}

