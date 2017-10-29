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


package guido

import "io/ioutil"
import "path/filepath"
import "os"
import "github.com/nu7hatch/gouuid"
import "github.com/lytics/confl"

type Config struct{
	Uuid string
}

func GetUID(path string) (*uuid.UUID,error) {
	var cfg Config
	var id *uuid.UUID
	f := filepath.Join(path,"guid.cfg")
	data,err := ioutil.ReadFile(f)
	if err==nil { err = confl.Unmarshal(data, &cfg) }
	if err==nil {
		id,err = uuid.ParseHex(cfg.Uuid)
		if err!=nil { err = &os.PathError{Op:"open",Err:err} }
	}
	if err!=nil {
		ioe,ok := err.(*os.PathError)
		if !ok { return nil,err }
		if ioe.Op!="open" { return nil,err }
		id,err = uuid.NewV4()
		if err!=nil { return nil,err }
		cfg.Uuid = id.String()
		data,_ = confl.Marshal(&cfg)
		ioutil.WriteFile(f,data,0600)
		if err!=nil { return nil,err }
	}
	return id,nil
}

