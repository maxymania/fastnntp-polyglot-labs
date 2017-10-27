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


package remstorage


//import "github.com/maxymania/fastnntp-polyglot-labs/binarix"
//import "github.com/maxymania/fastnntp-polyglot/buffer"

import "github.com/maxymania/fastnntp-polyglot"
//import "github.com/byte-mug/fastnntp/posting"
import "github.com/valyala/fasthttp"

import "github.com/vmihailenco/msgpack"
import "fmt"

type ClientHandler interface{
	Do(req *fasthttp.Request, resp *fasthttp.Response) error
}
type Client struct{
	H     ClientHandler
	Shard string
}

func (c *Client) ArticleDirectStat(id []byte) bool {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	req.Header.SetMethod("GET")
	req.URI().SetPath(fmt.Sprintf("/%s/direct/%s/stat",c.Shard,codec.EncodeToString(id)))
	if c.H.Do(req,resp)!=nil { return false }
	return resp.StatusCode()==200
}
func (c *Client) ArticleDirectGet(id []byte, head, body bool) *newspolyglot.ArticleObject {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	req.Header.SetMethod("GET")
	req.URI().SetPath(fmt.Sprintf("/%s/direct/%s/get",c.Shard,codec.EncodeToString(id)))
	if c.H.Do(req,resp)!=nil { return nil }
	if resp.StatusCode()!=200 { return nil }
	obj := new(newspolyglot.ArticleObject)
	err := msgpack.Unmarshal(resp.Body(),&obj.Head,&obj.Body)
	if err!=nil { return nil }
	return obj
}
func (c *Client) ArticleDirectOverview(id []byte) *newspolyglot.ArticleOverview {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	req.Header.SetMethod("GET")
	req.URI().SetPath(fmt.Sprintf("/%s/direct/%s/xover",c.Shard,codec.EncodeToString(id)))
	if c.H.Do(req,resp)!=nil { return nil }
	if resp.StatusCode()!=200 { return nil }
	obj := new(newspolyglot.ArticleOverview)
	err := msgpack.Unmarshal(resp.Body(),obj)
	if err!=nil { return nil }
	return obj
}



