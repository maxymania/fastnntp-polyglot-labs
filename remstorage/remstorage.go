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

/* Deprecated: Considered cruft right now. */
package remstorage

import "github.com/maxymania/fastnntp-polyglot-labs/binarix"
import "github.com/maxymania/fastnntp-polyglot/buffer"

import "github.com/maxymania/fastnntp-polyglot"
import "github.com/byte-mug/fastnntp/posting"
import "github.com/valyala/fasthttp"

import "encoding/base64"
import "github.com/vmihailenco/msgpack"

var codec = base64.RawURLEncoding

const (
	sTrue  = "1"
	sFalse = "0"
)
func boolToStr(v bool) string {
	if v { return sTrue }
	return sFalse
}

type Posting struct{
	Headp *posting.HeadInfo
	Body  []byte
	Ngs   [][]byte
	Numbs []int64
}


type StorageService struct{
	ArticleDirect  newspolyglot.ArticleDirectDB
	ArticleGroup   newspolyglot.ArticleGroupDB
	ArticlePosting newspolyglot.ArticlePostingDB
}
func (s *StorageService) Handle (ctx *fasthttp.RequestCtx) {
	var idbuf [256]byte
	var nbuf [32]byte
	path := binarix.Iterator{ctx.Path()}
	path.Split('/') // leading '/'
	path.Split('/') // uuid+'/'
	cat := path.Split('/')
	switch string(cat) {
	case "group": // '/8bd14200-bada-11e7-917f-3417ebbafb44/group/abcdu83bd/122/get'
		{
			grp := path.Split('/')
			rgbuf := buffer.Get(codec.DecodedLen(len(grp)))
			defer buffer.Put(rgbuf)
			n,err := codec.Decode((*rgbuf),grp)
			if err!=nil {
				ctx.Error("Invalid Base64",fasthttp.StatusBadRequest)
				return
			}
			group := (*rgbuf)[:n]
			num := binarix.Atoi(path.Split('/'))
			
			cmd := path.Split('/')
			
			backward := false
			switch string(cmd) {
			case "stat": // '/8bd14200-bada-11e7-917f-3417ebbafb44/group/abcdu83bd/122/stat'
				id,ok := s.ArticleGroup.ArticleGroupStat(group, num, idbuf[:0])
				if !ok {
					ctx.Error("No such article 404",404)
				} else {
					ctx.Error("OK",200)
					ctx.Response.Header.SetBytesV("Message-ID",id)
				}
				return
			case "get": // '/8bd14200-bada-11e7-917f-3417ebbafb44/group/abcdu83bd/122/get'
				id,obj := s.ArticleGroup.ArticleGroupGet(group, num, true, true, idbuf[:0])
				if obj==nil {
					ctx.Error("No such article 404",404)
				} else {
					ctx.Response.Header.SetBytesV("Message-ID",id)
					enc := msgpack.NewEncoder(ctx)
					enc.Encode(obj.Head,obj.Body)
					buffer.Put(obj.Bufs[0])
					buffer.Put(obj.Bufs[1])
				}
				return
			case "xover": // '/8bd14200-bada-11e7-917f-3417ebbafb44/group/abcdu83bd/122/xover/177'
				lastNum := binarix.Atoi(path.Split('/'))
				enc := msgpack.NewEncoder(ctx)
				s.ArticleGroup.ArticleGroupOverview(group, num, lastNum, func(num int64, obj *newspolyglot.ArticleOverview){
					enc.Encode(num,obj)
				})
				return
			case "list": // '/8bd14200-bada-11e7-917f-3417ebbafb44/group/abcdu83bd/122/list/177'
				lastNum := binarix.Atoi(path.Split('/'))
				enc := msgpack.NewEncoder(ctx)
				s.ArticleGroup.ArticleGroupList(group, num, lastNum, func(num int64){
					enc.Encode(num)
				})
				return
			case "prev": // '/8bd14200-bada-11e7-917f-3417ebbafb44/group/abcdu83bd/122/prev'
				backward = true
				fallthrough
			case "next": // '/8bd14200-bada-11e7-917f-3417ebbafb44/group/abcdu83bd/122/next'
				ni,id,ok := s.ArticleGroup.ArticleGroupMove(group, num, backward, idbuf[:0])
				if !ok {
					ctx.Error("No such article 404",404)
				} else {
					ctx.Error("OK",200)
					ctx.Response.Header.SetBytesV("Message-ID",id)
					ctx.Response.Header.SetBytesV("Message-Num",binarix.Itoa(ni,nbuf[:0]))
				}
				return
			}
		}
	case "direct": // '/8bd14200-bada-11e7-917f-3417ebbafb44/direct/dbebDbbKIe38/get'
		{
			var id []byte
			eid := path.Split('/')
			dleid := codec.DecodedLen(len(eid))
			if dleid>len(idbuf) {
				ribuf := buffer.Get(codec.DecodedLen(len(eid)))
				defer buffer.Put(ribuf)
				id = *ribuf
			} else {
				id = idbuf[:]
			}
			n,err := codec.Decode(id,eid)
			if err!=nil {
				ctx.Error("Invalid Base64",fasthttp.StatusBadRequest)
				return
			}
			id = id[:n]
			
			cmd := path.Split('/')
			
			switch string(cmd) {
			case "stat": // '/8bd14200-bada-11e7-917f-3417ebbafb44/direct/dbebDbbKIe38/stat'
				ok := s.ArticleDirect.ArticleDirectStat(id)
				if !ok {
					ctx.Error("No such article 404",404)
				} else {
					ctx.Error("OK",200)
				}
				return
			case "get": // '/8bd14200-bada-11e7-917f-3417ebbafb44/direct/dbebDbbKIe38/get'
				obj := s.ArticleDirect.ArticleDirectGet(id,true,true)
				if obj==nil {
					ctx.Error("No such article 404",404)
				} else {
					enc := msgpack.NewEncoder(ctx)
					enc.Encode(obj.Head,obj.Body)
					buffer.Put(obj.Bufs[0])
					buffer.Put(obj.Bufs[1])
				}
				return
			case "xover": // '/8bd14200-bada-11e7-917f-3417ebbafb44/direct/dbebDbbKIe38/xover'
				obj := s.ArticleDirect.ArticleDirectOverview(id)
				if obj==nil {
					ctx.Error("No such article 404",404)
				} else {
					enc := msgpack.NewEncoder(ctx)
					enc.Encode(obj)
				}
				return
			}
		}
	case "posting": // '/8bd14200-bada-11e7-917f-3417ebbafb44/posting/'
		if ctx.IsGet() {
			eid := path.Split('/')
			if len(eid)>0 { // '/8bd14200-bada-11e7-917f-3417ebbafb44/posting/dbebDbbKIe38' CheckID
				var id []byte
				dleid := codec.DecodedLen(len(eid))
				if dleid>len(idbuf) {
					ribuf := buffer.Get(codec.DecodedLen(len(eid)))
					defer buffer.Put(ribuf)
					id = *ribuf
				} else {
					id = idbuf[:]
				}
				n,err := codec.Decode(id,eid)
				if err!=nil {
					ctx.Error("Invalid Base64",fasthttp.StatusBadRequest)
					return
				}
				id = id[:n]
				wanted,possible := s.ArticlePosting.ArticlePostingCheckPostId(id)
				ctx.Error("OK",200)
				ctx.Response.Header.Set("X-Wanted"  ,boolToStr(wanted  ))
				ctx.Response.Header.Set("X-Possible",boolToStr(possible))
			} else { // '/8bd14200-bada-11e7-917f-3417ebbafb44/posting' CheckPosting
				possible := s.ArticlePosting.ArticlePostingCheckPost()
				ctx.Error("OK",200)
				ctx.Response.Header.Set("X-Possible",boolToStr(possible))
			}
			return
		} else if ctx.IsPost() { // POST '/8bd14200-bada-11e7-917f-3417ebbafb44/posting'
			var post Posting
			err := msgpack.Unmarshal(ctx.PostBody(),&post)
			if err!=nil {
				ctx.Error("Invalid Message",fasthttp.StatusBadRequest)
			}
			rejected,failed,err := s.ArticlePosting.ArticlePostingPost(post.Headp, post.Body, post.Ngs, post.Numbs)
			if err!=nil { rejected,failed = false,true }
			ctx.Error("OK",200)
			ctx.Response.Header.Set("X-Rejected",boolToStr(rejected))
			ctx.Response.Header.Set("X-Failed"  ,boolToStr(failed))
			return
		}
	}
	ctx.Error("Invalid command",fasthttp.StatusBadRequest)
	return
}


