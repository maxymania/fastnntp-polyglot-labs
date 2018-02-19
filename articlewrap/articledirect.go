/*
Copyright (c) 2017-2018 Simon Schmidt

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


package articlewrap

import "github.com/vmihailenco/msgpack"
import "github.com/byte-mug/fastnntp/posting"
import "github.com/maxymania/fastnntp-polyglot"
import "github.com/maxymania/fastnntp-polyglot-labs/bucketstore"
import "github.com/maxymania/fastnntp-polyglot-labs/bufferex"
import "github.com/maxymania/fastnntp-polyglot/policies"
//import "github.com/maxymania/fastnntp-polyglot/buffer"

func repool(bak, work *[]byte) {
	if cap(*bak)<cap(*work) { *bak = *work }
	if cap(*work)<cap(*bak) { *work = *bak }
}
func repoolObj(bak, work *newspolyglot.ArticleOverview){
	repool(&bak.Subject,&work.Subject)
	repool(&bak.From   ,&work.From   )
	repool(&bak.Date   ,&work.Date   )
	repool(&bak.MsgId  ,&work.MsgId  )
	repool(&bak.Refs   ,&work.Refs   )
}

type ArticleDirectBackend struct{
	Store   bucketstore.OverStore
	Bdb     BucketDatabase
	Policy  policies.PostingPolicy
}
func (adb *ArticleDirectBackend) ArticleDirectStat(id []byte) bool {
	bin,_ := adb.Bdb.QueryIDMapping(id)
	defer bin.Free()
	return len(bin.Bytes())>0
}
func (adb *ArticleDirectBackend) ArticleDirectGet(id []byte, head, body bool) *newspolyglot.ArticleObject {
	bin,_ := adb.Bdb.QueryIDMapping(id)
	defer bin.Free()
	if len(bin.Bytes())==0 { return nil } // Not found
	
	var bhead, bbody bufferex.Binary
	var phead, pbody *bufferex.Binary
	if head { phead = &bhead }
	if body { pbody = &bbody }
	ok,e := adb.Store.OverGet(bin.Bytes(),id, nil, phead, pbody)
	defer bhead.Free()
	defer bbody.Free()
	if e!=nil || !ok {
		return nil
	}
	obj := new(newspolyglot.ArticleObject)
	if head {
		obj.Bufs[0],obj.Head,e = zdecode(bhead.Bytes())
		if e!=nil { return nil }
	}
	if body {
		obj.Bufs[1],obj.Body,e = zdecode(bbody.Bytes())
		if e!=nil { return nil }
	}
	return obj
}
func (adb *ArticleDirectBackend) ArticleDirectOverview(id []byte) *newspolyglot.ArticleOverview {
	bin,_ := adb.Bdb.QueryIDMapping(id)
	defer bin.Free()
	if len(bin.Bytes())==0 { return nil } // Not found
	
	var bov bufferex.Binary
	ok,e := adb.Store.OverGet(bin.Bytes(),id, &bov, nil, nil)
	defer bov.Free()
	if e!=nil || !ok { return nil }
	
	obj := new(newspolyglot.ArticleOverview)
	e = zunmarshal(
		 bov.Bytes(),
		&obj.Subject,
		&obj.From,
		&obj.Date,
		&obj.MsgId,
		&obj.Refs,
		&obj.Bytes,
		&obj.Lines)
	if e!=nil { return nil }
	return obj
}

func (adb *ArticleDirectBackend) ArticleGroupStat(group []byte, num int64, id_buf []byte) ([]byte, bool) {
	msgid,bucket,_ := adb.Bdb.QueryGroupMapping(group,num)
	defer msgid.Free()
	defer bucket.Free()
	if len(msgid.Bytes())==0 { return nil,false }
	return append(id_buf[:0],msgid.Bytes()...),true
}
func (adb *ArticleDirectBackend) ArticleGroupGet (group []byte, num int64, head, body bool, id_buf []byte) ([]byte, *newspolyglot.ArticleObject) {
	msgid,bucket,_ := adb.Bdb.QueryGroupMapping(group,num)
	defer msgid.Free()
	defer bucket.Free()
	if len(msgid.Bytes())==0 { return nil,nil }
	
	var bhead, bbody bufferex.Binary
	var phead, pbody *bufferex.Binary
	if head { phead = &bhead }
	if body { pbody = &bbody }
	ok,e := adb.Store.OverGet(bucket.Bytes(),msgid.Bytes(), nil, phead, pbody)
	defer bhead.Free()
	defer bbody.Free()
	if e!=nil || !ok {
		return nil,nil
	}
	obj := new(newspolyglot.ArticleObject)
	if head {
		obj.Bufs[0],obj.Head,e = zdecode(bhead.Bytes())
		if e!=nil { return nil,nil }
	}
	if body {
		obj.Bufs[1],obj.Body,e = zdecode(bbody.Bytes())
		if e!=nil { return nil,nil }
	}
	return append(id_buf[:0],msgid.Bytes()...),obj
}
func (adb *ArticleDirectBackend) ArticleGroupMove(group []byte, i int64, backward bool, id_buf []byte) (ni int64, id []byte, ok bool) {
	num,msgid,err := adb.Bdb.QueryGroupShift(group,i,backward)
	if err!=nil { return }
	if num==0 { return }
	id = append(id_buf[:0],msgid.Bytes()...)
	ni = num
	ok = true
	return
}
func (adb *ArticleDirectBackend) ArticleGroupOverview(group []byte, first, last int64, targ func(int64, *newspolyglot.ArticleOverview)) {
	var obj, bak newspolyglot.ArticleOverview
	read := []interface{}{
		&obj.Subject,
		&obj.From,
		&obj.Date,
		&obj.MsgId,
		&obj.Refs,
		&obj.Bytes,
		&obj.Lines,
	}
	adb.Bdb.QueryGroupList(group,first,last,func(num int64, bucket, msgid bufferex.Binary){
		var xover bufferex.Binary
		ok,e := adb.Store.OverGet(bucket.Bytes(),msgid.Bytes(), &xover, nil, nil)
		bucket.Free()
		msgid.Free()
		
		if e==nil && ok {
			/*
			 * Reuse the Buffers from previous iterations,
			 * or safe Buffers from the current iteration, if those are bigger.
			 */
			repoolObj(&bak,&obj)
			e = zunmarshal(xover.Bytes(),read...)
		}
		
		xover.Free()
		
		if e!=nil || !ok { return }
		targ(num,&obj)
	})
	return
}
func (adb *ArticleDirectBackend) ArticleGroupList(group []byte, first, last int64, targ func(int64)) {
	adb.Bdb.QueryGroupList(group,first,last,func(num int64, bucket, msgid bufferex.Binary){
		bucket.Free()
		msgid.Free()
		targ(num)
	})
	return
}
//
func (adb *ArticleDirectBackend) ArticlePostingPost(headp *posting.HeadInfo, body []byte, ngs [][]byte, numbs []int64) (rejected bool, failed bool, err error) {
	ao := new(newspolyglot.ArticleOverview)
	ao.Subject = headp.Subject
	ao.From    = headp.From
	ao.Date    = headp.Date
	ao.MsgId   = headp.MessageId
	ao.Refs    = headp.References
	ao.Bytes   = int64(len(headp.RAW)+2+len(body))
	ao.Lines = posting.CountLines(body)
	
	overv,_ := msgpack.Marshal(ao.Subject,ao.From,ao.Date,ao.MsgId,ao.Refs,ao.Bytes,ao.Lines)
	
	decision := policies.Def(adb.Policy).Decide(ngs,ao.Lines,ao.Bytes)
	
	overv = decision.CompressXover.Def()(policies.DEFLATE{},overv)
	head := decision.CompressHeader.Def()(policies.DEFLATE{},headp.RAW)
	body  = decision.CompressBody.Def()(policies.DEFLATE{},body)
	
	var bucket bufferex.Binary
	bucket,err = adb.Store.Submit(headp.MessageId, overv, head, body, decision.ExpireAt)
	if err!=nil || len(bucket.Bytes())==0 { failed = true ; err = nil ; return } // Storage-Failure = failure
	
	defer bucket.Free()
	err = adb.Bdb.InsertIDMapping(headp.MessageId,bucket.Bytes(),decision.ExpireAt)
	if err!=nil { failed = true ; err = nil ; return } // ...Ditto
	for i,group := range ngs {
		err = adb.Bdb.InsertGoupMapping(group,numbs[i],headp.MessageId, decision.ExpireAt)
		if err!=nil { failed = true ; err = nil ; return } // ...Ditto
	}
	return
}
func (adb *ArticleDirectBackend) ArticlePostingCheckPost() (possible bool) {
	return true
}
func (adb *ArticleDirectBackend) ArticlePostingCheckPostId(id []byte) (wanted bool, possible bool) {
	bkt,err := adb.Bdb.QueryIDMapping(id)
	possible = err==nil
	wanted = len(bkt.Bytes())==0
	bkt.Free()
	return
}

