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


package boltidb

import "github.com/maxymania/fastnntp-polyglot-labs/indexdb"
import "github.com/vmihailenco/msgpack"
import "github.com/boltdb/bolt"
import "bytes"
import "sort"
import "sync"

func kick(c [][]byte, elem []byte) [][]byte {
	if len(c)==0 { return nil }
	i := sort.Search(len(c),func(i int) bool { return bytes.Compare(c[i],elem)>=0 })
	if i>=len(c) { i = len(c)-1 }
	if !bytes.Equal(c[i],elem) { return c }
	c[i] = c[len(c)-1]
	c = c[:len(c)-1]
	return c
}

var itself = []byte("itself")

type IndexDB struct{
	DB *bolt.DB
}
func (i *IndexDB) Insert(msg []byte) error {
	b := new(indexdb.IndexableBit)
	err := msgpack.Unmarshal(msg,b)
	if err!=nil { return err }
	return i.DB.Batch(func(tx *bolt.Tx) error{
		bkt,err := tx.CreateBucketIfNotExists(itself)
		if err!=nil { return err }
		primk := indexdb.Prepend(b.PrimaryKey,'@')
		if len(bkt.Get(primk.Bytes()))>0 { primk.Free() ; return nil }
		bkt.Put(primk.Bytes(),msg)
		primk.Free()
		for _,secKey := range b.SecondaryKeys {
			msk := indexdb.Prepend(secKey,'$')
			var c [][]byte
			msgpack.Unmarshal(bkt.Get(msk.Bytes()),&c)
			c = append(c,b.PrimaryKey)
			sort.Slice(c,func(i,j int) bool { return bytes.Compare(c[i],c[j])<0 })
			v,_ := msgpack.Marshal(c)
			bkt.Put(msk.Bytes(),v)
			msk.Free()
		}
		return nil
	})
}
func (i *IndexDB) StepIndex(secKey []byte, backward bool) (nextSecKey indexdb.Binary,itsPrimKey indexdb.Binary,err error) {
	err = i.DB.View(func(tx *bolt.Tx) error{
		bkt := tx.Bucket(itself)
		if bkt==nil { return nil }
		cur := bkt.Cursor()
		secK := indexdb.Prepend(secKey,'$')
		defer secK.Free()
		ba := [][]byte{}
		if !backward { // Forward
			key,val := cur.Seek(secK.Bytes())
			if len(key)==0 { return nil }
			if bytes.Equal(key,secK.Bytes()) { key,val = cur.Next() }
			if len(key)<=1 { return nil }
			if key[0]!='$' { return nil }
			nextSecKey = indexdb.AllocBinary(len(key)-1)
			copy(nextSecKey.Bytes(),key[1:])
			msgpack.Unmarshal(val,&ba)
			if len(ba)>0 { itsPrimKey = indexdb.NewBinaryInplace(ba[0]) }
		} else { // Backward
			key,val := cur.Seek(secK.Bytes())
			if len(key)==0 {
				key,val = cur.Last()
			} else {
				key,val = cur.Prev()
			}
			if len(key)<=1 { return nil }
			if key[0]!='$' { return nil }
			nextSecKey = indexdb.AllocBinary(len(key)-1)
			copy(nextSecKey.Bytes(),key[1:])
			msgpack.Unmarshal(val,&ba)
			if len(ba)>0 { itsPrimKey = indexdb.NewBinaryInplace(ba[0]) }
		}
		return nil
	})
	return 
}

func (i *IndexDB) DeletePrimary(priKey []byte) error {
	primk := indexdb.Prepend(priKey,'@')
	defer primk.Free()
	return i.DB.Batch(func(tx *bolt.Tx) error{
		bkt := tx.Bucket(itself)
		if bkt==nil { return nil }
		msg := bkt.Get(primk.Bytes())
		b := new(indexdb.IndexableBit)
		msgpack.Unmarshal(msg,b)
		bkt.Delete(primk.Bytes())
		for _,secKey := range b.SecondaryKeys {
			msk := indexdb.Prepend(secKey,'$')
			var c [][]byte
			msgpack.Unmarshal(bkt.Get(msk.Bytes()),&c)
			c = kick(c,primk.Bytes()[1:])
			if len(c)==0 {
				bkt.Delete(msk.Bytes())
			} else {
				sort.Slice(c,func(i,j int) bool { return bytes.Compare(c[i],c[j])<0 })
				v,_ := msgpack.Marshal(c)
				bkt.Put(msk.Bytes(),v)
			}
			msk.Free()
		}
		return nil
	})
}
func (i *IndexDB) deletePrimaryAll(priKeys [][]byte,waitGroup *sync.WaitGroup) error {
	if waitGroup!=nil { defer waitGroup.Done() }
	return i.DB.Batch(func(tx *bolt.Tx) error{
		bkt := tx.Bucket(itself)
		if bkt==nil { return nil }
		b := new(indexdb.IndexableBit)
		var primk indexdb.Binary
		for _,priKey := range priKeys {
			primk.Free()
			primk = indexdb.Prepend(priKey,'@')
			msg := bkt.Get(primk.Bytes())
			msgpack.Unmarshal(msg,b)
			bkt.Delete(primk.Bytes())
			if len(msg)==0 { continue }
			for _,secKey := range b.SecondaryKeys {
				msk := indexdb.Prepend(secKey,'$')
				var c [][]byte
				msgpack.Unmarshal(bkt.Get(msk.Bytes()),&c)
				c = kick(c,primk.Bytes()[1:])
				if len(c)==0 {
					bkt.Delete(msk.Bytes())
				} else {
					sort.Slice(c,func(i,j int) bool { return bytes.Compare(c[i],c[j])<0 })
					v,_ := msgpack.Marshal(c)
					bkt.Put(msk.Bytes(),v)
				}
				msk.Free()
			}
		}
		primk.Free()
		return nil
	})
}

func (i *IndexDB) DeleteUntil(secPrefix,secEnd []byte) error {
	prefix := indexdb.Prepend(secPrefix,'$')
	end := indexdb.Prepend(secEnd,'$')
	defer prefix.Free()
	defer end.Free()
	var waitGroup sync.WaitGroup
	err := i.DB.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(itself)
		if bkt==nil { return nil }
		cur := bkt.Cursor()
		key,val := cur.Seek(prefix.Bytes())
		for len(key)>0 && bytes.HasPrefix(key,prefix.Bytes()) && bytes.Compare(key,end.Bytes())<=0 {
			var c [][]byte
			msgpack.Unmarshal(val,&c)
			waitGroup.Add(1)
			go i.deletePrimaryAll(c,&waitGroup)
			key,val = cur.Next()
		}
		return nil
	})
	waitGroup.Wait()
	return err
}


func (i *IndexDB) ScanPrefix(secPrefix []byte, consumer func(key []byte,values [][]byte) bool) {
	prefix := indexdb.Prepend(secPrefix,'$')
	defer prefix.Free()
	i.DB.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(itself)
		if bkt==nil { return nil }
		cur := bkt.Cursor()
		key,val := cur.Seek(prefix.Bytes())
		for len(key)>0 && bytes.HasPrefix(key,prefix.Bytes()) {
			var c [][]byte
			msgpack.Unmarshal(val,&c)
			consumer(key,c)
			key,val = cur.Next()
		}
		return nil
	})
}


