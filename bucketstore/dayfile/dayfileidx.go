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


package dayfile

import "github.com/boltdb/bolt"
import "time"
import "path/filepath"
import "sync"
import "os"
import "encoding/binary"

import "github.com/maxymania/fastnntp-polyglot-labs/file"
import "github.com/maxymania/fastnntp-polyglot-labs/bufferex"
import "github.com/maxymania/fastnntp-polyglot-labs/bucketstore"
import "github.com/hashicorp/golang-lru"
import "github.com/vmihailenco/msgpack"
import "bytes"
import "io/ioutil"

const dayFile_Fmt = "20060102"
type DayID [8]byte

var bE = binary.BigEndian

var bktFileSize = []byte("filesize")
var bktIndex = []byte("index")
var bktIndexRel = []byte("indexrel")

type Position struct{
	_msgpack struct{} `msgpack:",asArray"`
	Day DayID
	Offset int64
	Over, Head, Body int
}

func evictFile (key interface{}, value interface{}) {
	f,_ := value.(*file.File)
	if f!=nil { f.Close() }
}

type DayfileIndex struct{
	db    *bolt.DB
	path  string
	cache *lru.Cache
	maxsp int64
	mutex sync.Mutex
}
func OpenDayfileIndex(path string, cfg *bucketstore.Config) (*DayfileIndex,error) {
	ch,err := lru.NewWithEvict(cfg.MaxFiles,evictFile)
	if err!=nil { return nil,err }
	
	dbp := filepath.Join(path,"bucket.db")
	db,err := bolt.Open(dbp,0600,nil)
	if err!=nil { return nil,err }
	
	return &DayfileIndex{db:db,path:path,cache:ch,maxsp:cfg.MaxSpace},nil
}

func openDayfileBucket(path string, cfg *bucketstore.Config) (bucketstore.BucketStore,error) {
	ch,err := lru.NewWithEvict(cfg.MaxFiles,evictFile)
	if err!=nil { return nil,err }
	
	dbp := filepath.Join(path,"bucket.db")
	db,err := bolt.Open(dbp,0600,nil)
	if err!=nil { return nil,err }
	
	return &DayfileIndex{db:db,path:path,cache:ch,maxsp:cfg.MaxSpace},nil
}
func init(){
	bucketstore.Backends["dayfile"] = openDayfileBucket
}

func (d *DayfileIndex) open(dayid DayID) (*file.File,error) {
	var f *file.File
	rf,ok := d.cache.Get(dayid)
	if ok { f = rf.(*file.File) ; return f,f.Open() }
	f = file.OpenFile(filepath.Join(d.path,string(dayid[:])),os.O_CREATE|os.O_RDWR,0600)
	if e := f.Open() ; e!=nil { return nil,e }
	f.Open()
	d.mutex.Lock(); defer d.mutex.Unlock()
	d.cache.Remove(dayid)
	d.cache.Add(dayid,f)
	return f,nil
}
func (d *DayfileIndex) delete(dayid DayID) {
	d.cache.Remove(dayid)
	os.Remove(filepath.Join(d.path,string(dayid[:])))
}
func (d *DayfileIndex) Put(id, overv, head, body []byte, expire time.Time) error {
	var dayid DayID
	expire.UTC().AppendFormat(dayid[:0],dayFile_Fmt)
	f,e := d.open(dayid)
	if e!=nil { return e }
	defer f.Close()
	
	var e2 error
	e = d.db.Batch(func(tx *bolt.Tx) error {
		var ibuf [8]byte
		fSz,err := tx.CreateBucketIfNotExists(bktFileSize)
		if err!=nil { return err }
		idx,err := tx.CreateBucketIfNotExists(bktIndex)
		if err!=nil { return err }
		idxrel,err := tx.CreateBucketIfNotExists(bktIndexRel)
		if err!=nil { return err }
		
		if len(idx.Get(id))!=0 { e2 = bucketstore.EExists ; return nil }
		
		copy(ibuf[:],fSz.Get(dayid[:]))
		lng := int64(bE.Uint64(ibuf[:]))
		
		pos := Position{struct{}{},dayid,lng,len(overv),len(head),len(body)}
		
		_,e2 = f.WriteAt(overv,lng)
		if e2!=nil { return nil }
		lng += int64(len(overv))
		_,e2 = f.WriteAt(head,lng)
		if e2!=nil { return nil }
		lng += int64(len(head))
		_,e2 = f.WriteAt(body,lng)
		if e2!=nil { return nil }
		lng += int64(len(body))
		
		bE.PutUint64(ibuf[:],uint64(lng))
		err = fSz.Put(dayid[:],ibuf[:])
		if err!=nil { return err }
		
		buf,_ := msgpack.Marshal(&pos)
		
		idx.Put(id,buf)
		relid := bufferex.AllocBinary(len(id)+len(dayid))
		defer relid.Free()
		copy(relid.Bytes(),dayid[:])
		copy(relid.Bytes()[len(dayid):],id)
		idxrel.Put(relid.Bytes(),id)
		
		return nil
	})
	if e==nil { e=e2 }
	return e
}
func (d *DayfileIndex) Get(id []byte, overv, head, body *bufferex.Binary) (ok bool,e error) {
	var pos Position
	e = d.db.View(func(tx *bolt.Tx) error {
		fSz := tx.Bucket(bktFileSize)
		if fSz==nil { return nil }
		idx := tx.Bucket(bktIndex)
		if idx==nil { return nil }
		
		err := msgpack.Unmarshal(idx.Get(id),&pos)
		if err!=nil { return nil }
		
		ok = true
		return nil
	})
	if e!=nil || !ok { return }
	var f *file.File
	f,e = d.open(pos.Day)
	if e!=nil { ok = false ; return }
	if overv!=nil {
		*overv = bufferex.AllocBinary(pos.Over)
		_,e = f.ReadAt(overv.Bytes(),pos.Offset)
		if e!=nil { ok = false ; return }
	}
	if head!=nil {
		*head = bufferex.AllocBinary(pos.Head)
		_,e = f.ReadAt(head.Bytes(),pos.Offset+int64(pos.Over))
		if e!=nil { ok = false ; return }
	}
	if body!=nil {
		*body = bufferex.AllocBinary(pos.Body)
		_,e = f.ReadAt(body.Bytes(),pos.Offset+int64(pos.Over+pos.Head))
		if e!=nil { ok = false ; return }
	}
	return
}

func (d *DayfileIndex) Expire(expire time.Time) error {
	var dayid DayID
	expire.UTC().AppendFormat(dayid[:0],dayFile_Fmt)
	
	// Step 1: delete all dayfiles until (including) expire!
	e := d.db.View(func(tx *bolt.Tx) error {
		fSz := tx.Bucket(bktFileSize)
		if fSz==nil { return nil }
		cur := fSz.Cursor()
		for key,_ := cur.First() ; len(key)>0 && bytes.Compare(key,dayid[:])<=0 ; key,_ = cur.Next() {
			d.delete(dayid)
		}
		return nil
	})
	
	if e!=nil { return e }
	
	// Step 2: Delete all Dayfile Size entries, and all Dayfile<->Message-ID mappings.
	e = d.db.Batch(func(tx *bolt.Tx) error {
		fSz := tx.Bucket(bktFileSize)
		if fSz==nil { return nil }
		idx := tx.Bucket(bktIndex)
		if idx==nil { return nil }
		idxrel := tx.Bucket(bktIndexRel)
		if idxrel==nil { return nil }
		
		cur := fSz.Cursor()
		
		for key,_ := cur.First() ; len(key)>0 && bytes.Compare(key,dayid[:])<=0 ; key,_ = cur.Next() {
			cur.Delete()
		}
		
		cur = idxrel.Cursor()
		
		for key,id := cur.First() ; len(key)>=len(dayid) && bytes.Compare(key[:len(dayid)],dayid[:])<=0 ; key,id = cur.Next() {
			idx.Delete(id)
			cur.Delete()
		}
		
		return nil
	})
	return e
}
func (d *DayfileIndex) FreeStorage() (int64,error){
	fi,e := ioutil.ReadDir(d.path)
	if e!=nil { return 0,e }
	n := d.maxsp
	for _,i := range fi {
		if i.IsDir() { continue }
		n -= i.Size()
	}
	if n<0 { n = 0 }
	return n,nil
}
