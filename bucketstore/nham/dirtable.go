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


package nham

import "github.com/boltdb/bolt"
import "time"
import "math/rand"
import "github.com/vmihailenco/msgpack"
import "errors"

var EAppendDenied = errors.New("Append Denied")

var bktNHIndex = []byte("nhindex")
var bktDirTable = []byte("dirtable")

type FileMetadata struct{
	_msgpack struct{} `msgpack:",asArray"`
	NailHouse int
	Sealed    bool
}

var DENIED AppendChecker = func(totalFilesize int64) error { return EAppendDenied }

type DirTable struct{
	Rnd rand.Source
	MinForwardDays int
}

func (dt *DirTable) Alloc(tx *bolt.Tx,size int64, timeNail int,checker AppendChecker) (fid FileID, fileOffset int64, e error) {
	var pref [4]byte
	var lfid FileID
	var fm FileMetadata
	var verr error
	var nonSealed []FileID
	
	idx,err := tx.CreateBucketIfNotExists(bktNHIndex)
	if err!=nil { e = err; return }
	
	tab,err := tx.CreateBucketIfNotExists(bktDirTable)
	if err!=nil { e = err; return }
	
	cur := idx.Cursor()
	
	bE.PutUint32(pref[:],uint32(timeNail))
	
	for k,v := cur.Seek(pref[:]) ; len(k)>4; k,v = cur.Next() {
		copy(lfid[:],v)
		obj := tab.Get(lfid[:])
		err = msgpack.Unmarshal(obj,&fm)
		if err!=nil { continue } /* Skip invalid entries. */
		
		lchk := checker
		
		if fm.Sealed { lchk = DENIED } /* Append is strictly denied. */
		
		copy(lfid[:],v)
		fileOffset,verr,err = FileTableInsert(tx,lfid,size,timeNail,lchk)
		if err!=nil { continue }
		if verr!=nil {
			/* Remember the non-sealed. */
			if !fm.Sealed { nonSealed = append(nonSealed,lfid) }
			continue
		}
		fid = lfid
		return
	}
	
	for {
		lfid = NewFileId(dt.Rnd)
		if len(tab.Get(lfid[:]))>0 { continue } /* Conflict*/
		break
	}
	
	limit1 := 0
	limit2 := 0
	if k,_ := cur.Last(); len(k)>=4 { limit1 = int(bE.Uint32(k)) }
	if dt.MinForwardDays!=0 { limit2 = deTime(time.Now().UTC().AddDate(0,0,dt.MinForwardDays)) }
	
	newNailHouse := timeNail
	if newNailHouse<limit1 { newNailHouse = limit1 }
	if newNailHouse<limit2 { newNailHouse = limit2 }
	
	var pkey [4+16]byte
	
	bE.PutUint32(pkey[:],uint32(newNailHouse))
	
	copy(pkey[4:],lfid[:])
	err = idx.Put(pkey[:],lfid[:])
	if err!=nil { e = err; return }
	
	fm.NailHouse = newNailHouse
	fm.Sealed = false
	obj,_ := msgpack.Marshal(&fm)
	err = tab.Put(lfid[:],obj)
	if err!=nil { e = err; return }
	
	fileOffset,verr,err = FileTableInsert(tx,lfid,size,timeNail,checker)
	if err!=nil { e = err; return }
	if verr!=nil { e = verr; return }
	
	fid = lfid
	
	return
}

