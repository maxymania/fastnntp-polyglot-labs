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
import "bytes"
import "github.com/vmihailenco/msgpack"

var bktFileTable = []byte("filetable")

// The Prefixes for FeId
const (
	// Used for all entries.
	FeId_Master byte = iota
	FeId_Free
	FeId_Used
	FeId_SizeCount
	FeId_EndSizeCount
)

type FeId [9]byte
func (f *FeId) Zero() {
	*f = FeId{}
}
func (f *FeId) Offset(i int64) {
	f[0] = FeId_Master
	bE.PutUint64(f[1:],uint64(i))
}
func (f *FeId) Get() int64 {
	return int64(bE.Uint64(f[1:]))
}
func (f *FeId) SetType(b byte) {
	f[0] = b
}

type AppendChecker func(totalFilesize int64) error

type FileEntry struct{
	_msgpack struct{} `msgpack:",asArray"`
	Offset int64
	Size int64
	Used bool
	TimeNail int
}
func fileTablePut(tab *bolt.Bucket,fe FileEntry) {
	var fek FeId
	fek.Offset(fe.Offset)
	data,_ := msgpack.Marshal(&fe)
	tab.Put(fek[:],data)
	if !fe.Used {
		var sk FeId
		var counter [8]byte
		sk.Offset(fe.Size)
		sk.SetType(FeId_SizeCount)
		
		fek.SetType(FeId_Free)
		tab.Put(fek[:],sk[1:])
		
		copy(counter[:],tab.Get(sk[:]))
		/* The counter is a Little-Endian number, because it is easier to handle. */
		for i := range counter {
			counter[i]++
			if counter[i]==0x00 { continue }
		}
		tab.Put(sk[:],counter[:])
	} else {
		fek.SetType(FeId_Used)
		tab.Put(fek[:],fek[:])
	}
}
func fileTableUnput(tab *bolt.Bucket,fe FileEntry) {
	var fek FeId
	fek.Offset(fe.Offset)
	if !fe.Used {
		fek.SetType(FeId_Free)
		tab.Delete(fek[:])
		
		fek.Offset(fe.Size) /* Get Count for Size */
		fek.SetType(FeId_SizeCount)
		
		var counter [8]byte
		copy(counter[:],tab.Get(fek[:]))
		isZero := true
		/* The counter is a Little-Endian number, because it is easier to handle. */
		for i := range counter {
			counter[i]--
			if counter[i]!=0xff { continue } /* 0x00-1 = 0xff = overflow. */
			if isZero { isZero = (counter[i]==0x00) }
		}
		if isZero {
			tab.Delete(fek[:]) /* If the value is ZERO, then delete it from the map. */
		} else {
			tab.Put(fek[:],counter[:])
		}
	} else {
		fek.SetType(FeId_Used)
		tab.Delete(fek[:])
	}
}


func FileTableInsert(tx *bolt.Tx, id FileID,size int64, timeNail int,checker AppendChecker) (finalOffset int64,ve error,e error) {
	freePX := [1]byte{FeId_Free}
	bkt,err := tx.CreateBucketIfNotExists(bktFileTable)
	if err!=nil { e = err; return }
	
	tab,err := bkt.CreateBucketIfNotExists(id[:])
	if err!=nil { e = err; return }
	
	cur := tab.Cursor()
	for k,v := cur.Seek(freePX[:]); bytes.HasPrefix(k,freePX[:]); k,v = cur.Next() {
		entSz := int64(safe_u64(v))
		if entSz<size { continue } /* Not enough space. */
		var fek FeId
		var fe FileEntry
		
		copy(fek[:],k)
		fek.SetType(FeId_Master)
		err := msgpack.Unmarshal(tab.Get(fek[:]),&fe)
		if err !=nil { continue } /* Ignore Corrupted entry. */
		
		/* Unput the entry. */
		fileTableUnput(tab,fe)
		
		nfe := fe
		nfe.Offset += size
		nfe.Size -= size
		if nfe.Size>0 {
			fileTablePut(tab,nfe)
		}
		fe.Used = true
		fe.Size = size
		fe.TimeNail = timeNail
		
		fileTablePut(tab,fe)
		
		finalOffset = fe.Offset
		return
	}
	
	/* Get the last Object. */
	var lo []byte
	if k,_ := cur.Seek(freePX[:]); len(k)!=0 {
		_,lo = cur.Prev()
	} else {
		_,lo = cur.Last()
	}
	
	{
		var fe,nfe FileEntry
		if len(lo)>0 {
			err := msgpack.Unmarshal(lo,&fe)
			if err!=nil { e = err; return }
		}
		grandOffset := fe.Offset+fe.Size
		ve = checker(grandOffset+size)
		if ve!=nil { return } /* The checker complains about the size. */
		
		nfe.Offset = grandOffset
		nfe.Size = size
		nfe.Used = true
		nfe.TimeNail = timeNail
		fileTablePut(tab,nfe)
		
		finalOffset = grandOffset
	}
	
	return
}

func FileTableExpire(tx *bolt.Tx, id FileID,size int64, upToTime int) (e error) {
	//usedPX := [1]byte{FeId_Used}
	freePX := [1]byte{FeId_Free}
	bkt := tx.Bucket(bktFileTable)
	if bkt==nil { return }
	tab := bkt.Bucket(id[:])
	if tab==nil { return }
	
	cur := tab.Cursor()
	var ofe FileEntry
	first := true
	
	for k,v := cur.Seek(freePX[:]); bytes.HasPrefix(k,freePX[:]); k,v = cur.Next() {
		var fe FileEntry
		err := msgpack.Unmarshal(v,&fe)
		if err!=nil { first = true; continue } /* Skip Corrupted entry. */
		first = false
		ofe = fe
		updated := false
		if fe.Used {
			if fe.TimeNail > upToTime { continue } /* Not yet expired. */
			fileTableUnput(tab,fe)
			fe.Used = false
			fe.TimeNail = 0
			updated = true
			//fileTablePut(tab,fe)
		}
		/* Lemma: at this point the current entry is free. */
		
		if !(first || ofe.Used) { /* The Previous entry is free. */
			fileTableUnput(tab,ofe)
			if !updated { fileTableUnput(tab,fe) }
			
			/* Merge the previous entry with the current, and delete the current one. */
			ofe.Size = (fe.Offset-ofe.Offset)+fe.Size
			//cur.Delete()
			tab.Delete(k)
			
			/* Update the previous entry. */
			fileTablePut(tab,ofe)
			continue
		}
		
		if !updated { continue }
		fileTablePut(tab,fe)
	}
	
	return
}


