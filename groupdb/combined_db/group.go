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


package combined_db

import "database/sql"

import "github.com/maxymania/fastnntp-polyglot/postauth"

func (b *Base) GroupHeadFilterWithAuth(rank postauth.AuthRank,groups [][]byte) ([][]byte,error) {
	var status byte
	stm,err := b.DB.Prepare(`
	SELECT
		n.status
	FROM
		newsactive n
	WHERE
		n.ngrp = $1
	;`)
	if err!=nil { return nil,err }
	defer stm.Close()
	i := 0
	for _,group := range groups {
		row := stm.QueryRow(group)
		ok := false
		err := row.Scan(&status)
		if err!=nil { continue }
		ok = rank.TestStatus(status)
		if ok {
			groups[i] = group
			i++
		}
	}
	return groups[:i],nil
}
func (b *Base) GroupAdmPutDescr(group []byte, descr []byte) {
	b.AdmPutDescr(group,descr)
}
func (b *Base) GroupAdmPutStatus(group []byte, status byte) {
	b.AdmPutStatus(group,status)
}

func (b *Base) AdmPutDescr(group []byte, descr []byte) {
	_,err := b.DB.Exec(`INSERT INTO newsgroups (ngrp,dscr) VALUES ($1,$2);`,group,descr)
	if err!=nil { b.DB.Exec(`UPDATE newsgroups SET descr=$1 WHERE ngrp=$2;`     ,descr,group) }
}
func (b *Base) AdmPutStatus(group []byte, status byte) {
	_,err := b.DB.Exec(`INSERT INTO newsactive (ngrp,ganlst,status) VALUES ($1,1,$2);`,group,int(status))
	if err!=nil { b.DB.Exec(`UPDATE newsactive SET status=$1 WHERE ngrp=$2;`,int(status),group) }
}

func (b *Base) GroupHeadInsert(groups [][]byte, buf []int64) (nums []int64, e error) {
	{
		l := len(groups)
		if cap(buf)<l { buf = make([]int64,l) }
		buf = buf[:l]
		nums = buf
	}
	for i,group := range groups {
		e = b.DB.QueryRow(`
		UPDATE	newsactive
		SET	ganlst = ganlst + 1
		WHERE	ngrp = $1
		RETURNING
			ganlst;
		`,group).Scan(&buf[i])
		if e!=nil { return }
	}
	return
}

func (b *Base) GroupHeadRevert(groups [][]byte, nums []int64) error {
	for i,group := range groups {
		b.DB.Exec(`
		UPDATE
			newsactive
		SET
			ganlst = ganlst - 1
		WHERE
			ganlst = $1,
			ngrp = $2
		;`,nums[i],group)
	}
	return nil
}

func (b *Base) GroupRealtimeQuery(group []byte) (number int64, low int64, high int64, ok bool) {
	err := b.DB.QueryRow(`
	SELECT
		COALESCE(n.nmsg,0),COALESCE(n.low,0),COALESCE(n.high,0)
	FROM
		artview n RIGHT JOIN newsactive m on n.ngrp=m.ngrp
	WHERE
		m.ngrp = $1
	;`,group).Scan(&number,&low,&high)
	ok = err==nil
	if ok {
		if low==0 && high >0 { low = 1 }
		if high ==0 { low = 0 }
	}
	return
}

func (b *Base) GroupRealtimeList(targ func(group []byte, high, low int64, status byte)) bool {
	rows,err := b.DB.Query(`
	SELECT
		m.ngrp,COALESCE(n.high,m.ganlst-1),COALESCE(n.low,0),m.status
	FROM
		artcache n RIGHT JOIN newsactive m on n.ngrp=m.ngrp
	;`)
	if err!=nil { return false }
	var group sql.RawBytes
	var high, low int64
	var status uint
	scn := []interface{}{&group,&high,&low,&status}
	for rows.Next() {
		err := rows.Scan(scn...)
		if err==nil {
			if low==0 && high >0 { low = 1 }
			if high <=0 { low = 0 ; high = 0 }
			targ(group,high,low,byte(status))
		}
	}
	return true
}

func (b *Base) GroupStaticList(targ func(group []byte, descr []byte)) bool {
	rows,err := b.DB.Query(`
	SELECT
		ngrp,dscr
	FROM
		newsgroups
	;`)
	if err!=nil { return false }
	var group,descr sql.RawBytes
	scn := []interface{}{&group,&descr}
	for rows.Next() {
		err = rows.Scan(scn...)
		if err==nil {
			targ(group,descr)
		}
	}
	return true
}

