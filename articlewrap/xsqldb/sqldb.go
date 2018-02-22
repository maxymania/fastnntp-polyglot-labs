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

// Deprecated.
package xsqldb

import "database/sql"
import "text/template"
import "bytes"
import "time"
import "github.com/maxymania/fastnntp-polyglot-labs/bufferex"

type AuthRank uint8
const (
	ARReader AuthRank = iota
	ARUser
	ARModerator
	ARFeeder
)

type Dialect struct{
	Binary, Int64, Date, Byte string
}

// PostgreSQL
var PgDialect = &Dialect{
	Binary: "bytea",
	Int64: "bigint",
	Date: "date",
	Byte: "smallint",
}

// Microsoft SQL Server
// (untested)
var MsSqlDialect = &Dialect{
	Binary: "varbinary",
	Int64: "bigint",
	Date: "date",
	Byte: "tinyint",
}

var createTables = template.Must(template.New("create").Parse(`
	CREATE TABLE ngrpnumvalue (
		ngrp  {{.Binary}},
		mnum  {{.Int64}},
		msgid {{.Binary}},
		expir {{.Date}},
		PRIMARY KEY(ngrp,mnum)
	);
	CREATE TABLE msgidbkt (
		msgid  {{.Binary}} PRIMARY KEY,
		bucket {{.Binary}},
		expir  {{.Date}}
	);
	CREATE TABLE ngrpstatic (
		ngrp  {{.Binary}} PRIMARY KEY,
		descr {{.Binary}}
	);
	CREATE TABLE ngrpcnt (
		ngrp   {{.Binary}} PRIMARY KEY,
		latest {{.Int64}},
		gcount {{.Int64}},
		status {{.Byte}}
	);
	
	CREATE VIEW ngrpstat AS
	SELECT
		ngrp,
		count(mnum) as narts,
		min(mnum) as low,
		max(mnum) as high
	FROM
		ngrpnumvalue
	GROUP BY
		ngrp
	;
`))

type Base struct{
	DB *sql.DB
}
/* This must be used to implement GroupHeadCache. */
type AuthBase struct {
	Base
	Rank AuthRank
}
/* This is for PostgreSQL only. This must not be used with any other Database. */
type PgBase struct{
	Base
}

func (b *AuthBase) GroupHeadFilter(groups [][]byte) ([][]byte, error) {
	var status byte
	stm,err := b.DB.Prepare(`
	SELECT
		n.status
	FROM
		ngrpcnt n
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
		switch status {
		case 'y':
			ok = ARUser<=b.Rank
		case 'm':
			ok = ARModerator<=b.Rank
		default: /* 'n' */
			ok = ARFeeder<=b.Rank
		}
		if ok {
			groups[i] = group
			i++
		}
	}
	
	return groups[:i],nil
}


func (b *Base) CreateTables(d *Dialect) error {
	buf := new(bytes.Buffer)
	createTables.Execute(buf, d)
	_,err := b.DB.Exec(buf.String())
	return err
}

/* =========================================================================================================================== */

func (b *Base) InsertGoupMapping(group []byte, num int64, msgid []byte, expire time.Time) error {
	_,err := b.DB.Exec(`INSERT INTO ngrpnumvalue (ngrp,mnum,msgid,expir) VALUES ($1,$2,$3,$4);`,group,num,msgid,expire)
	if err==nil {
		_,err = b.DB.Exec(`UPDATE ngrpcnt SET gcount = gcount + 1 WHERE ngrp = $1;`,group)
	}
	return err
}
func (b *Base) InsertIDMapping(msgid, bucket []byte, expire time.Time) error {
	_,err := b.DB.Exec(`INSERT INTO msgidbkt (msgid,bucket,expir) VALUES ($1,$2,$3);`,msgid,bucket,expire)
	return err
}
func (b *Base) AdmPutDescr(group []byte, descr []byte) {
	_,err := b.DB.Exec(`INSERT INTO ngrpstatic (ngrp,descr) VALUES ($1,$2);`,group,descr)
	if err!=nil { b.DB.Exec(`UPDATE ngrpstatic SET descr=$1 WHERE ngrp=$2;`     ,descr,group) }
}
func (b *Base) AdmPutStatus(group []byte, status byte) {
	_,err := b.DB.Exec(`INSERT INTO ngrpcnt (ngrp,latest,gcount,status) VALUES ($1,0,0,$2);`,group,int(status))
	if err!=nil { b.DB.Exec(`UPDATE ngrpcnt SET status=$1 WHERE ngrp=$2;`,int(status),group) }
}

/* =========================================================================================================================== */

func (b *Base) QueryGroupMapping(group []byte, num int64) (msgid, bucket bufferex.Binary,err error) {
	var res *sql.Rows
	res,err = b.DB.Query(`
	SELECT
		m.msgid,
		m.bucket
	FROM
		ngrpnumvalue n JOIN msgidbkt m ON n.msgid = m.msgid
	WHERE
		n.ngrp = $1 AND
		n.mnum = $2
	;`,group,num)
	if err!=nil { return }
	defer res.Close()
	if !res.Next() { return }
	var rmsgid,rbucket sql.RawBytes
	err = res.Scan(&rmsgid,&rbucket)
	if err!=nil { return }
	msgid = bufferex.NewBinary(rmsgid)
	bucket = bufferex.NewBinary(rbucket)
	return
}
func (b *Base) QueryIDMapping(msgid []byte) (bucket bufferex.Binary,err error) {
	var res *sql.Rows
	res,err = b.DB.Query(`
	SELECT
		m.bucket
	FROM
		msgidbkt m
	WHERE
		m.msgid = $1
	;`,msgid)
	if err!=nil { return }
	defer res.Close()
	if !res.Next() { return }
	var rbucket sql.RawBytes
	err = res.Scan(&rbucket)
	if err!=nil { return }
	bucket = bufferex.NewBinary(rbucket)
	return
}

func (b *Base) QueryGroupShift(group []byte, num int64, backward bool) (nxt int64,msgid bufferex.Binary,err error) {
	var res *sql.Rows
	aggr,comp := "min",">"
	if backward { aggr,comp = "max","<" }
	res,err = b.DB.Query(`
	SELECT
		`+aggr+`(n.mnum)
	FROM
		ngrpnumvalue n
	WHERE
		n.ngrp = $1 AND n.mnum `+comp+` $2
	;`,group,num)
	if err!=nil { return }
	if !res.Next() { res.Close() ; return }
	err = res.Scan(&nxt)
	res.Close()
	if err!=nil { return }
	
	res,err = b.DB.Query(`
	SELECT
		n.msgid
	FROM
		ngrpnumvalue n
	WHERE
		n.ngrp = $1 AND n.mnum = $2
	;`,group,nxt)
	if err!=nil { return }
	defer res.Close()
	
	if !res.Next() { nxt = 0 ; return }
	var rmsgid sql.RawBytes
	err = res.Scan(&rmsgid)
	if err!=nil { return }
	msgid = bufferex.NewBinary(rmsgid)
	return
}
func (b *Base) QueryGroupList(group []byte, first, last int64, targ func(num int64, bucket, msgid bufferex.Binary)) error {
	row,err := b.DB.Query(`
		SELECT
			n.mnum,
			m.bucket,
			m.msgid
		FROM
			ngrpnumvalue n LEFT OUTER JOIN msgidbkt m
			ON n.msgid = m.msgid
		WHERE
			n.ngrp = $1 AND n.mnum >= $2 AND n.mnum <= $3
	;`,group,first,last)
	if err!=nil { return err }
	defer row.Close()
	var num int64
	var bucket,msgid sql.RawBytes
	scan := []interface{}{&num,&bucket,&msgid}
	for row.Next() {
		err := row.Scan(scan...)
		if err!=nil { return err }
		targ(num,bufferex.NewBinary(bucket),bufferex.NewBinary(msgid))
	}
	return nil
}
func (b *Base) groupExpire(group []byte, expire time.Time,errs chan <- error) {
	tx,e := b.DB.Begin()
	if e!=nil { errs <- e; return }
	res,e := tx.Exec(`DELETE FROM ngrpnumvalue n WHERE n.expir <= $1 AND n.ngrp = $1 ;`,expire,group)
	if e!=nil { errs <- e; return }
	i,e := res.RowsAffected()
	if e!=nil { errs <- e; return } /* TODO: there is a better way. */
	_,e = tx.Exec(`UPDATE ngrpcnt SET gcount = gcount - $1 WHERE ngrp = $2`,i,group)
	if e!=nil { errs <- e; return }
	errs <- tx.Commit()
}
func (b *Base) Expire(expire time.Time) error {
	rows,err := b.DB.Query(`SELECT ngrp FROM ngrpcnt`)
	if err!=nil { return err }
	var group []byte
	errs := make(chan error,16)
	n := 0
	for rows.Next() {
		rows.Scan(group)
		go b.groupExpire(group,expire,errs)
		n++
	}
	n++
	go func(){
		_,e2 := b.DB.Exec(`DELETE FROM msgidbkt m WHERE m.expir <= $1 ;`,expire)
		errs <- e2
	}()
	for i := 0; i<n; i++ {
		e := <-errs
		if e!=nil { err = e }
	}
	
	return err
}

/* =================================================================================== */

func (b *Base) GroupHeadInsert(groups [][]byte, buf []int64) (nums []int64, e error) {
	{
		l := len(groups)
		if cap(buf)<l { buf = make([]int64,l) }
		buf = buf[:l]
		nums = buf
	}
	tx,err := b.DB.Begin()
	if err!=nil { return nil,err }
	defer func() {
		if e!=nil {
			tx.Rollback()
			return
		}
		
		err := tx.Commit()
		if err==nil {
			e = err
		}
	}()
	for i,group := range groups {
		tx.Exec(`
		UPDATE
			ngrpcnt
		SET
			latest = latest + 1
		WHERE
			ngrp = $1
		;`,group)
		e = tx.QueryRow(`
		SELECT
			n.latest
		FROM
			ngrpcnt n
		WHERE
			n.ngrp = $1
		;`,group).Scan(&buf[i])
		if e!=nil { return }
	}
	
	return
}

/* PostgreSQL specific variant/Optimization */
func (b *PgBase) GroupHeadInsert(groups [][]byte, buf []int64) (nums []int64, e error) {
	{
		l := len(groups)
		if cap(buf)<l { buf = make([]int64,l) }
		buf = buf[:l]
		nums = buf
	}
	for i,group := range groups {
		e = b.DB.QueryRow(`
		UPDATE	ngrpcnt
		SET	latest = latest + 1
		WHERE	ngrp = $1
		RETURNING
			latest;
		`,group).Scan(&buf[i])
		if e!=nil { return }
	}
	return
}

func (b *Base) GroupHeadRevert(groups [][]byte, nums []int64) error {
	for i,group := range groups {
		b.DB.Exec(`
		UPDATE
			ngrpcnt
		SET
			latest = latest - 1
		WHERE
			latest = $1,
			ngrp = $2
		;`,nums[i],group)
	}
	return nil
}

func (b *Base) GroupRealtimeQuery(group []byte) (number int64, low int64, high int64, ok bool) {
	err := b.DB.QueryRow(`
	SELECT
		COALESCE(n.narts,0),COALESCE(n.low,0),COALESCE(n.high,0)
	FROM
		ngrpstat n RIGHT JOIN ngrpcnt m on n.ngrp=m.ngrp
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
		m.ngrp,COALESCE(n.high,0),COALESCE(n.low,0),m.status
	FROM
		ngrpstat n RIGHT JOIN ngrpcnt m on n.ngrp=m.ngrp
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
			if high ==0 { low = 0 }
			targ(group,high,low,byte(status))
		}
	}
	return true
}

func (b *Base) GroupStaticList(targ func(group []byte, descr []byte)) bool {
	rows,err := b.DB.Query(`
	SELECT
		ngrp,descr
	FROM
		ngrpstatic
	;`)
	if err!=nil { return false }
	var group,descr sql.RawBytes
	scn := []interface{}{&group,&descr}
	for rows.Next() {
		if rows.Scan(scn...)==nil {
			targ(group,descr)
		}
	}
	return true
}

