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

import "fmt"
import "database/sql"
import "text/template"
import "bytes"
import "time"
import "github.com/maxymania/fastnntp-polyglot-labs/bufferex"
import "github.com/maxymania/fastnntp-polyglot-labs/util/sqlutil"

type Dialect struct{
	Binary, Int64, Date string
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
	CREATE TABLE newsgroups (
		ngrp {{.Binary}} PRIMARY KEY,
		dscr {{.Binary}}
	);
	CREATE TABLE newsactive (
		ngrp   {{.Binary}} PRIMARY KEY,
		status {{.Byte}},
		ganlst {{.Int64}}
	);
	
	CREATE MATERIALIZED VIEW artcache AS
		SELECT
			ngrp,
			count(mnum) nmsg,
			min(mnum)   low,
			max(mnum)   high
		FROM ngrpnumvalue
		GROUP BY ngrp
	WITH DATA;
	CREATE VIEW artnw AS
		SELECT
			ngrp,
			count(mnum) nmsg,
			min(mnum)   low,
			max(mnum)   high
		FROM (
			SELECT
				l.ngrp ngrp,
				l.mnum mnum
			FROM ngrpnumvalue l LEFT JOIN artcache a ON a.ngrp = l.ngrp
			WHERE
				a.high IS NULL OR a.high < l.mnum
		) t
		GROUP BY ngrp
	;
	CREATE VIEW artview AS
		SELECT
			COALESCE(c.ngrp,d.ngrp) ngrp,
			COALESCE(c.nmsg,0) + COALESCE(d.nmsg) nmsg,
			COALESCE(c.low,d.low) low,
			COALESCE(d.high,c.high) high
		FROM
			artcache c FULL OUTER JOIN artnw d ON c.ngrp=d.ngrp
	;
`))

type Base struct{
	DB *sql.DB
}

func (b *Base) CreateSqlModel(d *sqlutil.Dialect) error {
	if d!=sqlutil.PgDialect { return fmt.Errorf("Support only PostgreSQL") }
	buf := new(bytes.Buffer)
	createTables.Execute(buf, d)
	_,err := b.DB.Exec(buf.String())
	return err
}

func (b *Base) InsertGoupMapping(group []byte, num int64, msgid []byte, expire time.Time) error {
	_,err := b.DB.Exec(`INSERT INTO ngrpnumvalue (ngrp,mnum,msgid,expir) VALUES ($1,$2,$3,$4);`,group,num,msgid,expire)
	return err
}
func (b *Base) InsertIDMapping(msgid, bucket []byte, expire time.Time) error {
	_,err := b.DB.Exec(`INSERT INTO msgidbkt (msgid,bucket,expir) VALUES ($1,$2,$3);`,msgid,bucket,expire)
	return err
}

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
func (b *Base) Expire(expire time.Time) error {
	_,e1 := b.DB.Exec(`DELETE FROM ngrpnumvalue n WHERE n.expir <= $1 ;`,expire)
	_,e2 := b.DB.Exec(`DELETE FROM msgidbkt m WHERE m.expir <= $1 ;`,expire)
	if e1==nil { e1=e2 }
	return e1
}

