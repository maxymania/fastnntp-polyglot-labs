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


package groupdb

import "database/sql"
import "text/template"
import "bytes"

type AuthRank uint8
const (
	ARReader AuthRank = iota
	ARUser
	ARModerator
	ARFeeder
)

type Dialect struct{
	Binary, Int64, Byte string
}

// PostgreSQL
var PgDialect = &Dialect{
	Binary: "bytea",
	Int64: "bigint",
	Byte: "smallint",
}

// Microsoft SQL Server
// (untested)
var MsSqlDialect = &Dialect{
	Binary: "varbinary",
	Int64: "bigint",
	Byte: "tinyint",
}

var createTables = template.Must(template.New("create").Parse(`
	CREATE TABLE ngrpstatic (
		ngrp  {{.Binary}} PRIMARY KEY,
		descr {{.Binary}}
	);
	CREATE TABLE ngrprt (
		ngrp   {{.Binary}} PRIMARY KEY,
		arcoun {{.Int64}},
		low    {{.Int64}},
		high   {{.Int64}},
		status {{.Byte}}
	);
`))

type Base struct{
	DB *sql.DB
}

func (b *Base) CreateTables(d *Dialect) error {
	buf := new(bytes.Buffer)
	createTables.Execute(buf, d)
	_,err := b.DB.Exec(buf.String())
	return err
}

type AuthBase struct {
	*Base
	Rank AuthRank
}

/*
select ngrp::text,count(mnum),min(mnum),max(mnum) from ngrpnumvalue group by ngrp;
*/

/* No */
func (b *AuthBase) GroupHeadFilter(groups [][]byte) ([][]byte, error) {
	var status byte
	stm,err := b.DB.Prepare(`
	SELECT
		n.status
	FROM
		ngrprt n
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
func (b *Base) AdmPutDescr(group []byte, descr []byte) {
	_,err := b.DB.Exec(`INSERT INTO ngrpstatic (ngrp,descr) VALUES ($1,$2);`,group,descr)
	if err!=nil { b.DB.Exec(`UPDATE ngrpstatic SET descr=$1 WHERE ngrp=$2;`     ,descr,group) }
}
func (b *Base) AdmPutStatus(group []byte, status byte) {
	_,err := b.DB.Exec(`INSERT INTO ngrprt (ngrp,arcoun,low,high,status) VALUES ($1,0,1,0,$2);`,group,int(status))
	if err!=nil { b.DB.Exec(`UPDATE ngrprt SET status=$1 WHERE ngrp=$2;`     ,int(status),group) }
}

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
			ngrprt
		SET
			arcoun = arcoun + 1,
			high = high + 1
		WHERE
			ngrp = $1
		;`,group)
		e = tx.QueryRow(`
		SELECT
			n.high
		FROM
			ngrprt n
		WHERE
			n.ngrp = $1
		;`,group).Scan(&buf[i])
		if e!=nil { return }
	}
	
	return
}

func (b *Base) GroupHeadRevert(groups [][]byte, nums []int64) error {
	for i,group := range groups {
		b.DB.Exec(`
		UPDATE
			ngrprt
		SET
			high = high - 1,
			arcoun = arcoun - 1
		WHERE
			high = $2,
			ngrp = $3
		;`,group,nums[i])
		
	}
	return nil
}

func (b *Base) GroupRealtimeQuery(group []byte) (number int64, low int64, high int64, ok bool) {
	err := b.DB.QueryRow(`
	SELECT
		arcoun,low,high
	FROM
		ngrprt
	WHERE
		ngrp = $1
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
		ngrp,high,low,status
	FROM
		ngrprt
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
		if rows.Scan(scn...)!=nil {
			targ(group,descr)
		}
	}
	return true
}


