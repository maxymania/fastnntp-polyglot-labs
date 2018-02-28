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


package sqlauth

import "fmt"
import "github.com/byte-mug/fastnntp"
import "database/sql"
import "text/template"
import "golang.org/x/crypto/bcrypt"
import "bytes"
import "github.com/maxymania/fastnntp-polyglot-labs/util/sqlutil"
import "github.com/maxymania/fastnntp-polyglot/postauth"

var createTables = template.Must(template.New("create").Parse(`
	CREATE TABLE userauth (
		usernm {{.Binary}} PRIMARY KEY,
		pcrypt {{.Binary}},
		u_rank {{.Byte}}
	);
`))


type LoginDB struct {
	DB *sql.DB
}

func (l *LoginDB) CreateSqlModel(d *sqlutil.Dialect) error {
	buf := new(bytes.Buffer)
	createTables.Execute(buf, d)
	_,err := l.DB.Exec(buf.String())
	return err
}
func (l *LoginDB) InsertUser(user, password []byte,rank postauth.AuthRank) error {
	b,e := bcrypt.GenerateFromPassword(password,0)
	if e!=nil { return e }
	_,e = l.DB.Exec(`INSERT INTO userauth VALUES ($1,$2,$3)`,user,b,uint8(rank))
	return e
}
func (l *LoginDB) UpdateUserPassword(user, password []byte) error {
	b,e := bcrypt.GenerateFromPassword(password,0)
	if e!=nil { return e }
	res,e := l.DB.Exec(`UPDATE userauth SET pcrypt=$1 WHERE usernm=$2`,b,user)
	if e!=nil { return e }
	if ra,ee := res.RowsAffected() ; ee==nil && ra<1 { e = fmt.Errorf("No such user: %s",user) }
	return e
}
func (l *LoginDB) UpdateUserRank(user []byte, rank postauth.AuthRank) error {
	res,e := l.DB.Exec(`UPDATE userauth SET u_rank=$1 WHERE usernm=$2`,uint8(rank),user)
	if e!=nil { return e }
	if ra,ee := res.RowsAffected() ; ee==nil && ra<1 { e = fmt.Errorf("No such user: %s",user) }
	return e
}

func (l *LoginDB) CheckUser(user []byte) bool {
	var i int
	return l.DB.QueryRow(`SELECT 1 FROM userauth WHERE usernm=$1`,user).Scan(&i)!=nil && i==1
}
func (l *LoginDB) AuhtUser(user, password []byte, h *fastnntp.Handler) (postauth.AuthRank,bool,*fastnntp.Handler) {
	var rb sql.RawBytes
	var rank uint8
	if l.DB.QueryRow(`SELECT pcrypt, u_rank FROM userauth WHERE usernm=$1`,user).Scan(&rb,&rank)!=nil { return 0,false,nil }
	if bcrypt.CompareHashAndPassword(rb,password)!=nil { return 0,false,nil }
	return postauth.AuthRank(rank),true,nil
}
func (l *LoginDB) AuhtUserLite(user, password []byte) (postauth.AuthRank,bool) {
	var rb sql.RawBytes
	var rank uint8
	if l.DB.QueryRow(`SELECT pcrypt, u_rank FROM userauth WHERE usernm=$1`,user).Scan(&rb,&rank)!=nil { return 0,false }
	if bcrypt.CompareHashAndPassword(rb,password)!=nil { return 0,false }
	return postauth.AuthRank(rank),true
}

