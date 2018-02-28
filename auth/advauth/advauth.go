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


package advauth

import "unsafe"
import "sync/atomic"
import "github.com/byte-mug/fastnntp"
import "github.com/maxymania/fastnntp-polyglot/postauth"

type LoginHookLite interface{
	CheckUser(user []byte) bool
	AuhtUserLite(user, password []byte) (postauth.AuthRank,bool)
}

type Deriver interface{
	// This must not return nil
	// The results of this method will be cached.
	DeriveRank(h *fastnntp.Handler,r postauth.AuthRank) *fastnntp.Handler
	
	// This might return nil
	// This hook enables the implementor to implement its own policy
	// based upon the user's name rather than just the rank.
	// This is useful, for example, to implement accounting such as download volume,
	// or to implement per-newsgroup ACLs.
	//
	// Note, that the results will not be cached, please do it yourself.
	DeriveRankUserOpt(h *fastnntp.Handler,r postauth.AuthRank, user []byte) *fastnntp.Handler
}

// Implements Deriver
type DefaultDeriver struct{}
func (d DefaultDeriver) DeriveRank(h *fastnntp.Handler,r postauth.AuthRank) *fastnntp.Handler { panic("Not implemented") }
func (d DefaultDeriver) DeriveRankUserOpt(h *fastnntp.Handler,r postauth.AuthRank, user []byte) *fastnntp.Handler { return nil }

var _ Deriver = DefaultDeriver{}

type Auther struct{
	array [postauth.AR_MAX]unsafe.Pointer
	backl [postauth.AR_MAX]Backlink
	Base  *fastnntp.Handler
	Deriv Deriver
	Hook  LoginHookLite
}

type Backlink struct{
	Rank postauth.AuthRank
}
func (a *Auther) Backlink() {
	for i := range a.backl { a.backl[i] = Backlink{postauth.AuthRank(i)} }
}
func (a *Auther) getFor(r postauth.AuthRank) *fastnntp.Handler {
	if r>=postauth.AR_MAX { r = postauth.AR_MAX-1 }
	NULL := unsafe.Pointer(nil)
	ptr := atomic.LoadPointer(&a.array[r])
	if ptr!=NULL { return (*fastnntp.Handler)(ptr) }
	h := a.Deriv.DeriveRank(a.Base,r)
	if h==nil { panic("Invalid behavoir: a.Deriv.DeriveRank returned <nil>") }
	h.LoginCaps = &a.backl[0]
	ptr = unsafe.Pointer(h)
	atomic.CompareAndSwapPointer(&a.array[r],NULL,ptr)
	return h
}

/* ============================================================================================================== */


// This Method SHOULD return true, if authentication has already occurred.
func (a *Auther) AuthinfoDone(h *fastnntp.Handler) bool {
	return false
}

// Checks a privilege. Returns true if it is allowed.
func (a *Auther) AuthinfoCheckPrivilege(p fastnntp.LoginPriv, h *fastnntp.Handler) bool {
	return false
}

// This Method returns true, if the combination of username is accepted without password.
// The method can optionally return a new Handler object in place of the old one.
func (a *Auther) AuthinfoUserOny(user []byte, oldh *fastnntp.Handler) (bool, *fastnntp.Handler) {
	return a.Hook.CheckUser(user),nil
}

// This Method returns true, if the combination of username and password is accepted.
func (a *Auther) AuthinfoUserPass(user, password []byte, oldh *fastnntp.Handler) (bool, *fastnntp.Handler) {
	rank,ok := a.Hook.AuhtUserLite(user, password)
	if !ok { return false,nil }
	
	nh := a.Deriv.DeriveRankUserOpt(oldh,rank,user)
	
	if nh==nil { nh = a.getFor(rank) }
	return true,nh
}

/* ============================================================================================================== */


// This Method SHOULD return true, if authentication has already occurred.
func (b *Backlink) AuthinfoDone(h *fastnntp.Handler) bool {
	return true
}

// Checks a privilege. Returns true if it is allowed.
func (b *Backlink) AuthinfoCheckPrivilege(p fastnntp.LoginPriv, h *fastnntp.Handler) bool {
	switch p {
	case fastnntp.LoginPriv_Post: return b.Rank.TestStatus('y')
	}
	return true
}

// This Method returns true, if the combination of username is accepted without password.
// The method can optionally return a new Handler object in place of the old one.
func (b *Backlink) AuthinfoUserOny(user []byte, oldh *fastnntp.Handler) (bool, *fastnntp.Handler) {
	return false,nil
}

// This Method returns true, if the combination of username and password is accepted.
func (b *Backlink) AuthinfoUserPass(user, password []byte, oldh *fastnntp.Handler) (bool, *fastnntp.Handler) {
	return false,nil
}

/* ============================================================================================================== */

