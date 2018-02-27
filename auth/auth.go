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


package auth

import "github.com/byte-mug/fastnntp"
import "github.com/maxymania/fastnntp-polyglot/postauth"
import "github.com/maxymania/fastnntp-polyglot/caps"

type LoginHook interface{
	CheckUser(user []byte) bool
	AuhtUser(user, password []byte, h *fastnntp.Handler) (postauth.AuthRank,bool,*fastnntp.Handler)
}

type Authenticator struct{
	Post   postauth.AuthRank
	Logged bool
	Hook   LoginHook
}

// This Method SHOULD return true, if authentication has already occurred.
func (a *Authenticator) AuthinfoDone(h *fastnntp.Handler) bool {
	return a.Logged
}

// Checks a privilege. Returns true if it is allowed.
func (a *Authenticator) AuthinfoCheckPrivilege(p fastnntp.LoginPriv, h *fastnntp.Handler) bool {
	switch p {
	case fastnntp.LoginPriv_Post: return a.Post.TestStatus('y')
	}
	return true
}

// This Method returns true, if the combination of username is accepted without password.
// The method can optionally return a new Handler object in place of the old one.
func (a *Authenticator) AuthinfoUserOny(user []byte, oldh *fastnntp.Handler) (bool, *fastnntp.Handler) {
	return a.Hook.CheckUser(user),nil
}

// This Method returns true, if the combination of username and password is accepted.
func (a *Authenticator) AuthinfoUserPass(user, password []byte, oldh *fastnntp.Handler) (bool, *fastnntp.Handler) {
	post,logged,nh := a.Hook.AuthUser(user,password,oldh)
	if !logged { return false,nil }
	if nh==nil { nh = oldh }
	nh = doCloneHandler(&Authenticator{post,nil,a.Hook},nh)
	return true,nh
}

func update(r postauth.AuthRank,h *fastnntp.Handler) *fastnntp.Handler {
	caps,ok := h.GroupCaps.(*caps.Caps)
	if !ok { return nil }
	augh,ok := caps.GroupHeadCache.(*postauth.GroupHeadCacheAuthed)
	if !ok { return nil }
	
	nh    := new(fastnntp.Handler)
	ncaps := new(caps.Caps)
	naugh := new(postauth.GroupHeadCacheAuthed)
	*naugh = *augh
	*ncaps = *caps
	*nh    = *h
	
	naugh.Rank           = r
	ncaps.GroupHeadCache = naugh
	nh.GroupCaps         = ncaps
	nh.ArticleCaps       = ncaps
	nh.PostingCaps       = ncaps
	nh.GroupListingCaps  = ncaps
	
	return nh
}

func doCloneHandler(a *Authenticator,h *fastnntp.Handler) *fastnntp.Handler {
	nh := update(a.Post,h)
	if nh==nil {
		nh = new(fastnntp.Handler)
		*nh = *h
	}
	nh.LoginCaps = a
	return nh
}
