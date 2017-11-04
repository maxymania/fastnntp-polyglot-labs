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


package cluster

import "github.com/maxymania/fastnntp-polyglot-labs/bucketstore"
import "github.com/maxymania/fastnntp-polyglot-labs/bucketstore/remote"
import "github.com/hashicorp/memberlist"
import "github.com/vmihailenco/msgpack"
import "net"
import "sync"
import "github.com/valyala/fasthttp"

const (
	Proto_HTTP = iota
	Proto_OO
	Proto_KCP
)

type NodeError int
func (a NodeError) Error() string { return "Node Error!" }

var ENodeError error = NodeError(0)

type KCPOptions struct{
	_msgpack struct{} `msgpack:",asArray"`
	DataShards   int
	ParityShards int
	Salsa20Key   []byte
	TurboMode    bool
}

type MetaData struct{
	Buckets []string
	Proto   uint
	Port    uint
	KCP     KCPOptions
}
type ClientLoader func(*MetaData,net.IP) remote.HttpClient

var ClientPlugins = map[uint]ClientLoader{
	Proto_HTTP: ccHttp,
	Proto_OO  : ccOO,
	Proto_KCP : ccKcp,
}

type ListenerAndServer func(*MetaData,fasthttp.RequestHandler)

var ServerPlugins = map[uint]ListenerAndServer{
	Proto_HTTP: lasHttp,
	Proto_OO  : lasOO,
	Proto_KCP : lasKCP,
}

type OtherMember struct{
	Client remote.HttpClient
	Meta MetaData
}
type MemberMap map[string]*OtherMember

type Membered struct{
	Router *remote.BucketRouter
	Meta   MetaData
	
	Ml     sync.Mutex
	Member MemberMap
}
func NewMembered() *Membered{
	return &Membered{
		Router:remote.NewBucketRouter(),
		Member:make(MemberMap),
	}
}
func (m *Membered) ListenAndServe() {
	las := ServerPlugins[m.Meta.Proto]
	if las!=nil { las(&m.Meta,m.Router.Handler) }
}
func (m *Membered) AddLocal(bu *bucketstore.Bucket) {
	m.Meta.Buckets = append(m.Meta.Buckets,bu.Uuid)
	m.Router.AddLocal(bu)
}
func (m *Membered) NodeMeta(limit int) []byte {
	b,_ := msgpack.Marshal(&m.Meta)
	if len(b)>limit { return nil }
	return b
}
func (m *Membered) NotifyMsg([]byte){}
func (m *Membered) GetBroadcasts(overhead, limit int) [][]byte { return nil }
func (m *Membered) LocalState(join bool) []byte { return nil }
func (m *Membered) MergeRemoteState(buf []byte, join bool) {}
func (m *Membered) NotifyUpdate(n *memberlist.Node) {
	m.NotifyLeave(n)
	m.NotifyJoin(n)
}
func (m *Membered) NotifyJoin(n *memberlist.Node) {
	member := new(OtherMember)
	if msgpack.Unmarshal(n.Meta,&member.Meta)!=nil { return }
	if len(member.Meta.Buckets)==0 { return }
	ldr := ClientPlugins[member.Meta.Proto]
	if ldr==nil { return }
	m.Ml.Lock(); defer m.Ml.Unlock()
	if _,ok := m.Member[n.Name] ; ok { return } // Unlikely...
	member.Client = ldr(&member.Meta,n.Addr)
	if member.Client==nil { return }
	m.Member[n.Name] = member
	m.Router.AddNode2(member.Meta.Buckets,member.Client)
}
func (m *Membered) NotifyLeave(n *memberlist.Node) {
	m.Ml.Lock(); defer m.Ml.Unlock()
	member := m.Member[n.Name]
	if member==nil { return }
	delete(m.Member,n.Name)
	m.Router.Remove2(member.Meta.Buckets)
	type lDestroy interface{ Destroy() }
	if ld,ok := member.Client.(lDestroy) ; ok { ld.Destroy() }
}
func (m *Membered) NotifyMerge(peers []*memberlist.Node) error {
	md := &MetaData{}
	for _,peer := range peers {
		if msgpack.Unmarshal(peer.Meta,md)!=nil { return ENodeError }
	}
	return nil
}
func (m *Membered) NotifyAlive(peer *memberlist.Node) error {
	if msgpack.Unmarshal(peer.Meta,&MetaData{})!=nil { return ENodeError }
	return nil
}


