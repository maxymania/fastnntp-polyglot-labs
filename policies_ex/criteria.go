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


package policies_ex

import "regexp"
import "github.com/maxymania/fastnntp-polyglot-labs/policies_ex/format"

/*
 * This is a regular expression can never be matched.
 *
 * This is done by constructing a regexp class containing ALL characters:
 *
 *	Define a character class, that:
 *		Contains all Word Characters ('\w')
 *		Contains all Non-Word Characters ('\W', or in other words, all characters, that not in '\w')
 *	The combination of \w and \W ([\w\W]) results in a character class, that contains ALL characters.
 *
 *	Negate this character class ([^\w\W]).
 *	That results in a character class, that DOES NOT CONTAIN ANY CHARACTER.
 */
//const re_Impossible = `[^\w\W]`

var wildmat = regexp.MustCompile(`[\*\?\,\;]`)

func wildmat2regexp(wm,def string) string {
	if wm=="" { return def }
	s := make([]byte,0,len(wm)*2)
	s = append(s,'^','(')
	pss := wildmat.FindAllStringIndex(wm,-1)
	i := 0
	for _,p := range pss {
		s = append(s,regexp.QuoteMeta(wm[i:p[0]])...)
		i = p[1]
		switch wm[p[0]] {
		case '*': s = append(s,".*"...)
		case '?': s = append(s,"."...)
		case ',',';': s = append(s,"|"...)
		}
	}
	s = append(s,regexp.QuoteMeta(wm[i:])...)
	s = append(s,')','$')
	return string(s)
}

type MatcherConfig struct{
	Newsgroups string `inn:"newsgroups"`
	Except     string `inn:"except"`
	Exclude    string `inn:"exclude"`
	Size   format.Range64 `inn:"size"`
	Lines  format.Range64 `inn:"lines"`
}

type Matcher struct{
	Newsgroups,Except,Exclude *regexp.Regexp
	
	SizeMin, SizeMax, LinesMin, LinesMax int64
}
func (c *MatcherConfig) Build(m *Matcher) {
	m.Newsgroups = regexp.MustCompile(wildmat2regexp(c.Newsgroups,".*"))
	m.Except = regexp.MustCompile(wildmat2regexp(c.Except,`^[\n]*$`))
	m.Exclude = regexp.MustCompile(wildmat2regexp(c.Exclude,`^[\n]*$`))
	m.SizeMin  = c.Size.Min
	m.SizeMax  = c.Size.Max
	m.LinesMin = c.Lines.Min
	m.LinesMax = c.Lines.Max
}

func (m *Matcher) Match(groups [][]byte, lines, length int64) bool {
	mf := false
	for _,group := range groups {
		if !m.Newsgroups.Match(group) { continue }
		if m.Except.Match(group) { continue }
		mf = true
		break
	}
	if !mf { return false } /* Fast Path */
	for _,group := range groups {
		if !m.Exclude.Match(group) { continue }
		return false
	}
	
	if length<m.SizeMin { return false }
	if (m.SizeMax!=0) && (m.SizeMax<length) { return false }
	
	if lines<m.LinesMin { return false }
	if (m.LinesMax!=0) && (m.LinesMax<length) { return false }
	
	return true
}
