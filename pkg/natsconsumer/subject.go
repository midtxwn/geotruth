package natsconsumer

import "strings"

func tokenize(subject string) []string {
	if subject == "" {
		return nil
	}
	return strings.Split(subject, ".")
}

// SubjectMatches reports whether a NATS subject matches a pattern
// that may contain wildcards.
//
// Pattern wildcards:
//   - ">" at the end of a pattern matches one or more remaining tokens.
//     A lone ">" matches any non-empty subject.
//   - "*" matches exactly one token.
//
// The subject must be a concrete subject (no wildcards).
func SubjectMatches(pattern, subject string) bool {
	if pattern == ">" {
		return subject != ""
	}
	if pattern == subject {
		return true
	}

	ptokens := tokenize(pattern)
	stokens := tokenize(subject)

	return matchTokens(ptokens, stokens, 0, 0)
}

func matchTokens(ptokens, stokens []string, pi, si int) bool {
	for {
		if pi == len(ptokens) {
			return si == len(stokens)
		}
		if ptokens[pi] == ">" {
			// A lone ">" (the entire pattern is ">") matches anything,
			// including an empty subject. Otherwise, ">" matches one
			// or more remaining tokens.
			if pi == 0 && len(ptokens) == 1 {
				return true
			}
			return si < len(stokens)
		}
		if si == len(stokens) {
			return false
		}
		if ptokens[pi] == "*" {
			pi++
			si++
			continue
		}
		if ptokens[pi] != stokens[si] {
			return false
		}
		pi++
		si++
	}
}

// patternContains reports whether the container pattern semantically
// contains the contained pattern - that is, every concrete subject
// matched by contained is also matched by container. Both arguments
// may contain wildcards.
//
// Rules:
//   - ">" matches one or more remaining tokens, so it contains any
//     pattern that also has at least one remaining token at this position
//     (concrete, "*", or nested ">"), but NOT a pattern that has ended.
//   - "*" matches exactly one token, so it can contain a concrete token
//     or another "*", but NOT ">" (which can produce multiple tokens).
//   - A concrete token can only contain itself; it cannot contain "*"
//     or ">" (which are broader).
func patternContains(container, contained string) bool {
	ct := tokenize(container)
	dt := tokenize(contained)
	return containsTokens(ct, dt, 0, 0)
}

func containsTokens(ct, dt []string, ci, di int) bool {
	for {
		if ci == len(ct) {
			return di == len(dt)
		}
		if ct[ci] == ">" {
			return di < len(dt)
		}
		if di == len(dt) {
			return false
		}
		if ct[ci] == "*" {
			if dt[di] == ">" {
				return false
			}
			ci++
			di++
			continue
		}
		if dt[di] == ">" || dt[di] == "*" {
			return false
		}
		if ct[ci] != dt[di] {
			return false
		}
		ci++
		di++
	}
}

// subTrie is a subject trie that supports O(k) matching where k is the
// depth of the subject (number of dot-separated tokens). It routes
// incoming messages to subscribers whose patterns (which may contain
// ">" and "*" wildcards) match the message's concrete subject.
type subTrie struct {
	root *trieNode
}

type trieNode struct {
	children map[string]*trieNode
	wcStar   *trieNode
	wcGt     []subEntry
	entries  []subEntry
}

func newSubTrie() *subTrie {
	return &subTrie{root: &trieNode{}}
}

func (t *subTrie) insert(pattern string, entry subEntry) {
	tokens := tokenize(pattern)
	node := t.root
	for _, tok := range tokens {
		switch tok {
		case ">":
			node.wcGt = append(node.wcGt, entry)
			return
		case "*":
			if node.wcStar == nil {
				node.wcStar = &trieNode{}
			}
			node = node.wcStar
		default:
			if node.children == nil {
				node.children = make(map[string]*trieNode)
			}
			child, ok := node.children[tok]
			if !ok {
				child = &trieNode{}
				node.children[tok] = child
			}
			node = child
		}
	}
	node.entries = append(node.entries, entry)
}

func (t *subTrie) remove(pattern string, id uint64) subEntry {
	tokens := tokenize(pattern)
	var removed subEntry
	var found bool
	t.removeWalk(t.root, tokens, 0, id, &removed, &found)
	return removed
}

func (t *subTrie) removeWalk(node *trieNode, tokens []string, idx int, id uint64, removed *subEntry, found *bool) bool {
	if idx == len(tokens) {
		for i, e := range node.entries {
			if e.id == id {
				*removed = e
				*found = true
				node.entries[i] = node.entries[len(node.entries)-1]
				node.entries[len(node.entries)-1] = subEntry{}
				node.entries = node.entries[:len(node.entries)-1]
				break
			}
		}
		return len(node.entries) == 0 && len(node.wcGt) == 0 && node.wcStar == nil && len(node.children) == 0
	}

	tok := tokens[idx]
	prune := false

	switch tok {
	case ">":
		for i, e := range node.wcGt {
			if e.id == id {
				*removed = e
				*found = true
				node.wcGt[i] = node.wcGt[len(node.wcGt)-1]
				node.wcGt[len(node.wcGt)-1] = subEntry{}
				node.wcGt = node.wcGt[:len(node.wcGt)-1]
				break
			}
		}
		prune = len(node.wcGt) == 0
	case "*":
		if node.wcStar != nil {
			prune = t.removeWalk(node.wcStar, tokens, idx+1, id, removed, found)
			if prune {
				node.wcStar = nil
			}
		}
	default:
		if child, ok := node.children[tok]; ok {
			prune = t.removeWalk(child, tokens, idx+1, id, removed, found)
			if prune {
				delete(node.children, tok)
				if len(node.children) == 0 {
					node.children = nil
				}
			}
		}
	}

	return prune && len(node.entries) == 0 && len(node.wcGt) == 0 && node.wcStar == nil && len(node.children) == 0
}

func (t *subTrie) match(subject string) []subEntry {
	tokens := tokenize(subject)
	var result []subEntry
	t.matchWalk(t.root, tokens, 0, &result)
	return result
}

func (t *subTrie) matchWalk(node *trieNode, tokens []string, idx int, result *[]subEntry) {
	// Exact-match entries are only included when we've consumed the entire
	// subject (all tokens matched). Wildcard ">" entries match as long as
	// there is at least one remaining subject token.
	if idx == len(tokens) {
		*result = append(*result, node.entries...)
		// wcGt entries are excluded here because ">" requires at least one
		// remaining subject token. When all tokens are consumed, there are
		// zero remaining tokens, so ">" does not match. All valid ">"
		// matches are handled in the idx < len(tokens) branch below.
		return
	}

	// ">" entries match when there are remaining subject tokens.
	if len(node.wcGt) > 0 {
		*result = append(*result, node.wcGt...)
	}

	tok := tokens[idx]

	if child, ok := node.children[tok]; ok {
		t.matchWalk(child, tokens, idx+1, result)
	}

	if node.wcStar != nil {
		t.matchWalk(node.wcStar, tokens, idx+1, result)
	}
}
