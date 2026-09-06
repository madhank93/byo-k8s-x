package main

import (
	"fmt"
	"strings"
)

// diffContext is how many unchanged lines to keep around each change,
// matching the readability of a typical unified diff.
const diffContext = 2

// op is one line of a line-level edit script: ' ' unchanged, '-' present
// only in old, '+' present only in new.
type op struct {
	kind byte
	text string
}

// unifiedDiff renders the change from old to new in unified-diff style and
// reports whether the two differ at all. Reference snapshots are cumulative
// programs, so this is what lets a stage show only the code it added.
func unifiedDiff(old, new string) (out string, changed bool) {
	if old == new {
		return "", false
	}
	return format(lineOps(strings.Split(old, "\n"), strings.Split(new, "\n"))), true
}

// lineOps computes a minimal line-level edit script via a standard LCS
// dynamic program. A stage's program tops out in the low hundreds of lines,
// so the O(n·m) table is cheap enough to build on demand.
func lineOps(old, new []string) []op {
	n, m := len(old), len(new)
	dp := make([][]int, n+1)
	for i := range dp {
		dp[i] = make([]int, m+1)
	}
	for i := n - 1; i >= 0; i-- {
		for j := m - 1; j >= 0; j-- {
			switch {
			case old[i] == new[j]:
				dp[i][j] = dp[i+1][j+1] + 1
			case dp[i+1][j] >= dp[i][j+1]:
				dp[i][j] = dp[i+1][j]
			default:
				dp[i][j] = dp[i][j+1]
			}
		}
	}
	var ops []op
	i, j := 0, 0
	for i < n && j < m {
		switch {
		case old[i] == new[j]:
			ops = append(ops, op{' ', old[i]})
			i++
			j++
		case dp[i+1][j] >= dp[i][j+1]:
			ops = append(ops, op{'-', old[i]})
			i++
		default:
			ops = append(ops, op{'+', new[j]})
			j++
		}
	}
	for ; i < n; i++ {
		ops = append(ops, op{'-', old[i]})
	}
	for ; j < m; j++ {
		ops = append(ops, op{'+', new[j]})
	}
	return ops
}

// format renders an edit script as unified-diff-style text (a leading
// "+"/"-"/" " per line, no hunk headers), collapsing long unchanged runs to
// a short elision marker so the output stays on what actually changed.
func format(ops []op) string {
	var out []string
	n := len(ops)
	for i := 0; i < n; {
		if ops[i].kind != ' ' {
			out = append(out, string(ops[i].kind)+ops[i].text)
			i++
			continue
		}
		j := i
		for j < n && ops[j].kind == ' ' {
			j++
		}
		runLen := j - i
		atStart, atEnd := i == 0, j == n
		switch {
		case atStart && atEnd:
			for k := i; k < j; k++ {
				out = append(out, " "+ops[k].text)
			}
		case atStart:
			start := i
			if skip := runLen - diffContext; skip > 0 {
				out = append(out, fmt.Sprintf(" … %d unchanged lines …", skip))
				start = j - diffContext
			}
			for k := start; k < j; k++ {
				out = append(out, " "+ops[k].text)
			}
		case atEnd:
			show := min(diffContext, runLen)
			for k := i; k < i+show; k++ {
				out = append(out, " "+ops[k].text)
			}
			if runLen > show {
				out = append(out, fmt.Sprintf(" … %d unchanged lines …", runLen-show))
			}
		case runLen <= diffContext*2:
			for k := i; k < j; k++ {
				out = append(out, " "+ops[k].text)
			}
		default:
			for k := i; k < i+diffContext; k++ {
				out = append(out, " "+ops[k].text)
			}
			out = append(out, fmt.Sprintf(" … %d unchanged lines …", runLen-diffContext*2))
			for k := j - diffContext; k < j; k++ {
				out = append(out, " "+ops[k].text)
			}
		}
		i = j
	}
	return strings.Join(out, "\n")
}
