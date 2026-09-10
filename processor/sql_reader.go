package processor

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
)

// readSQL frames SQL without treating delimiters inside literals or comments as
// statement boundaries. It preserves all input bytes, including the EOF tail.
// Emitted strings remain immutable and can be queued for parallel processing.
func readSQL(ctx context.Context, input io.Reader, maxBytes int64, emit func(string, string) error) error {
	r := bufio.NewReaderSize(input, 64<<10)
	var out strings.Builder
	capacityHint := 0
	delimiter := ";"
	var quote byte
	var escaped, lineComment, blockComment, hasSQL bool
	var carry []byte
	chunk := make([]byte, 64<<10)
	emptyReads := 0
	appendBytes := func(data []byte) error {
		if int64(out.Len())+int64(len(data)) > maxBytes {
			return fmt.Errorf("SQL statement exceeds maxStatementBytes (%d)", maxBytes)
		}
		if out.Cap() == 0 && len(data) > 0 {
			// Extended INSERTs in a dump usually have similar sizes. Reserve
			// from the last statement to avoid repeatedly copying a growing
			// buffer. Each emitted string still owns its immutable allocation.
			out.Grow(max(len(data), capacityHint))
		}
		out.Write(data)
		return nil
	}
	flush := func() error {
		err := emit(out.String(), delimiter)
		capacityHint = min(out.Len(), 1<<20)
		if capacityHint > 64<<10 {
			// Round large reservations to a read chunk so slightly larger
			// subsequent statements do not immediately grow again.
			capacityHint = (capacityHint + (64 << 10) - 1) &^ ((64 << 10) - 1)
		}
		if int64(capacityHint) > maxBytes {
			capacityHint = int(maxBytes)
		}
		out = strings.Builder{}
		hasSQL = false
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, readErr := r.Read(chunk)
		if n == 0 && readErr == nil {
			emptyReads++
			if emptyReads >= 100 {
				return io.ErrNoProgress
			}
		} else {
			emptyReads = 0
		}
		data := chunk[:n]
		if len(carry) > 0 {
			data = append(carry, data...)
			carry = nil
		}
		limit := len(data)
		start, i := 0, 0
		for i < limit {
			b := data[i]
			if quote != 0 {
				if escaped {
					// An odd trailing backslash run in the previous chunk
					// escapes the first byte of this chunk.
					escaped = false
					i++
					continue
				}
				// Only the matching quote can end a literal. Search for it
				// using the standard library's optimized byte search, then
				// inspect the immediately preceding backslash run for parity.
				end := limit
				if offset := bytes.IndexByte(data[i:], quote); offset >= 0 {
					end = i + offset
				}
				if quote != '`' {
					for j := end - 1; j >= i && data[j] == '\\'; j-- {
						escaped = !escaped
					}
				}
				i = end
				if i == limit {
					continue
				}
				if escaped {
					escaped = false
					i++
					continue
				}
				b = data[i]
			}
			// Carry only ambiguous suffixes into the next read. In particular,
			// emit a complete statement immediately, without waiting for a
			// newline or EOF from a live producer.
			if limit-i < 32 && readErr == nil && !lineComment {
				tail := data[i:]
				if (blockComment && b == '*' && len(tail) == 1) ||
					(quote != 0 && !escaped && b == quote && len(tail) == 1) {
					break
				}
				if !blockComment && quote == 0 {
					completeDelimiter := len(tail) >= len(delimiter) && string(tail[:len(delimiter)]) == delimiter
					if !completeDelimiter && ((b == '/' && len(tail) == 1) || (b == '-' && len(tail) < 3) ||
						(len(tail) < len(delimiter) && strings.HasPrefix(delimiter, string(tail)))) {
						break
					}
					if !hasSQL && (b == 'D' || b == 'd') && len(tail) <= 9 &&
						strings.HasPrefix("DELIMITER", strings.ToUpper(string(tail))) {
						break
					}
				}
			}
			if lineComment {
				if b == '\n' {
					lineComment = false
				}
				i++
				continue
			}
			if blockComment {
				if b == '*' && i+1 < len(data) && data[i+1] == '/' {
					blockComment = false
					i += 2
				} else {
					i++
				}
				continue
			}
			if quote != 0 {
				if i+1 < len(data) && data[i+1] == quote {
					i += 2
					continue
				}
				quote = 0
				i++
				continue
			}
			if b == delimiter[0] && len(data)-i >= len(delimiter) && string(data[i:i+len(delimiter)]) == delimiter {
				i += len(delimiter)
				if err := appendBytes(data[start:i]); err != nil {
					return err
				}
				if err := flush(); err != nil {
					return err
				}
				start = i
				continue
			}
			if b == '#' {
				lineComment = true
				i++
				continue
			}
			if b == '-' && i+2 < len(data) && data[i+1] == '-' && isSQLSpace(data[i+2]) {
				lineComment = true
				i += 2
				continue
			}
			if b == '/' && i+1 < len(data) && data[i+1] == '*' {
				blockComment = true
				i += 2
				continue
			}
			if !hasSQL && (b == 'D' || b == 'd') && len(data)-i >= 10 && strings.EqualFold(string(data[i:i+9]), "DELIMITER") && isSQLSpace(data[i+9]) {
				rest := data[i+9:]
				end := bytes.IndexByte(rest, '\n')
				if end >= 0 {
					rest = rest[:end+1]
				}
				if len(rest) > 128 {
					return fmt.Errorf("invalid DELIMITER directive")
				}
				if end < 0 && readErr == nil {
					break
				}
				fields := strings.Fields(string(rest))
				if len(fields) != 1 || len(fields[0]) > 16 || strings.ContainsAny(fields[0], "'\"`\\") {
					return fmt.Errorf("invalid DELIMITER directive")
				}
				i += 9 + len(rest)
				if err := appendBytes(data[start:i]); err != nil {
					return err
				}
				if err := flush(); err != nil {
					return err
				}
				delimiter = fields[0]
				start = i
				continue
			}
			if b == '\'' || b == '"' || b == '`' {
				quote = b
			}
			if !isSQLSpace(b) {
				hasSQL = true
			}
			i++
		}
		if err := appendBytes(data[start:i]); err != nil {
			return err
		}
		if i < len(data) {
			carry = append([]byte(nil), data[i:]...)
		}
		if readErr == io.EOF {
			if quote != 0 || blockComment {
				return fmt.Errorf("unterminated SQL literal or block comment at EOF")
			}
			if out.Len() > 0 {
				return flush()
			}
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

func isSQLSpace(b byte) bool { return b <= ' ' }

type sqlToken struct {
	text   string
	quoted bool
}
type sqlPrefix struct {
	sql string
	pos int
}

// next is only used for the short statement header. Unlike a multiline regex,
// it cannot mistake text inside a literal or a routine body for a new INSERT.
func (s *sqlPrefix) next() sqlToken {
	for s.pos < len(s.sql) {
		p := s.sql[s.pos:]
		if isSQLSpace(p[0]) {
			s.pos++
			continue
		}
		if strings.HasPrefix(p, "*/") {
			s.pos += 2
			continue
		}
		if p[0] == '#' || (strings.HasPrefix(p, "--") && (len(p) == 2 || isSQLSpace(p[2]))) {
			if i := strings.IndexByte(p, '\n'); i >= 0 {
				s.pos += i + 1
				continue
			}
			s.pos = len(s.sql)
			return sqlToken{}
		}
		if strings.HasPrefix(p, "/*!") {
			s.pos += 3
			for s.pos < len(s.sql) && s.sql[s.pos] >= '0' && s.sql[s.pos] <= '9' {
				s.pos++
			}
			continue
		}
		if strings.HasPrefix(p, "/*") {
			if i := strings.Index(p[2:], "*/"); i >= 0 {
				s.pos += i + 4
				continue
			}
			s.pos = len(s.sql)
			return sqlToken{}
		}
		break
	}
	if s.pos == len(s.sql) {
		return sqlToken{}
	}
	start := s.pos
	b := s.sql[s.pos]
	s.pos++
	if b == '`' || b == '"' || b == '\'' {
		for s.pos < len(s.sql) {
			if s.sql[s.pos] == b {
				s.pos++
				if s.pos < len(s.sql) && s.sql[s.pos] == b {
					s.pos++
					continue
				}
				return sqlToken{strings.ReplaceAll(s.sql[start+1:s.pos-1], string([]byte{b, b}), string(b)), true}
			}
			if s.sql[s.pos] == '\\' && b != '`' && s.pos+1 < len(s.sql) {
				s.pos++
			}
			s.pos++
		}
		return sqlToken{s.sql[start:], true}
	}
	if strings.ContainsRune(".;(),=", rune(b)) {
		return sqlToken{text: string(b)}
	}
	for s.pos < len(s.sql) {
		b = s.sql[s.pos]
		tail := s.sql[s.pos:]
		if strings.HasPrefix(tail, "/*") || strings.HasPrefix(tail, "*/") || b == '#' ||
			(strings.HasPrefix(tail, "--") && (len(tail) == 2 || isSQLSpace(tail[2]))) {
			break
		}
		if isSQLSpace(b) || strings.ContainsRune(".;(),=`'\"", rune(b)) {
			break
		}
		s.pos++
	}
	return sqlToken{text: s.sql[start:s.pos]}
}
func (t sqlToken) is(word string) bool { return !t.quoted && strings.EqualFold(t.text, word) }

type statementInfo struct{ kind, database, table string }

func identifySQL(line string) statementInfo {
	s := sqlPrefix{sql: line}
	first := s.next()
	info := statementInfo{}
	switch {
	case first.is("CREATE"):
		t := s.next()
		if t.is("TEMPORARY") {
			t = s.next()
		}
		if !t.is("TABLE") {
			return info
		}
		info.kind = "create"
		t = s.next()
		if t.is("IF") {
			if !s.next().is("NOT") || !s.next().is("EXISTS") {
				return statementInfo{}
			}
			t = s.next()
		}
		info.table = t.text
	case first.is("INSERT"):
		info.kind = "insert"
		t := s.next()
		if t.is("LOW_PRIORITY") || t.is("HIGH_PRIORITY") || t.is("DELAYED") {
			t = s.next()
		}
		if t.is("IGNORE") {
			t = s.next()
		}
		if t.is("INTO") {
			t = s.next()
		}
		info.table = t.text
	case first.is("USE"):
		return statementInfo{kind: "use", database: s.next().text}
	default:
		return info
	}
	if s.next().text == "." {
		info.database = info.table
		info.table = s.next().text
	}
	return info
}

// Count tuple separators only outside strings and comments, without allocating
// a match slice. The first tuple accounts for the initial row.
func countInsertRows(line string) int {
	count, depth := 1, 0
	var quote byte
	var escaped, block, comment, afterTuple bool
	for i := 0; i < len(line); i++ {
		b := line[i]
		if comment {
			if b == '\n' {
				comment = false
			}
			continue
		}
		if block {
			if b == '*' && i+1 < len(line) && line[i+1] == '/' {
				block = false
				i++
			}
			continue
		}
		if quote != 0 {
			if escaped {
				escaped = false
				continue
			}
			if b == '\\' && quote != '`' {
				escaped = true
				continue
			}
			if b == quote {
				if i+1 < len(line) && line[i+1] == quote {
					i++
				} else {
					quote = 0
				}
			}
			continue
		}
		if b == '#' {
			comment = true
			continue
		}
		if b == '-' && i+2 < len(line) && line[i+1] == '-' && isSQLSpace(line[i+2]) {
			comment = true
			i++
			continue
		}
		if b == '/' && i+1 < len(line) && line[i+1] == '*' {
			if i+2 < len(line) && line[i+2] == '!' {
				i += 2
				continue
			}
			block = true
			i++
			continue
		}
		if b == '\'' || b == '"' || b == '`' {
			quote = b
			afterTuple = false
			continue
		}
		if isSQLSpace(b) {
			continue
		}
		if b == '(' {
			if depth == 0 && afterTuple {
				count++
			}
			depth++
			afterTuple = false
			continue
		}
		if b == ')' {
			depth--
			afterTuple = depth == 0
			continue
		}
		if b != ',' {
			afterTuple = false
		}
	}
	return count
}
