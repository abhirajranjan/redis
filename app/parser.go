package main

import (
	"bytes"
	"fmt"
	"io"
	"math/big"
	"strconv"

	"github.com/pkg/errors"
)

type CMD interface {
	Bytes() []byte
}

var ErrInvalidChar = errors.New("invalid char")

func ParseResp(r io.Reader) (CMD, error) {
	b := make([]byte, 1)
	if _, err := r.Read(b); err != nil {
		return nil, err
	}

	switch b[0] {
	case '+':
		return ParseSimpleString(r)
	case '-':
		return ParseSimpleError(r)
	case ':':
		return ParseInt(r)
	case '$':
		return ParseBulkString(r)
	case '*':
		return ParseArray(r)
	case '_':

	case '#':
		return ParseBool(r)
	case ',':
		return ParseDouble(r)
	case '(':
		return ParseBigInt(r)
	case '!':
		return ParseBulkError(r)
	case '=':
		return ParseVerbatimStr(r)
	case '%':
		return ParseMap(r)
	case '~':
		return ParseSet(r)
	case '>':
	}

	return nil, errors.New("invalid starting char")
}

// *** *** //

type SimpleString string

func ParseSimpleString(r io.Reader) (SimpleString, error) {
	s, err := readTillCRLF(r)
	if err != nil {
		return "", err
	}

	return SimpleString(s), nil
}

func (s SimpleString) Bytes() []byte {
	return []byte(fmt.Sprintf("+%s\r\n", s))
}

// *** *** //

type SimpleError string

func ParseSimpleError(r io.Reader) (redisErr SimpleError, err error) {
	s, err := readTillCRLF(r)
	return SimpleError(s), err
}

func (err SimpleError) Bytes() []byte {
	return []byte(fmt.Sprintf("-%s\r\n", err))
}

// *** *** //

type Int int64

func ParseInt(r io.Reader) (Int, error) {
	num, err := readTillCRLF(r)
	if err != nil {
		return 0, err
	}

	i, err := strconv.ParseInt(num, 10, 64)
	if err != nil {
		return 0, err
	}

	return Int(i), nil
}

func (i Int) Bytes() []byte {
	return []byte(fmt.Sprintf(":%d\r\n", i))
}

// *** *** //

type BulkString string

func ParseBulkString(r io.Reader) (s BulkString, err error) {
	len, err := ParseInt(r)
	if err != nil {
		return "", err
	}

	if len < 0 {
		return "", nil
	}

	bytes := make([]byte, len)
	if len > 0 {
		if _, err := r.Read(bytes[:]); err != nil {
			return "", errors.Wrap(ErrInvalidChar, "Read")
		}
	}

	var crlf [2]byte
	if _, err := r.Read(crlf[:]); err != nil {
		return "", errors.Wrap(ErrInvalidChar, "Read")
	}

	return BulkString(bytes), nil
}

func (s BulkString) Bytes() []byte {
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(s), s))
}

// *** *** //

type Array []CMD

func ParseArray(r io.Reader) (cmdArr Array, err error) {
	numArgs, err := ParseInt(r)
	if err != nil {
		return nil, err
	}

	if numArgs < 0 {
		return Array{}, nil
	}

	cmdArr = make(Array, numArgs)
	for i := 0; i < int(numArgs); i++ {
		cmdArr[i], err = ParseResp(r)
		if err != nil {
			return nil, err
		}
	}

	return cmdArr, nil
}

func (a Array) Bytes() []byte {
	r := []byte(fmt.Sprintf("*%d\r\n", len(a)))
	for _, i := range a {
		r = append(r, i.Bytes()...)
	}
	return r
}

// *** *** //

type Bool bool

func ParseBool(r io.Reader) (Bool, error) {
	var b [1]byte
	if _, err := r.Read(b[:]); err != nil {
		return false, errors.Wrap(ErrInvalidChar, "Read")
	}

	defer func() {
		var crlf [2]byte
		r.Read(crlf[:])
	}()

	if b[0] == 'f' {
		return false, nil
	}
	if b[0] == 't' {
		return true, nil
	}

	return false, errors.Wrap(ErrInvalidChar, "expected t|f")
}

func (b Bool) Bytes() []byte {
	bs := 't'
	if !b {
		bs = 'f'
	}
	return []byte(fmt.Sprintf("#%c\r\n", bs))
}

// *** *** //

type Double float64

func ParseDouble(r io.Reader) (Double, error) {
	data, err := readTillCRLF(r)
	if err != nil {
		return 0, err
	}

	f, err := strconv.ParseFloat(data, 64)
	if err != nil {
		return 0, err
	}

	return Double(f), nil
}

func (f Double) Bytes() []byte {
	return []byte(fmt.Sprintf(",%s\r\n", strconv.FormatFloat(float64(f), 'e', -1, 64)))
}

// *** *** //

type BigInt struct {
	*big.Int
}

func ParseBigInt(r io.Reader) (BigInt, error) {
	num, err := readTillCRLF(r)
	if err != nil {
		return BigInt{}, err
	}

	var bigInt BigInt
	if _, ok := bigInt.SetString(num, 10); !ok {
		return BigInt{}, errors.New("invalid big Int")
	}

	return bigInt, nil
}

func (b BigInt) Bytes() []byte {
	return []byte(fmt.Sprintf("(%s\r\n", b.String()))
}

// *** *** //

type BulkError string

func ParseBulkError(r io.Reader) (BulkError, error) {
	len, err := ParseInt(r)
	if err != nil {
		return "", err
	}

	errB := make([]byte, len)
	if _, err := r.Read(errB); err != nil {
		return "", errors.Wrap(err, "Read")
	}

	defer func() {
		var crlf [2]byte
		r.Read(crlf[:])
	}()

	return BulkError(errB), nil
}

func (err BulkError) Bytes() []byte {
	return []byte(fmt.Sprintf("!%d\r\n%s\r\n", len(err), err))
}

// *** *** //

type VerbatimStr struct {
	enc string
	str string
}

func ParseVerbatimStr(r io.Reader) (v VerbatimStr, err error) {
	len, err := ParseInt(r)
	if err != nil {
		return VerbatimStr{}, err
	}

	var encB [3]byte
	if _, err := r.Read(encB[:]); err != nil {
		return VerbatimStr{}, errors.Wrap(err, "Read")
	}

	var b [1]byte
	r.Read(b[:])

	data := make([]byte, len-4)
	if _, err := r.Read(data[:]); err != nil {
		return VerbatimStr{}, err
	}
	return VerbatimStr{enc: string(encB[:]), str: string(data[:])}, nil
}

func (v VerbatimStr) Bytes() []byte {
	return []byte(fmt.Sprintf("=%d\r\n%s:%s\r\n", len(v.enc)+1+len(v.str), v.enc, v.str))
}

// *** *** //

type Map map[CMD]CMD

func ParseMap(r io.Reader) (Map, error) {
	len, err := ParseInt(r)
	if err != nil {
		return nil, err
	}

	mapp := make(Map, len)
	for ; len > 0; len-- {
		key, err := ParseResp(r)
		if err != nil {
			return nil, err
		}

		value, err := ParseResp(r)
		if err != nil {
			return nil, err
		}

		mapp[key] = value
	}

	return mapp, nil
}

func (m Map) Bytes() []byte {
	b := append([]byte("%"), fmt.Sprintf("%d\r\n", len(m))...)
	for k, v := range m {
		b = append(b, k.Bytes()...)
		b = append(b, v.Bytes()...)
	}
	return b
}

// *** *** //

type Set map[CMD]struct{}

func ParseSet(r io.Reader) (Set, error) {
	len, err := ParseInt(r)
	if err != nil {
		return nil, err
	}

	s := make(Set, len)
	for ; len > 0; len-- {
		v, err := ParseResp(r)
		if err != nil {
			return nil, err
		}
		if _, ok := s[v]; !ok {
			s[v] = struct{}{}
		}
	}
	return s, nil
}

func (s Set) Bytes() []byte {
	b := []byte(fmt.Sprintf("~%d\r\n", len(s)))
	for v := range s {
		b = append(b, v.Bytes()...)
	}
	return b
}

// *** *** //

func readTillCRLF(r io.Reader) (string, error) {
	buf := bytes.Buffer{}
	var b [1]byte
	if _, err := r.Read(b[:]); err != nil {
		return "", errors.Wrap(ErrInvalidChar, "Read")
	}

	for b[0] != '\r' {
		if err := buf.WriteByte(b[0]); err != nil {
			return "", errors.Wrap(ErrInvalidChar, "Write")
		}
		if _, err := r.Read(b[:]); err != nil {
			return "", errors.Wrap(ErrInvalidChar, "Read")
		}
	}

	defer func() {
		var b [1]byte
		r.Read(b[:])
	}()

	s := buf.String()
	return s, nil
}
