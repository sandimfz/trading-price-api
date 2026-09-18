package tvsocket

import (
	"bufio"
	"errors"
	"strconv"
	"strings"
)

// ErrInvalidFrame adalah sentinel error untuk frame yang tidak sesuai format ~m~{len}~m~{payload}.
var ErrInvalidFrame = errors.New("invalid tv frame format")

// splitTVFrame adalah bufio.SplitFunc untuk memisahkan pesan
// dengan format ~m~{length}~m~{payload}.
func splitTVFrame(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	const prefix = "~m~"
	if !strings.HasPrefix(string(data), prefix) {
		return 0, nil, nil
	}

	rest := data[len(prefix):]
	sep := strings.Index(string(rest), prefix)
	if sep == -1 {
		// belum lengkap, tunggu data lebih banyak
		return 0, nil, nil
	}

	length, err := strconv.Atoi(string(rest[:sep]))
	if err != nil {
		return 0, nil, ErrInvalidFrame
	}

	payloadStart := len(prefix) + sep + len(prefix)
	if len(data) < payloadStart+length {
		// payload belum lengkap
		return 0, nil, nil
	}

	payload := data[payloadStart : payloadStart+length]
	return payloadStart + length, payload, nil
}

// NewFrameScanner membuat bufio.Scanner dengan custom split function.
// Gunakan buffer besar karena payload JSON bisa panjang.
func NewFrameScanner(r *bufio.Reader) *bufio.Scanner {
	scanner := bufio.NewScanner(r)
	scanner.Split(splitTVFrame)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	return scanner
}

// frameMessage membungkus payload dengan format ~m~{len}~m~{payload}.
func frameMessage(payload string) string {
	return "~m~" + strconv.Itoa(len(payload)) + "~m~" + payload
}
