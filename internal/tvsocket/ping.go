package tvsocket

import (
	"strconv"
	"strings"
)

const pingPrefix = "~h~"

// isPing mengecek apakah token adalah pesan ping ~h~{n}.
func isPing(token []byte) bool {
	return strings.HasPrefix(string(token), pingPrefix)
}

// pingNumber mengambil angka dari pesan ping ~h~{n}, dikirim balik apa adanya.
func pingNumber(token []byte) string {
	return strings.TrimPrefix(string(token), pingPrefix)
}

// echoPing membentuk balasan ping dalam format yang sama.
func echoPing(token []byte) string {
	n := pingNumber(token)
	if n == "" {
		return pingPrefix
	}
	// echo nomor yang diterima; tambah 1 mengikuti konvensi socket.io
	i, err := strconv.Atoi(n)
	if err != nil {
		return pingPrefix + n
	}
	return pingPrefix + strconv.Itoa(i+1)
}
