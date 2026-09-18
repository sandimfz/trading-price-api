package tvsocket

import (
	"bufio"
	"io"
	"strings"
	"testing"
	"time"
)

func TestSplitTVFrame(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{
			name:  "frame lengkap",
			input: "~m~5~m~hello",
			want:  "hello",
		},
		{
			name:  "frame json",
			input: "~m~18~m~{\"m\":\"qsd\",\"p\":[]}",
			want:  "{\"m\":\"qsd\",\"p\":[]}",
		},
		{
			name:  "multiple frame dalam satu stream",
			input: "~m~2~m~ab~m~3~m~cde",
			want:  "ab",
		},
		{
			name:  "frame terpotong (payload belum lengkap)",
			input: "~m~10~m~abc",
			want:  "",
		},
		{
			name:  "ping frame",
			input: "~m~6~m~~h~123",
			want:  "~h~123",
		},
		{
			name:    "frame malformed (length bukan angka)",
			input:   "~m~abc~m~xyz",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			advance, token, err := splitTVFrame([]byte(tt.input), false)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.want == "" {
				if token != nil {
					t.Fatalf("expected no token yet, got %q (advance=%d)", token, advance)
				}
				return
			}
			if string(token) != tt.want {
				t.Fatalf("token = %q, want %q", token, tt.want)
			}
		})
	}
}

func TestScanMultipleFrames(t *testing.T) {
	input := "~m~2~m~ab~m~2~m~cd~m~2~m~ef"
	scanner := NewFrameScanner(bufio.NewReader(strings.NewReader(input)))

	var got []string
	for scanner.Scan() {
		got = append(got, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan error: %v", err)
	}
	want := []string{"ab", "cd", "ef"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestScanIncrementalFeed(t *testing.T) {
	// simulasi partial read: kirim data byte per byte lewat io.Pipe
	input := "~m~7~m~payload"
	pr, pw := io.Pipe()

	scanner := NewFrameScanner(bufio.NewReader(pr))

	go func() {
		for i := 0; i < len(input); i++ {
			_, _ = pw.Write([]byte{input[i]})
			time.Sleep(time.Millisecond)
		}
		_ = pw.Close()
	}()

	var tokens []string
	for scanner.Scan() {
		tokens = append(tokens, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan error: %v", err)
	}
	if len(tokens) != 1 || tokens[0] != "payload" {
		t.Fatalf("got %v, want [payload]", tokens)
	}
}

func TestIsPing(t *testing.T) {
	if !isPing([]byte("~h~5")) {
		t.Error("~h~5 harus dianggap ping")
	}
	if isPing([]byte("{\"m\":\"qsd\"}")) {
		t.Error("json qsd tidak boleh dianggap ping")
	}
}

func TestEchoPing(t *testing.T) {
	got := echoPing([]byte("~h~42"))
	want := "~h~43"
	if got != want {
		t.Fatalf("echoPing(~h~42) = %q, want %q", got, want)
	}
}

func TestFrameMessageRoundTrip(t *testing.T) {
	payload := `{"m":"quote_add_symbols","p":["qs_abc","FOREXCOM:XAUUSD"]}`
	framed := frameMessage(payload)

	scanner := NewFrameScanner(bufio.NewReader(strings.NewReader(framed)))
	if !scanner.Scan() {
		t.Fatal("scan failed")
	}
	if got := scanner.Text(); got != payload {
		t.Fatalf("round trip mismatch: %q != %q", got, payload)
	}
}
