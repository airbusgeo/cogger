package cogger

import (
	"bytes"
	"crypto/md5"
	"io"
	"os"
	"testing"
)

func testCase(t *testing.T, filename string) {
	t.Helper()
	f, err := os.Open("testdata/cog_" + filename)
	if err != nil {
		t.Fatal(err)
	}
	hasher := md5.New()
	_, _ = io.Copy(hasher, f)
	srchash := hasher.Sum(nil)
	f.Close()
	f, err = os.Open("testdata/" + filename)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	_ = f.Seek(0, io.SeekStart)

	buf := bytes.Buffer{}

	hasher.Reset()
	_ = Rewrite(&buf, f)
	_, _ = io.Copy(hasher, &buf)

	coghash := hasher.Sum(nil)

	if !bytes.Equal(coghash, srchash) != 0 {
		t.Errorf("mismatch on %s: %x / %x", filename, srchash, coghash)
	}
}

func TestCases(t *testing.T) {
	cases := []string{
		"band4mask.tif",
		"band4.tif",
		"graymask.tif",
		"gray.tif",
		"rgbmask.tif",
		"rgb.tif",
	}
	for i := range cases {
		testCase(t, cases[i])
	}
}
