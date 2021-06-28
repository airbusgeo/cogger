package cogger

import (
	"bytes"
	"crypto/md5"
	"io"
	"os"
	"testing"

	"github.com/google/tiff"
)

func testCase(t *testing.T, expected_filename string, filenames ...string) {
	t.Helper()
	f, err := os.Open("testdata/" + expected_filename)
	if err != nil {
		t.Fatal(err)
	}
	hasher := md5.New()
	_, _ = io.Copy(hasher, f)
	srchash := hasher.Sum(nil)
	f.Close()

	files := make([]tiff.ReadAtReadSeeker, len(filenames))
	for i := range filenames {
		f, err = os.Open("testdata/" + filenames[i])
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		files[i] = f
	}

	buf := bytes.Buffer{}

	hasher.Reset()
	_ = Rewrite(&buf, files...)
	_, _ = io.Copy(hasher, &buf)

	coghash := hasher.Sum(nil)

	if !bytes.Equal(coghash, srchash) {
		t.Errorf("mismatch on %v: %x / %x", filenames, srchash, coghash)
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
		testCase(t, "cog_"+cases[i], cases[i])
	}
}

func TestMultiFiles(t *testing.T) {
	testCase(t, "cog_ext_ovr.tif", "exttest.tif", "exttest.tif.ovr")
	testCase(t, "cog_ext_multi.tif", "exttest.tif", "exttest.tif.2", "exttest.tif.4")
}
