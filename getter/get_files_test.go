package getter

import (
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type getFilesTest struct {
	FullSourcePath   string
	RemoteSourcePath string
	Expected         string
}

var getFilesTestCases = map[string]getFilesTest{
	"no special characters": {
		FullSourcePath:   "s3.amazonaws.com/bucket///*.tf?aws_access_key_id=ABCDEFGHIJ4KLMNO3PQ&aws_access_key_secret=ThisIsateSt123secr3tT0Val1dat3",
		RemoteSourcePath: "s3.amazonaws.com/bucket/",
		Expected:         "s3.amazonaws.com/bucket/?aws_access_key_id=ABCDEFGHIJ4KLMNO3PQ&aws_access_key_secret=ThisIsateSt123secr3tT0Val1dat3",
	},
	"access key - special characters - +": {
		FullSourcePath:   "s3.amazonaws.com/bucket///*.tf?aws_access_key_id=ABCDEFGH+J4KLMNO3PQ&aws_access_key_secret=ThisIsateSt123secr3tT0Val1dat3",
		RemoteSourcePath: "s3.amazonaws.com/bucket/",
		Expected:         "s3.amazonaws.com/bucket/?aws_access_key_id=ABCDEFGH%2BJ4KLMNO3PQ&aws_access_key_secret=ThisIsateSt123secr3tT0Val1dat3",
	},
	"secret key - special characters - +": {
		FullSourcePath:   "s3.amazonaws.com/bucket///*.tf?aws_access_key_id=ABCDEFGHIJ4KLMNO3PQ&aws_access_key_secret=ThisIsateSt12+secr3tT0Val1dat3",
		RemoteSourcePath: "s3.amazonaws.com/bucket/",
		Expected:         "s3.amazonaws.com/bucket/?aws_access_key_id=ABCDEFGHIJ4KLMNO3PQ&aws_access_key_secret=ThisIsateSt12%2Bsecr3tT0Val1dat3",
	},
	"access key - special characters - /": {
		FullSourcePath:   "s3.amazonaws.com/bucket///*.tf?aws_access_key_id=ABCDEFGH/J4KLMNO3PQ&aws_access_key_secret=ThisIsateSt123secr3tT0Val1dat3",
		RemoteSourcePath: "s3.amazonaws.com/bucket/",
		Expected:         "s3.amazonaws.com/bucket/?aws_access_key_id=ABCDEFGH%252FJ4KLMNO3PQ&aws_access_key_secret=ThisIsateSt123secr3tT0Val1dat3",
	},
	"secret key - special characters - //": {
		FullSourcePath:   "s3.amazonaws.com/bucket///*.tf?aws_access_key_id=ABCDEFGHIJ4KLMNO3PQ&aws_access_key_secret=ThisIsateSt1//secr3tT0Val1dat3",
		RemoteSourcePath: "s3.amazonaws.com/bucket/",
		Expected:         "s3.amazonaws.com/bucket/?aws_access_key_id=ABCDEFGHIJ4KLMNO3PQ&aws_access_key_secret=ThisIsateSt1%252F%252Fsecr3tT0Val1dat3",
	},
	"session token - special characters": {
		FullSourcePath:   "s3.amazonaws.com/bucket///*.tf?aws_access_key_id=ABCDEFGHIJ4KLMNO3PQ&aws_access_key_secret=ThisIsateSt1//secr3tT0Val1dat3&aws_access_token=lsghglhshgjsgjejgrekg74592645982jlsdhjlgfhs////fsjfhjwlohfowyhhfhwq6796264629hwkhfyy69+ljgdj",
		RemoteSourcePath: "s3.amazonaws.com/bucket/",
		Expected:         "s3.amazonaws.com/bucket/?aws_access_key_id=ABCDEFGHIJ4KLMNO3PQ&aws_access_key_secret=ThisIsateSt1%252F%252Fsecr3tT0Val1dat3&aws_access_token=lsghglhshgjsgjejgrekg74592645982jlsdhjlgfhs%252F%252F%252F%252Ffsjfhjwlohfowyhhfhwq6796264629hwkhfyy69%2Bljgdj",
	},
}

func TestGetFiles(t *testing.T) {
	for name, test := range getFilesTestCases {

		// parse source path to extract the raw query string
		u, err := url.Parse(test.FullSourcePath)
		if err != nil {
			t.Errorf(`failed to parse the sourcePath: %s`, test.FullSourcePath)
		}

		remoteSourcePath := addQueryToSourcePath(u, test.RemoteSourcePath)

		if remoteSourcePath != test.Expected {
			t.Errorf(`Test: '%s'' FAILED : expected %v, got %v`, name, test.Expected, remoteSourcePath)
		}
	}
}

// TestGetFilesRemote runs GetFiles through go-getter itself, against a local HTTP server
// the test owns, so a go-getter upgrade that breaks the download path fails here.
func TestGetFilesRemote(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/data/hello.txt", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("hello from go-getter"))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	t.Run("downloads a remote file", func(t *testing.T) {
		tmpDir := t.TempDir()
		dest, glob, err := GetFiles(server.URL+"/data/hello.txt", tmpDir)
		if err != nil {
			t.Fatalf("GetFiles failed: %v", err)
		}
		if glob == "" || dest == "" {
			t.Fatalf("expected a destination and glob, got %q and %q", dest, glob)
		}
		// go-getter downloads an http file into a folder of that name
		found := false
		err = filepath.WalkDir(dest, func(p string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return err
			}
			content, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			if string(content) != "hello from go-getter" {
				t.Errorf("unexpected content %q in %s", content, p)
			}
			found = true
			return nil
		})
		if err != nil {
			t.Fatalf("walking %s failed: %v", dest, err)
		}
		if !found {
			t.Errorf("no downloaded file found under %s", dest)
		}
	})

	t.Run("returns an error for a missing remote file", func(t *testing.T) {
		_, _, err := GetFiles(server.URL+"/data/missing.txt", t.TempDir())
		if err == nil {
			t.Fatal("expected an error for a 404, got none")
		}
		if !strings.Contains(err.Error(), "failed to get directory specified by the source") {
			t.Errorf("unexpected error: %v", err)
		}
	})
}
