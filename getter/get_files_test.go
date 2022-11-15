package getter

import (
	"net/url"
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
