package transform

import (
	"reflect"
	"testing"
)

type TransformTest struct {
	d        *TransformData
	function TransformFunc
	expected interface{}
}

type taggedStruct struct {
	ID       int     `json:"id,omitempty"`
	NodeID   *string `json:"node_id,omitempty"`
	HTMLURL  string  `json:"html_url,omitempty"`
	CloneURL string  `json:"clone_url,omitempty"`
	GitURL   string  `json:"git_url,omitempty"`
	NoTag    string
}

var nodeId = "id1"
var taggedStructInstance = &taggedStruct{
	ID:       100,
	NodeID:   &nodeId,
	HTMLURL:  "www.turbot.com",
	CloneURL: "www.turbot2.com",
	GitURL:   "www.github.com",
	NoTag:    "no tag for some reason",
}

type testStruct struct {
	a string
	b string
}

var upperString = "FOO"
var lowerString = "foo"

var testCasesTransform = map[string]TransformTest{
	// do not need every permutation of input as the go-kit unit tests cover this
	"ToBool string TRUE": {
		d: &TransformData{
			Value: "TRUE",
		},
		function: ToBool,
		expected: true,
	},
	"ToBool nil": {
		d: &TransformData{
			Value: nil,
		},
		function: ToBool,
		expected: nil,
	},
	"ToBool bool false": {
		d: &TransformData{
			Value: false,
		},
		function: ToBool,
		expected: false,
	},
	"ToBool bad string": {
		d: &TransformData{
			Value: "FOO",
		},
		function: ToBool,
		expected: "ERROR",
	},
	"FieldValueGo SomeID": {
		d: &TransformData{
			HydrateItem: map[string]string{
				"SomeID":   "id",
				"TTLValue": "100",
			},
			ColumnName: "some_id",
		},
		function: FieldValueGo,
		expected: "id",
	},
	"FieldValueGo TTLValue": {
		d: &TransformData{
			HydrateItem: map[string]string{
				"SomeID":   "id",
				"TTLValue": "100",
			},
			ColumnName: "ttl_value",
		},
		function: FieldValueGo,
		expected: "100",
	},
	"RawValue string": {
		d: &TransformData{
			HydrateItem: "FOO",
		},
		function: RawValue,
		expected: "FOO",
	},
	"RawValue int": {
		d: &TransformData{
			HydrateItem: 1,
		},
		function: RawValue,
		expected: 1,
	},
	"RawValue []string": {
		d: &TransformData{
			HydrateItem: []string{"a", "b"},
		},
		function: RawValue,
		expected: []string{"a", "b"},
	},
	"RawValue map": {
		d: &TransformData{
			HydrateItem: map[string]string{"a": "A", "b": "B"},
		},
		function: RawValue,
		expected: map[string]string{"a": "A", "b": "B"},
	},
	"RawValue struct": {
		d: &TransformData{
			HydrateItem: &testStruct{"A", "B"},
		},
		function: RawValue,
		expected: &testStruct{"A", "B"},
	},
	"UnmarshalYAML nil": {
		d: &TransformData{
			Value: nil,
		},
		function: UnmarshalYAML,
		expected: nil,
	},
	"ToUpper": {
		d: &TransformData{
			Value: lowerString,
		},
		function: ToUpper,
		expected: upperString,
	},
	"ToLower": {
		d: &TransformData{
			Value: upperString,
		},
		function: ToLower,
		expected: lowerString,
	},
	"ToUpper pointer": {
		d: &TransformData{
			Value: &lowerString,
		},
		function: ToUpper,
		expected: upperString,
	},
	"ToLower pointer": {
		d: &TransformData{
			Value: &upperString,
		},
		function: ToLower,
		expected: lowerString,
	},
	"ToUpper nil": {
		d: &TransformData{
			Value: nil,
		},
		function: ToUpper,
		expected: nil,
	},
	"ToLower nil": {
		d: &TransformData{
			Value: nil,
		},
		function: ToLower,
		expected: nil,
	},
	"ToString nil": {
		d: &TransformData{
			Value: nil,
		},
		function: ToString,
		expected: nil,
	},
	"ToInt nil": {
		d: &TransformData{
			Value: nil,
		},
		function: ToInt,
		expected: nil,
	},
	"ToDouble nil": {
		d: &TransformData{
			Value: nil,
		},
		function: ToDouble,
		expected: nil,
	},
	"UnmarshalYAML JSON array": {
		d: &TransformData{
			Value: `["foo", "bar"]`,
		},
		function: UnmarshalYAML,
		expected: []interface{}{"foo", "bar"},
	},
	"UnmarshalYAML JSON map": {
		d: &TransformData{
			Value: `
{
   "Version":"2012-10-17",
   "Statement":["foo"]
}`,
		},
		function: UnmarshalYAML,
		expected: map[string]interface{}{
			"Version": "2012-10-17",
			"Statement": []interface{}{
				"foo",
			},
		},
	},
	"UnmarshalYAML JSON iam policy normalization encoded": {
		d: &TransformData{
			Value: "%7B%0A%20%20%22Version%22%3A%20%222012-10-17%22%2C%0A%20%20%22Statement%22%3A%20%5B%7B%22Sid%22%3A%22AWSAdmin%22%2C%22Effect%22%3A%22Allow%22%2C%22Action%22%3A%5B%22aws-marketplace-management%3Av%2A%22%2C%22aws-portal%3Av%2A%22%2C%22ce%3A%2A%22%2C%22iam%3Ag%2A%22%2C%22iam%3Al%2A%22%2C%22iam%3Asi%2A%22%2C%22iam%3At%2A%22%2C%22iam%3Aun%2A%22%2C%22pricing%3A%2A%22%2C%22s3%3Aa%2A%22%2C%22s3%3Ac%2A%22%2C%22s3%3Adeletea%2A%22%2C%22s3%3Adeleteb%2A%22%2C%22s3%3Adeleteo%2A%22%2C%22s3%3Ag%2A%22%2C%22s3%3Ah%2A%22%2C%22s3%3Al%2A%22%2C%22s3%3Aputa%2A%22%2C%22s3%3Aputbucketn%2A%22%2C%22s3%3Aputbucketp%2A%22%2C%22s3%3Aputbuckett%2A%22%2C%22s3%3Aputbucketv%2A%22%2C%22s3%3Aputbucketw%2A%22%2C%22s3%3Apute%2A%22%2C%22s3%3Aputi%2A%22%2C%22s3%3Aputl%2A%22%2C%22s3%3Aputm%2A%22%2C%22s3%3Aputobject%22%2C%22s3%3Aputobjectt%2A%22%2C%22s3%3Aputobjectversiont%2A%22%2C%22s3%3Ar%2A%22%2C%22sts%3A%2A%22%2C%22support%3A%2A%22%5D%2C%22Resource%22%3A%22%2A%22%7D%5D%0A%7D%0A",
		},
		function: UnmarshalYAML,
		expected: map[string]interface{}{
			"Version": "2012-10-17",
			"Statement": []interface{}{
				map[string]interface{}{
					"Sid":    "AWSAdmin",
					"Effect": "Allow",
					"Action": []interface{}{
						"aws-marketplace-management:v*",
						"aws-portal:v*",
						"ce:*",
						"iam:g*",
						"iam:l*",
						"iam:si*",
						"iam:t*",
						"iam:un*",
						"pricing:*",
						"s3:a*",
						"s3:c*",
						"s3:deletea*",
						"s3:deleteb*",
						"s3:deleteo*",
						"s3:g*",
						"s3:h*",
						"s3:l*",
						"s3:puta*",
						"s3:putbucketn*",
						"s3:putbucketp*",
						"s3:putbuckett*",
						"s3:putbucketv*",
						"s3:putbucketw*",
						"s3:pute*",
						"s3:puti*",
						"s3:putl*",
						"s3:putm*",
						"s3:putobject",
						"s3:putobjectt*",
						"s3:putobjectversiont*",
						"s3:r*",
						"sts:*",
						"support:*",
					},
					"Resource": "*",
				},
			},
		},
	},
	"UnmarshalYAML JSON iam policy normalization decoded": {
		d: &TransformData{
			Value: `{
   "Version":"2012-10-17",
   "Statement":[
      {
         "Sid":"AWSAdmin",
         "Effect":"Allow",
         "Action":[
            "aws-marketplace-management:v*",
            "aws-portal:v*",
            "ce:*",
            "iam:g*",
            "iam:l*",
            "iam:si*",
            "iam:t*",
            "iam:un*",
            "pricing:*",
            "s3:a*",
            "s3:c*",
            "s3:deletea*",
            "s3:deleteb*",
            "s3:deleteo*",
            "s3:g*",
            "s3:h*",
            "s3:l*",
            "s3:puta*",
            "s3:putbucketn*",
            "s3:putbucketp*",
            "s3:putbuckett*",
            "s3:putbucketv*",
            "s3:putbucketw*",
            "s3:pute*",
            "s3:puti*",
            "s3:putl*",
            "s3:putm*",
            "s3:putobject",
            "s3:putobjectt*",
            "s3:putobjectversiont*",
            "s3:r*",
            "sts:*",
            "support:*"
         ],
         "Resource":"*"
      }
   ]
}`,
		},
		function: UnmarshalYAML,
		expected: map[string]interface{}{
			"Version": "2012-10-17",
			"Statement": []interface{}{
				map[string]interface{}{
					"Sid":    "AWSAdmin",
					"Effect": "Allow",
					"Action": []interface{}{
						"aws-marketplace-management:v*",
						"aws-portal:v*",
						"ce:*",
						"iam:g*",
						"iam:l*",
						"iam:si*",
						"iam:t*",
						"iam:un*",
						"pricing:*",
						"s3:a*",
						"s3:c*",
						"s3:deletea*",
						"s3:deleteb*",
						"s3:deleteo*",
						"s3:g*",
						"s3:h*",
						"s3:l*",
						"s3:puta*",
						"s3:putbucketn*",
						"s3:putbucketp*",
						"s3:putbuckett*",
						"s3:putbucketv*",
						"s3:putbucketw*",
						"s3:pute*",
						"s3:puti*",
						"s3:putl*",
						"s3:putm*",
						"s3:putobject",
						"s3:putobjectt*",
						"s3:putobjectversiont*",
						"s3:r*",
						"sts:*",
						"support:*",
					},
					"Resource": "*",
				},
			},
		},
	},
	"UnmarshalYAML iam policy normalization decoded": {
		d: &TransformData{
			Value: `---
Version: '2012-10-17'
Statement:
- Sid: AWSAdmin
  Effect: Allow
  Action:
  - aws-marketplace-management:v*
  - aws-portal:v*
  - ce:*
  - iam:g*
  - iam:l*
  - iam:si*
  - iam:t*
  - iam:un*
  - pricing:*
  - s3:a*
  - s3:c*
  - s3:deletea*
  - s3:deleteb*
  - s3:deleteo*
  - s3:g*
  - s3:h*
  - s3:l*
  - s3:puta*
  - s3:putbucketn*
  - s3:putbucketp*
  - s3:putbuckett*
  - s3:putbucketv*
  - s3:putbucketw*
  - s3:pute*
  - s3:puti*
  - s3:putl*
  - s3:putm*
  - s3:putobject
  - s3:putobjectt*
  - s3:putobjectversiont*
  - s3:r*
  - sts:*
  - support:*
  Resource: "*"
`,
		},
		function: UnmarshalYAML,
		expected: map[string]interface{}{
			"Version": "2012-10-17",
			"Statement": []interface{}{
				map[string]interface{}{
					"Sid":    "AWSAdmin",
					"Effect": "Allow",
					"Action": []interface{}{
						"aws-marketplace-management:v*",
						"aws-portal:v*",
						"ce:*",
						"iam:g*",
						"iam:l*",
						"iam:si*",
						"iam:t*",
						"iam:un*",
						"pricing:*",
						"s3:a*",
						"s3:c*",
						"s3:deletea*",
						"s3:deleteb*",
						"s3:deleteo*",
						"s3:g*",
						"s3:h*",
						"s3:l*",
						"s3:puta*",
						"s3:putbucketn*",
						"s3:putbucketp*",
						"s3:putbuckett*",
						"s3:putbucketv*",
						"s3:putbucketw*",
						"s3:pute*",
						"s3:puti*",
						"s3:putl*",
						"s3:putm*",
						"s3:putobject",
						"s3:putobjectt*",
						"s3:putobjectversiont*",
						"s3:r*",
						"sts:*",
						"support:*",
					},
					"Resource": "*",
				},
			},
		},
	},
	"UnmarshalYAML JSON policy array encoded": {
		d: &TransformData{
			Value: "%5B%22aws-marketplace-management%3Av%2A%22%2C%22aws-portal%3Av%2A%22%2C%22ce%3A%2A%22%2C%22iam%3Ag%2A%22%2C%22iam%3Al%2A%22%2C%22iam%3Asi%2A%22%2C%22iam%3At%2A%22%2C%22iam%3Aun%2A%22%2C%22pricing%3A%2A%22%2C%22s3%3Aa%2A%22%2C%22s3%3Ac%2A%22%2C%22s3%3Adeletea%2A%22%2C%22s3%3Adeleteb%2A%22%2C%22s3%3Adeleteo%2A%22%2C%22s3%3Ag%2A%22%2C%22s3%3Ah%2A%22%2C%22s3%3Al%2A%22%2C%22s3%3Aputa%2A%22%2C%22s3%3Aputbucketn%2A%22%2C%22s3%3Aputbucketp%2A%22%2C%22s3%3Aputbuckett%2A%22%2C%22s3%3Aputbucketv%2A%22%2C%22s3%3Aputbucketw%2A%22%2C%22s3%3Apute%2A%22%2C%22s3%3Aputi%2A%22%2C%22s3%3Aputl%2A%22%2C%22s3%3Aputm%2A%22%2C%22s3%3Aputobject%22%2C%22s3%3Aputobjectt%2A%22%2C%22s3%3Aputobjectversiont%2A%22%2C%22s3%3Ar%2A%22%2C%22sts%3A%2A%22%2C%22support%3A%2A%22%5D",
		},
		function: UnmarshalYAML,
		expected: []interface{}{
			"aws-marketplace-management:v*",
			"aws-portal:v*",
			"ce:*",
			"iam:g*",
			"iam:l*",
			"iam:si*",
			"iam:t*",
			"iam:un*",
			"pricing:*",
			"s3:a*",
			"s3:c*",
			"s3:deletea*",
			"s3:deleteb*",
			"s3:deleteo*",
			"s3:g*",
			"s3:h*",
			"s3:l*",
			"s3:puta*",
			"s3:putbucketn*",
			"s3:putbucketp*",
			"s3:putbuckett*",
			"s3:putbucketv*",
			"s3:putbucketw*",
			"s3:pute*",
			"s3:puti*",
			"s3:putl*",
			"s3:putm*",
			"s3:putobject",
			"s3:putobjectt*",
			"s3:putobjectversiont*",
			"s3:r*",
			"sts:*",
			"support:*",
		},
	},
	"UnmarshalYAML JSON policy array decoded": {
		d: &TransformData{
			Value: `["aws-marketplace-management:v*","aws-portal:v*","ce:*","iam:g*","iam:l*","iam:si*","iam:t*","iam:un*","pricing:*","s3:a*","s3:c*","s3:deletea*","s3:deleteb*","s3:deleteo*","s3:g*","s3:h*","s3:l*","s3:puta*","s3:putbucketn*","s3:putbucketp*","s3:putbuckett*","s3:putbucketv*","s3:putbucketw*","s3:pute*","s3:puti*","s3:putl*","s3:putm*","s3:putobject","s3:putobjectt*","s3:putobjectversiont*","s3:r*","sts:*","support:*"]`,
		},
		function: UnmarshalYAML,
		expected: []interface{}{
			"aws-marketplace-management:v*",
			"aws-portal:v*",
			"ce:*",
			"iam:g*",
			"iam:l*",
			"iam:si*",
			"iam:t*",
			"iam:un*",
			"pricing:*",
			"s3:a*",
			"s3:c*",
			"s3:deletea*",
			"s3:deleteb*",
			"s3:deleteo*",
			"s3:g*",
			"s3:h*",
			"s3:l*",
			"s3:puta*",
			"s3:putbucketn*",
			"s3:putbucketp*",
			"s3:putbuckett*",
			"s3:putbucketv*",
			"s3:putbucketw*",
			"s3:pute*",
			"s3:puti*",
			"s3:putl*",
			"s3:putm*",
			"s3:putobject",
			"s3:putobjectt*",
			"s3:putobjectversiont*",
			"s3:r*",
			"sts:*",
			"support:*",
		},
	},

	"FieldValueTag string": {
		d: &TransformData{
			Value:       "TRUE",
			HydrateItem: taggedStructInstance,
			ColumnName:  "git_url",
			Param:       "json",
		},
		function: FieldValueTag,
		expected: taggedStructInstance.GitURL,
	},
	"FieldValueTag int": {
		d: &TransformData{
			Value:       "TRUE",
			HydrateItem: taggedStructInstance,
			ColumnName:  "id",
			Param:       "json",
		},
		function: FieldValueTag,
		expected: taggedStructInstance.ID,
	},
	"FieldValueTag int*": {
		d: &TransformData{
			Value:       "TRUE",
			HydrateItem: taggedStructInstance,
			ColumnName:  "node_id",
			Param:       "json",
		},
		function: FieldValueTag,
		expected: taggedStructInstance.NodeID,
	},
	"FieldValueTag missing": {
		d: &TransformData{
			Value:       "TRUE",
			HydrateItem: taggedStructInstance,
			ColumnName:  "no_tag",
			Param:       "json",
		},
		function: FieldValueTag,
		expected: "ERROR",
	},
	"FieldsValue string first value": {
		d: &TransformData{
			HydrateItem: taggedStructInstance,
			Param:       []string{"GitURL", "GetColumn"},
		},
		function: FieldsValue,
		expected: taggedStructInstance.GitURL,
	},
	"FieldsValue string second value": {
		d: &TransformData{
			HydrateItem: taggedStructInstance,
			Param:       []string{"GetColumn", "NodeID"},
		},
		function: FieldsValue,
		expected: taggedStructInstance.NodeID,
	},
	"FieldsValue Invalid Value": {
		d: &TransformData{
			HydrateItem: taggedStructInstance,
			Param:       []string{"GetColumn", "ListColumn"},
		},
		function: FieldsValue,
		expected: nil,
	},
	"UnixToTimestamp time conversion int64": {
		d: &TransformData{
			Value: 1611061921,
		},
		function: UnixToTimestamp,
		expected: "2021-01-19T18:42:01+05:30",
	},
	"UnixToTimestamp time conversion string": {
		d: &TransformData{
			Value: "1610821712",
		},
		function: UnixToTimestamp,
		expected: "2021-01-16T23:58:32+05:30",
	},
	"UnixToTimestamp time conversion float": {
		d: &TransformData{
			Value: 915148799.75,
		},
		function: UnixToTimestamp,
		expected: "1999-01-01T05:29:59+05:30",
	},
	"UnixToTimestamp time conversion float string": {
		d: &TransformData{
			Value: "999999999.75",
		},
		function: UnixToTimestamp,
		expected: "2001-09-09T07:16:39+05:30",
	},
	"UnixToTimestamp time conversion zero": {
		d: &TransformData{
			Value: 0,
		},
		function: UnixToTimestamp,
		expected: nil,
	},
	"UnixToTimestamp nil": {
		d: &TransformData{
			Value: nil,
		},
		function: UnixToTimestamp,
		expected: nil,
	},
	"UnixToTimestamp random string": {
		d: &TransformData{
			Value: "stringtest",
		},
		function: UnixToTimestamp,
		expected: "ERROR",
	},
	"UnixToTimestamp struct": {
		d: &TransformData{
			Value: &testStruct{"A", "B"},
		},
		function: UnixToTimestamp,
		expected: "ERROR",
	},
	"UnixMsToTimestamp time conversion string": {
		d: &TransformData{
			Value: "1611057198070",
		},
		function: UnixMsToTimestamp,
		expected: "2021-01-19T17:23:18+05:30",
	},
	"UnixMsToTimestamp time conversion int64": {
		d: &TransformData{
			Value: 1611057198070,
		},
		function: UnixMsToTimestamp,
		expected: "2021-01-19T17:23:18+05:30",
	},
	"UnixMsToTimestamp string": {
		d: &TransformData{
			Value: "stringtest",
		},
		function: UnixMsToTimestamp,
		expected: "ERROR",
	},
	"UnixMsToTimestamp struct": {
		d: &TransformData{
			Value: &testStruct{"A", "B"},
		},
		function: UnixMsToTimestamp,
		expected: "ERROR",
	},
	"UnixMsToTimestamp zero": {
		d: &TransformData{
			Value: 0,
		},
		function: UnixMsToTimestamp,
		expected: nil,
	},
	"UnixMsToTimestamp nil": {
		d: &TransformData{
			Value: nil,
		},
		function: UnixMsToTimestamp,
		expected: nil,
	},
	"EnsureStringArray nil": {
		d: &TransformData{
			Value: nil,
		},
		function: EnsureStringArray,
		expected: nil,
	},
	"EnsureStringArray string": {
		d: &TransformData{
			Value: "arn:aws:acm:us-east-2:123456789012:certificate/ec12345a-6121-47c3-9cd2-29fc7298889d",
		},
		function: EnsureStringArray,
		expected: []string{"arn:aws:acm:us-east-2:123456789012:certificate/ec12345a-6121-47c3-9cd2-29fc7298889d"},
	},
	"EnsureStringArray array": {
		d: &TransformData{
			Value: []string{"arn:aws:acm:us-east-2:123456789012:certificate/ec12345a-6121-47c3-9cd2-29fc7298889d"},
		},
		function: EnsureStringArray,
		expected: []string{"arn:aws:acm:us-east-2:123456789012:certificate/ec12345a-6121-47c3-9cd2-29fc7298889d"},
	},
	"StringArrayToMap array": {
		d: &TransformData{
			Value: []string{"foo", "bar"},
		},
		function: StringArrayToMap,
		expected: map[string]bool{"foo": true, "bar": true},
	},
	"StringArrayToMap nil": {
		d: &TransformData{
			Value: []string{},
		},
		function: StringArrayToMap,
		expected: map[string]bool{},
	},
	"StringArrayToMap struct": {
		d: &TransformData{
			Value: &testStruct{"A", "B"},
		},
		function: StringArrayToMap,
		expected: "ERROR",
	},
}

func TestTransformFunctions(t *testing.T) {
	for name, test := range testCasesTransform {
		executeTransformTest(name, t, test)
	}
}

func executeTransformTest(name string, t *testing.T, test TransformTest) {
	defer func() {
		if r := recover(); r != nil {
			if test.expected != "PANIC" {
				t.Errorf(`Test: '%s'' FAILED : unexpected panic %v`, name, r)
			}
		}
	}()

	output, err := test.function(textCtx, test.d)
	if err != nil {
		if test.expected != "ERROR" {
			t.Errorf("Test: '%s'' FAILED : \nunexpected error %v", name, err)
		}
		return
	}
	if test.expected == "ERROR" {
		t.Errorf("Test: '%s'' FAILED - expected error", name)
	}
	if !reflect.DeepEqual(test.expected, output) {
		t.Errorf("Test: '%s'' FAILED : \nexpected:\n %v, \ngot:\n %v\n", name, test.expected, output)
	}
}
