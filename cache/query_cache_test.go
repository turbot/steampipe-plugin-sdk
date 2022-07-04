package cache

//
//type calcCostTest struct {
//	data     *QueryCacheResult
//	columns  []string
//	expected int
//}
//
//var pluginSchema = map[string]*proto.TableSchema{
//	"test_table": {
//		Columns: []*proto.ColumnDefinition{
//			{
//				Name: "string",
//				Type: proto.ColumnType_STRING,
//			},
//			{
//				Name: "int",
//				Type: proto.ColumnType_INT,
//			},
//			{
//				Name: "bool",
//				Type: proto.ColumnType_BOOL,
//			},
//			{
//				Name: "double",
//				Type: proto.ColumnType_DOUBLE,
//			},
//			{
//				Name: "ipaddr",
//				Type: proto.ColumnType_IPADDR,
//			},
//			{
//				Name: "json",
//				Type: proto.ColumnType_JSON,
//			},
//			{
//				Name: "cidr",
//				Type: proto.ColumnType_CIDR,
//			},
//			{
//				Name: "timestamp",
//				Type: proto.ColumnType_TIMESTAMP,
//			},
//			{
//				Name: "ltree",
//				Type: proto.ColumnType_LTREE,
//			},
//		},
//	},
//}
//
//var testCasesCalcCost = map[string]*calcCostTest{
//	"all": {
//		data: &QueryCacheResult{
//			Rows: []*proto.Row{
//				{
//					Columns: map[string]*proto.Column{
//						"string": {Value: &proto.Column_StringValue{StringValue: ""}},
//						"int":    {Value: &proto.Column_IntValue{IntValue: 999}},
//						"bool":   {Value: &proto.Column_BoolValue{BoolValue: false}},
//						"double": {Value: &proto.Column_DoubleValue{DoubleValue: 999.9}},
//						"ipaddr": {Value: &proto.Column_IpAddrValue{IpAddrValue: "10.0.0.10"}},
//						"json":   {Value: &proto.Column_JsonValue{JsonValue: []byte("")}},
//						"cidr":   {Value: &proto.Column_CidrRangeValue{CidrRangeValue: "10.0.0.10/22"}},
//						"timestamp": {Value: &proto.Column_TimestampValue{TimestampValue: &timestamppb.Timestamp{
//							Seconds: 1000,
//							Nanos:   1000,
//						}}},
//						"ltree": {Value: &proto.Column_LtreeValue{LtreeValue: ""}},
//					},
//				},
//			},
//		},
//
//		columns:  []string{"string", "int", "bool", "double", "ipaddr", "json", "cidr", "timestamp", "ltree"},
//		expected: 797,
//	},
//}
//
//func TestCalcCost(t *testing.T) {
//
//	cache, err := NewQueryCache("cache_test", "cache_test", pluginSchema, nil)
//	if err != nil {
//		t.Fatal(err)
//	}
//	//
//	//log.Printf("[WARN] empty map size %d", size.Of(map[string]*proto.Column{}))
//	//log.Printf("[WARN] map size 2 empty elements 2 char  name %d", size.Of(map[string]*proto.Column{"10": {}, "20": {}}))
//	//log.Printf("[WARN] map size 4 empty elements 2 char  name %d", size.Of(map[string]*proto.Column{"10": {}, "20": {}, "30": {}, "40": {}}))
//	//log.Printf("[WARN] map size 8 empty elements 2 char  name %d", size.Of(map[string]*proto.Column{"10": {}, "20": {}, "30": {}, "40": {}, "50": {}, "60": {}, "70": {}, "80": {}}))
//	//log.Printf("[WARN] map size 1 empty element empty name %d", size.Of(map[string]*proto.Column{"": {}}))
//	//
//	//log.Printf("[WARN] map size 1 int element empty name %d", size.Of(map[string]*proto.Column{"": {Value: &proto.Column_IntValue{IntValue: 100}}}))
//	//log.Printf("[WARN] map size 1 string element (8 byte) empty name %d", size.Of(map[string]*proto.Column{"": {Value: &proto.Column_StringValue{StringValue: "12345678"}}}))
//	//log.Printf("[WARN] map size 1 string element (0 byte) empty name %d", size.Of(map[string]*proto.Column{"": {Value: &proto.Column_StringValue{StringValue: ""}}}))
//	//log.Printf("[WARN] map size 1 bool element empty name %d", size.Of(map[string]*proto.Column{"": {Value: &proto.Column_BoolValue{BoolValue: false}}}))
//	//log.Printf("[WARN] map size 1 double element empty name %d", size.Of(map[string]*proto.Column{"": {Value: &proto.Column_DoubleValue{DoubleValue: 100.99}}}))
//	//log.Printf("[WARN] map size 1 ipaddr element empty name %d", size.Of(map[string]*proto.Column{"": {Value: &proto.Column_IpAddrValue{IpAddrValue: "255.255.255.255"}}}))
//	//log.Printf("[WARN] map size 1 timestamp element empty name %d", size.Of(map[string]*proto.Column{"": {Value: &proto.Column_TimestampValue{TimestampValue: &timestamppb.Timestamp{Seconds: 1000, Nanos: 1000}}}}))
//	//log.Printf("[WARN] map size 1 cidr element empty name %d", size.Of(map[string]*proto.Column{"": {Value: &proto.Column_IpAddrValue{IpAddrValue: "255.255.255.255/32"}}}))
//	//log.Printf("[WARN] map size 1 json element (8 bytes) empty name %d", size.Of(map[string]*proto.Column{"": {Value: &proto.Column_JsonValue{JsonValue: []byte(`{"a": 1}`)}}}))
//	//log.Printf("[WARN] map size 1 json element (0 bytes) empty name %d", size.Of(map[string]*proto.Column{"": {Value: &proto.Column_JsonValue{JsonValue: []byte(``)}}}))
//	//log.Printf("[WARN] map size 1 ltree element (8 bytes) empty name %d", size.Of(map[string]*proto.Column{"": {Value: &proto.Column_LtreeValue{LtreeValue: "a.b.c.ww"}}}))
//	//log.Printf("[WARN] map size 1 ltree element (0 bytes) empty name %d", size.Of(map[string]*proto.Column{"": {Value: &proto.Column_LtreeValue{LtreeValue: ""}}}))
//	//
//	//log.Printf("[WARN] map size 1 element 8 char name name %d", size.Of(map[string]*proto.Column{"12345678": {Value: &proto.Column_IntValue{IntValue: 100}}}))
//	//log.Printf("[WARN] map size 2 elements 1 char  name %d", size.Of(map[string]*proto.Column{"1": {Value: &proto.Column_IntValue{IntValue: 100}}, "2": {Value: &proto.Column_BoolValue{BoolValue: true}}}))
//	//
//	//log.Printf("[WARN] empty column %d", size.Of(&proto.Column{}))
//	//log.Printf("[WARN] int column %d", size.Of(&proto.Column{Value: &proto.Column_IntValue{IntValue: 100}}))
//	//log.Printf("[WARN] string column 0 chars %d", size.Of(&proto.Column{Value: &proto.Column_StringValue{StringValue: ""}}))
//	//log.Printf("[WARN] string column 8 chars %d", size.Of(&proto.Column{Value: &proto.Column_StringValue{StringValue: "12345678"}}))
//	//log.Printf("[WARN] ipaddr column %d", size.Of(&proto.Column{Value: &proto.Column_IpAddrValue{IpAddrValue: "255.255.255.255"}}))
//	//log.Printf("[WARN] cidr column %d", size.Of(&proto.Column{Value: &proto.Column_CidrRangeValue{"255.255.255.255/32"}}))
//	//log.Printf("[WARN] timestamp column %d", size.Of(&proto.Column{Value: &proto.Column_TimestampValue{TimestampValue: &timestamppb.Timestamp{
//	//	Seconds: 1000,
//	//	Nanos:   1000,
//	//}}}))
//	//log.Printf("[WARN] double column %d", size.Of(&proto.Column{Value: &proto.Column_DoubleValue{DoubleValue: 100.99}}))
//	//log.Printf("[WARN] JSON column 8 chars%d", size.Of(&proto.Column{Value: &proto.Column_JsonValue{JsonValue: []byte(`{"a": 1}`)}}))
//	//log.Printf("[WARN] bool column %d", size.Of(&proto.Column{Value: &proto.Column_BoolValue{BoolValue: false}}))
//	//log.Printf("[WARN] ltree column 8 chars%d", size.Of(&proto.Column{Value: &proto.Column_LtreeValue{LtreeValue: "a.b.c.ww"}}))
//
//	//s := size.Of(map[string]*proto.Column{
//	//	"string": {Value: &proto.Column_StringValue{StringValue: "FOO"}},
//	//})
//	//s = size.Of(&proto.Column{Value: &proto.Column_StringValue{StringValue: "FOO"}})
//	//s = size.Of(&proto.Column_StringValue{StringValue: "FOO"})
//	//
//	//s = size.Of(testCasesCalcCost["all"].data.Rows[0])
//	//s = size.Of(testCasesCalcCost["all"].data)
//	//s = size.Of(proto.Column_IpAddrValue{IpAddrValue: "255.255.255.255"})
//	//s = size.Of(proto.Column_StringValue{StringValue: "FOOWAasssi88888888888888888888888888FOOWAasssi88888888888888888888888888FOOWAasssi88888888888888888888888888FOOWAasssi88888888888888888888888888FOOWAasssi88888888888888888888888888FOOWAasssi88888888888888888888888888"})
//	//s = size.Of(proto.Column_IntValue{IntValue: 999})
//	//s = size.Of(proto.Column_BoolValue{BoolValue: false})
//	//s = size.Of(proto.Column_DoubleValue{DoubleValue: 999.9})
//	//s = size.Of(proto.Column_JsonValue{JsonValue: []byte("FOO")})
//	//s = size.Of(proto.Column_CidrRangeValue{CidrRangeValue: "255.255.255.255/32"})
//	//s = size.Of(&proto.Column_TimestampValue{TimestampValue: &timestamppb.Timestamp{
//	//	Seconds: 1000,
//	//	Nanos:   1000,
//	//}})
//	//s = size.Of(&proto.Column_StringValue{StringValue: "a.b.c.d"})
//
//	//log.Print(s)
//	//nameLength := 0
//	for name, test := range testCasesCalcCost {
//		resMap := test.data.Rows[0].Columns
//		log.Printf("[WARN] actual size %d", size.Of(resMap))
//		log.Printf("[WARN] actual size %d", size.Of(resMap))
//		log.Printf("[WARN] struct actual size %d", size.Of(test.data))
//		cost := cache.calcCost("test_table", test.columns, test.data)
//		cum := map[string]*proto.Column{}
//		for name, column := range test.data.Rows[0].Columns {
//			log.Printf("[WARN] actual size %s = %d + %d=%d", name, size.Of(map[string]*proto.Column{name: column})-len(name)-8, len(name), size.Of(map[string]*proto.Column{name: column}))
//
//			cum[name] = column
//			log.Printf("[WARN] cum size  %d", size.Of(cum))
//		}
//		log.Printf("[WARN] cost  %d", cost)
//
//		if cost != test.expected {
//			fmt.Printf("")
//			t.Errorf(`Test: '%s' FAILED : expected %d, got %d`, name, test.expected, cost)
//		}
//
//	}
//}
//
//var loadTestPluginSchema = map[string]*proto.TableSchema{
//	"test_table": {
//		Columns: []*proto.ColumnDefinition{
//			{
//				Name: "string",
//				Type: proto.ColumnType_STRING,
//			}, {
//				Name: "string2",
//				Type: proto.ColumnType_STRING,
//			}, {
//				Name: "string3",
//				Type: proto.ColumnType_STRING,
//			}, {
//				Name: "string4",
//				Type: proto.ColumnType_STRING,
//			}, {
//				Name: "string5",
//				Type: proto.ColumnType_STRING,
//			}, {
//				Name: "string6",
//				Type: proto.ColumnType_STRING,
//			},
//			{
//				Name: "int",
//				Type: proto.ColumnType_INT,
//			},
//			{
//				Name: "bool",
//				Type: proto.ColumnType_BOOL,
//			},
//			{
//				Name: "double",
//				Type: proto.ColumnType_DOUBLE,
//			},
//			{
//				Name: "ipaddr",
//				Type: proto.ColumnType_IPADDR,
//			},
//			{
//				Name: "json",
//				Type: proto.ColumnType_JSON,
//			},
//			{
//				Name: "cidr",
//				Type: proto.ColumnType_CIDR,
//			},
//			{
//				Name: "timestamp",
//				Type: proto.ColumnType_TIMESTAMP,
//			},
//			{
//				Name: "ltree",
//				Type: proto.ColumnType_LTREE,
//			},
//		},
//	},
//}
//
//func TestCalcCostLoad(t *testing.T) {
//	cache, err := NewQueryCache("cache_test", "cache_test", loadTestPluginSchema, nil)
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	results, columns := buildLoadTestResults()
//
//	start := time.Now()
//	cache.calcCost("test_table", columns, results)
//	time := time.Since(start)
//	log.Printf("[WARN] time: %s", time)
//}
//
//func buildLoadTestResults() (*QueryCacheResult, []string) {
//	loadTestNumRows := 1000
//	res := &QueryCacheResult{
//		Rows: make([]*proto.Row, loadTestNumRows),
//	}
//	for i := 0; i < loadTestNumRows; i++ {
//		res.Rows[i] = &proto.Row{
//			Columns: map[string]*proto.Column{
//				"string":  {Value: &proto.Column_StringValue{StringValue: "some string data"}},
//				"string2": {Value: &proto.Column_StringValue{StringValue: "some string data"}},
//				"string3": {Value: &proto.Column_StringValue{StringValue: "some string data"}},
//				"string4": {Value: &proto.Column_StringValue{StringValue: "some string data"}},
//				"string5": {Value: &proto.Column_StringValue{StringValue: "some string data"}},
//				"string6": {Value: &proto.Column_StringValue{StringValue: "some string data"}},
//				"int":     {Value: &proto.Column_IntValue{IntValue: 999}},
//				"bool":    {Value: &proto.Column_BoolValue{BoolValue: false}},
//				"double":  {Value: &proto.Column_DoubleValue{DoubleValue: 999.9}},
//				"ipaddr":  {Value: &proto.Column_IpAddrValue{IpAddrValue: "10.0.0.10"}},
//				"json":    {Value: &proto.Column_JsonValue{JsonValue: []byte(`{"a" : 1`)}},
//				"cidr":    {Value: &proto.Column_CidrRangeValue{CidrRangeValue: "10.0.0.10/22"}},
//				"timestamp": {Value: &proto.Column_TimestampValue{TimestampValue: &timestamppb.Timestamp{
//					Seconds: 1000,
//					Nanos:   1000,
//				}}},
//				"ltree": {Value: &proto.Column_LtreeValue{LtreeValue: "a.b.c.d.s"}},
//			},
//		}
//	}
//
//	return res, []string{"string", "string2", "string3", "string4", "string5", "string6", "int", "bool", "double", "ipaddr", "json", "cidr", "timestamp", "ltree"}
//}
