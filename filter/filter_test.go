package filter

import (
	"strings"
	"testing"
)

var validCases = map[string]string{

	// Formatting
	`foo = 'foo'`:           `( "foo" = 'foo' )`,
	`foo  = 'foo'`:          `( "foo" = 'foo' )`,
	`foo='foo'`:             `( "foo" = 'foo' )`,
	`foo  =    'foo'`:       `( "foo" = 'foo' )`,
	` foo = 'foo'`:          `( "foo" = 'foo' )`,
	`foo = 'foo' `:          `( "foo" = 'foo' )`,
	` foo = 'foo' `:         `( "foo" = 'foo' )`,
	"\n\nfoo\n=\n'foo'\n\n": `( "foo" = 'foo' )`,
	`(foo = 'foo')`:         `( "foo" = 'foo' )`,
	`((foo = 'foo'))`:       `( "foo" = 'foo' )`,
	"foo = bar":             `( "foo" = "bar" )`,

	// String constants
	`foo = ''`:                        `( "foo" = '' )`,
	`foo = ' '`:                       `( "foo" = ' ' )`,
	`foo = 'with ''escaped'' quotes'`: `( "foo" = 'with ''escaped'' quotes' )`,
	`foo = '''fully escaped'''`:       `( "foo" = '''fully escaped''' )`,
	`'foo' = foo`:                     `( 'foo' = "foo" )`,

	// Numbers
	"foo = 123":   `( "foo" = 123 )`,
	"foo = -123":  `( "foo" = -123 )`,
	"foo = 0":     `( "foo" = 0 )`,
	"foo = 0.0":   `( "foo" = 0.0 )`,
	"foo = 1.23":  `( "foo" = 1.23 )`,
	"foo = -1.23": `( "foo" = -1.23 )`,
	"123 = foo":   `( 123 = "foo" )`,

	// Time calculations
	"foo = now()":                      `( "foo" = now() )`,
	"foo = now()+interval '1 hr'":      `( "foo" = now() + interval '1 hr' )`,
	"foo = now() + interval '1 hr'":    `( "foo" = now() + interval '1 hr' )`,
	"foo = now() - interval '1 day'":   `( "foo" = now() - interval '1 day' )`,
	"foo = now() - interval '2 weeks'": `( "foo" = now() - interval '2 weeks' )`,
	"foo = NOW() - INTERVAL '2 weeks'": `( "foo" = now() - interval '2 weeks' )`,
	"now() = foo":                      `( now() = "foo" )`,

	// Booleans
	`foo`:                       `"foo"`,
	`"FOO"`:                     `"FOO"`,
	`"with ""escaped"" quotes"`: `"with ""escaped"" quotes"`,
	`"""fully escaped"""`:       `"""fully escaped"""`,
	`foo is true`:               `( "foo" is true )`,
	`foo is TRue`:               `( "foo" is true )`,
	`foo is false`:              `( "foo" is false )`,
	`foo is FALSE`:              `( "foo" is false )`,
	`foo = true`:                `( "foo" = true )`,
	`foo = false`:               `( "foo" = false )`,
	`true = foo`:                `( true = "foo" )`,
	`foo is not false`:          `( "foo" is not false )`,
	`foo is not TRUE`:           `( "foo" is not true )`,

	// Not
	`not foo`:                            `( not "foo" )`,
	`not "FOO"`:                          `( not "FOO" )`,
	`not foo and not bar`:                `( ( not "foo" ) and ( not "bar" ) )`,
	`not foo = 'foo'`:                    `( not ( "foo" = 'foo' ) )`,
	`foo = 'foo' and not bar = 'bar'`:    `( ( "foo" = 'foo' ) and ( not ( "bar" = 'bar' ) ) )`,
	`not foo = 'foo' or not bar = 'bar'`: `( ( not ( "foo" = 'foo' ) ) or ( not ( "bar" = 'bar' ) ) )`,
	`not (foo = 'foo' and bar = 'bar')`:  `( not ( ( "foo" = 'foo' ) and ( "bar" = 'bar' ) ) )`,
	`foo = 'foo' and not (bar = 'bar' and baz = 'baz')`: `( ( "foo" = 'foo' ) and ( not ( ( "bar" = 'bar' ) and ( "baz" = 'baz' ) ) ) )`,

	// LIKE
	`foo like ''`:           `( "foo" like '' )`,
	`foo like 'bar'`:        `( "foo" like 'bar' )`,
	`foo like 'bar%'`:       `( "foo" like 'bar%' )`,
	`foo like '%bar%'`:      `( "foo" like '%bar%' )`,
	`foo like 'bar''s baz'`: `( "foo" like 'bar''s baz' )`,
	`foo like 'bar%baz'`:    `( "foo" like 'bar%baz' )`,
	`foo like 'bar_baz'`:    `( "foo" like 'bar_baz' )`,
	`foo LIKE 'bar'`:        `( "foo" like 'bar' )`,
	`foo LiKe 'bar'`:        `( "foo" like 'bar' )`,
	`foo not like 'bar'`:    `( "foo" not like 'bar' )`,
	`foo NoT LiKe 'bar'`:    `( "foo" not like 'bar' )`,

	// ILIKE
	`foo ilike ''`:           `( "foo" ilike '' )`,
	`foo ilike 'bar'`:        `( "foo" ilike 'bar' )`,
	`foo ilike 'bar%'`:       `( "foo" ilike 'bar%' )`,
	`foo ilike '%bar%'`:      `( "foo" ilike '%bar%' )`,
	`foo ilike 'bar''s baz'`: `( "foo" ilike 'bar''s baz' )`,
	`foo ilike 'bar%baz'`:    `( "foo" ilike 'bar%baz' )`,
	`foo ilike 'bar_baz'`:    `( "foo" ilike 'bar_baz' )`,
	`foo iLIKE 'bar'`:        `( "foo" ilike 'bar' )`,
	`foo iLiKe 'bar'`:        `( "foo" ilike 'bar' )`,
	`foo not ilike 'bar'`:    `( "foo" not ilike 'bar' )`,
	`foo NoT iLiKe 'bar'`:    `( "foo" not ilike 'bar' )`,

	// In
	`foo in ()`:                    `( "foo" in (  ) )`,
	`foo in (12)`:                  `( "foo" in ( 12 ) )`,
	`foo in (12,23)`:               `( "foo" in ( 12, 23 ) )`,
	`foo in ( 12, 23)`:             `( "foo" in ( 12, 23 ) )`,
	`foo in (12, 23 )`:             `( "foo" in ( 12, 23 ) )`,
	`foo in (12,23 )`:              `( "foo" in ( 12, 23 ) )`,
	`foo in ( 12,23 )`:             `( "foo" in ( 12, 23 ) )`,
	`foo in ( 12,23)`:              `( "foo" in ( 12, 23 ) )`,
	`foo in ('foo', 'bar')`:        `( "foo" in ( 'foo', 'bar' ) )`,
	`foo IN (12)`:                  `( "foo" in ( 12 ) )`,
	`foo not in ()`:                `( "foo" not in (  ) )`,
	`foo not in (12)`:              `( "foo" not in ( 12 ) )`,
	`foo not in ( 'foo' , 'bar' )`: `( "foo" not in ( 'foo', 'bar' ) )`,
	`foo NoT In (12)`:              `( "foo" not in ( 12 ) )`,

	// Null
	`foo is null`:     `( "foo" is null )`,
	`foo is NULL`:     `( "foo" is null )`,
	`foo is not NULL`: `( "foo" is not null )`,

	// Comparison operators
	`foo < 24`:  `( "foo" < 24 )`,
	`foo <= 24`: `( "foo" <= 24 )`,
	`foo = 24`:  `( "foo" = 24 )`,
	`foo != 24`: `( "foo" != 24 )`,
	`foo <> 24`: `( "foo" <> 24 )`,
	`foo >= 24`: `( "foo" >= 24 )`,
	`foo > 24`:  `( "foo" > 24 )`,

	// Identifiers
	`_ = 'foo'`:                         `( "_" = 'foo' )`,
	`Foo = 'foo'`:                       `( "foo" = 'foo' )`,
	`FoO = 'foo'`:                       `( "foo" = 'foo' )`,
	`foo_bar = 'foo'`:                   `( "foo_bar" = 'foo' )`,
	`f123 = 'foo'`:                      `( "f123" = 'foo' )`,
	`foo__bar = 'foo'`:                  `( "foo__bar" = 'foo' )`,
	`foo__bar__ = 'foo'`:                `( "foo__bar__" = 'foo' )`,
	`__foo__bar__ = 'foo'`:              `( "__foo__bar__" = 'foo' )`,
	`"foo" = 'foo'`:                     `( "foo" = 'foo' )`,
	`"foo bar" = 'foo'`:                 `( "foo bar" = 'foo' )`,
	`"FoO BaR" = 'foo'`:                 `( "FoO BaR" = 'foo' )`,
	`" foo bar " = 'foo'`:               `( " foo bar " = 'foo' )`,
	`"123_foo_bar" = 'foo'`:             `( "123_foo_bar" = 'foo' )`,
	`"with ""escaped"" quotes" = 'foo'`: `( "with ""escaped"" quotes" = 'foo' )`,
	`"""fully escaped""" = 'foo'`:       `( """fully escaped""" = 'foo' )`,

	// JSONB
	`foo ->> 'foo' = 'foo'`:                    `( "foo" ->> 'foo' = 'foo' )`,
	`foo ->> 0 = 'foo'`:                        `( "foo" ->> 0 = 'foo' )`,
	`foo = bar ->> 'bar'`:                      `( "foo" = "bar" ->> 'bar' )`,
	`foo -> 'foo' -> 'bar'`:                    `"foo" -> 'foo' -> 'bar'`,
	`foo -> 'foo' ->> 'bar' < "FOO" ->> 'foo'`: `( "foo" -> 'foo' ->> 'bar' < "FOO" ->> 'foo' )`,
	`foo -> 'with ''escaped'' quotes' = bar ->> '''fully escaped'''`: `( "foo" -> 'with ''escaped'' quotes' = "bar" ->> '''fully escaped''' )`,

	// AND and OR
	`foo and bar`:                         `( "foo" and "bar" )`,
	`foo or bar`:                          `( "foo" or "bar" )`,
	`foo and bar or baz`:                  `( ( "foo" and "bar" ) or "baz" )`,
	`foo and (bar or baz)`:                `( "foo" and ( "bar" or "baz" ) )`,
	`foo = 'foo' and bar = 'bar'`:         `( ( "foo" = 'foo' ) and ( "bar" = 'bar' ) )`,
	`foo = 'foo' or bar = 'bar'`:          `( ( "foo" = 'foo' ) or ( "bar" = 'bar' ) )`,
	`"FOO" = 'foo' and "BAR" = 'bar'`:     `( ( "FOO" = 'foo' ) and ( "BAR" = 'bar' ) )`,
	`foo = 'foo' aNd bar = 'bar'`:         `( ( "foo" = 'foo' ) and ( "bar" = 'bar' ) )`,
	`foo = 12 AND bar = 34`:               `( ( "foo" = 12 ) and ( "bar" = 34 ) )`,
	`foo = 12 AND bar = 34 and baz > 24`:  `( ( "foo" = 12 ) and ( "bar" = 34 ) and ( "baz" > 24 ) )`,
	`foo = 12 or bar = 34 or baz > 24`:    `( ( "foo" = 12 ) or ( "bar" = 34 ) or ( "baz" > 24 ) )`,
	`foo = 12 and bar = 34 or baz > 24`:   `( ( ( "foo" = 12 ) and ( "bar" = 34 ) ) or ( "baz" > 24 ) )`,
	`foo = 12 and (bar = 34 or baz > 24)`: `( ( "foo" = 12 ) and ( ( "bar" = 34 ) or ( "baz" > 24 ) ) )`,
	`foo = 12 or (bar = 34 or baz > 24)`:  `( ( "foo" = 12 ) or ( ( "bar" = 34 ) or ( "baz" > 24 ) ) )`,
	`(foo = 12) and bar = 34`:             `( ( "foo" = 12 ) and ( "bar" = 34 ) )`,
	`(foo = 12) and (bar = 34)`:           `( ( "foo" = 12 ) and ( "bar" = 34 ) )`,
	`foo = 12 and (bar = 34)`:             `( ( "foo" = 12 ) and ( "bar" = 34 ) )`,

	// Steampipe Cloud examples
	`type = 'user'`:                                         `( "type" = 'user' )`,
	`type = 'org'`:                                          `( "type" = 'org' )`,
	`status = 'accepted'`:                                   `( "status" = 'accepted' )`,
	`status = 'invited'`:                                    `( "status" = 'invited' )`,
	`action_type = 'workspace.mod.variable.setting.create'`: `( "action_type" = 'workspace.mod.variable.setting.create' )`,
	`created_at > now() - interval '7 days'`:                `( "created_at" > now() - interval '7 days' )`,
	`tags -> 'foo' is not null and created_at > now() - interval '7 days'`:                        `( ( "tags" -> 'foo' is not null ) and ( "created_at" > now() - interval '7 days' ) )`,
	`tags ->> 'foo' = 'bar' and created_at > now() - interval '7 days'`:                           `( ( "tags" ->> 'foo' = 'bar' ) and ( "created_at" > now() - interval '7 days' ) )`,
	`action_type = 'workspace.create' and identity_handle = 'jane' and created_at > '2022-07-14'`: `( ( "action_type" = 'workspace.create' ) and ( "identity_handle" = 'jane' ) and ( "created_at" > '2022-07-14' ) )`,
}

func TestValidCases(t *testing.T) {
	for tc, exp := range validCases {
		got, err := Parse("", []byte(tc))
		if err != nil {
			t.Errorf("%q: want no error, got %v", tc, err)
			continue
		}
		sql, _, err := ComparisonToSQL(got.(ComparisonNode), []string{})
		if err != nil {
			t.Errorf("SQL build error: %v", err)
			continue
		}
		if exp != sql {
			t.Errorf("%q: want %s, got %s", tc, exp, sql)
		}
	}
}

var invalidCases = []string{

	// Invalid SQL
	"'foo",
	"foo = ';delete from foo;",

	// Operators
	"foo == 24",
	"foo = = 24",
	"foo => 24",

	// Identifiers
	"123_foo_bar = 'foo'",

	// Operators type combinations
	"foo is 24",
	"foo is 'bar'",
	"foo like 24",
	"foo not like true",
	"foo ilike false",
	"foo not ilike 12",
}

func TestInvalidCases(t *testing.T) {
	for _, tc := range invalidCases {
		got, err := Parse("", []byte(tc))
		if err == nil {
			t.Errorf("%q: want error, got none (%v)", tc, got)
			continue
		}
		el, ok := err.(errList)
		if !ok {
			t.Errorf("%q: want error type %T, got %T", tc, &errList{}, err)
			continue
		}
		for _, e := range el {
			if _, ok := e.(*parserError); !ok {
				t.Errorf("%q: want all individual errors to be %T, got %T (%[3]v)", tc, &parserError{}, e)
			}
		}
		if !strings.Contains(err.Error(), "no match found") {
			t.Errorf("%q: wanted no match found, got \n%s\n", tc, err)
		}
	}
}
