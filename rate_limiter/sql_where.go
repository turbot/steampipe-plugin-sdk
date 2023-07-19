package rate_limiter

import (
	"fmt"
	"github.com/xwb1989/sqlparser"
)

func parseWhere(w string) (sqlparser.Expr, error) {
	// convert where clause to valid SQL statement
	sql := fmt.Sprintf("select * from a where %s", w)
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, err

	}
	return stmt.(*sqlparser.Select).Where.Expr, nil
}

func whereSatisfied(whereExpr sqlparser.Expr, values map[string]string) bool {
	//switch e := whereExpr.(type){
	//case *sqlparser.ComparisonExpr
	//}
	return true
}
