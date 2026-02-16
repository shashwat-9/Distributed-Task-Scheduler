package util

import "fmt"

const updateQueryTemplate string = "UPDATE %s SET %s WHERE %s"

func BuildUpdateQuery(tableName string, params []string) string {
	return fmt.Sprintf(updateQueryTemplate, tableName, params[0], params[1])
}
