package util

type QueryBuilder interface {
	BuildQuery(tableName string)
}
