module performance_testing/clickhouse/ck-query

go 1.21.3

require (
	github.com/ClickHouse/clickhouse-go v1.5.4
	performance_testing/common v0.0.0-00010101000000-000000000000
)

require (
	github.com/astaxie/beego v1.12.3 // indirect
	github.com/cloudflare/golz4 v0.0.0-20150217214814-ef862a3cdc58 // indirect
	github.com/go-sql-driver/mysql v1.5.0 // indirect
	github.com/shiena/ansicolor v0.0.0-20151119151921-a422bbe96644 // indirect
)

replace performance_testing/common => ../../common
