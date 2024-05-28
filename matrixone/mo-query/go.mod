module mo-query

go 1.20

require (
	github.com/go-sql-driver/mysql v1.8.1 // indirect
	gorm.io/driver/mysql v1.5.6
	gorm.io/gorm v1.25.9
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/astaxie/beego v1.12.3 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/shiena/ansicolor v0.0.0-20151119151921-a422bbe96644 // indirect
	performance_testing/common v0.0.0-00010101000000-000000000000 // indirect
)

replace performance_testing/common => ../../common
