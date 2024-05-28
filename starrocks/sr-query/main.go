package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"os"
	"performance_testing/common"
	"strconv"
	"sync"
	"time"
)

var T, table string

func init() {
	firstArgWithDash := 1

	for i := 1; i < len(os.Args); i++ {
		firstArgWithDash = i
		if len(os.Args[i]) > 0 && os.Args[i][0] == '-' {
			break
		}
	}

	flag.StringVar(&T, "T", "1", " The number of threads. default 1")
	flag.CommandLine.Parse(os.Args[firstArgWithDash:])
}

func main() {
	var T1 int
	var err error

	T1, err = strconv.Atoi(T)
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}

	fmt.Printf("T=%d \n", T1)

	srConfig, err := common.ReadSRFile("../conf/db.conf")
	url := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?loc=UTC&parseTime=true", srConfig.User, srConfig.Password, srConfig.Host, srConfig.JdbcPort, srConfig.Database)
	fmt.Printf("url:%s\n", url)
	table = srConfig.Database + "." + srConfig.Table

	// 创建T个数据库连接
	err, dbList := GetDbconn(T1, url)
	if err != nil {
		fmt.Printf("get dbconn fail:%v\n", err)
		return
	}

	defer func() {
		for _, dbConn := range dbList {
			dbConn.Close()
		}
	}()

	// 开始查询count总数
	startTime2 := time.Now()
	err, count := common.QueryCount(dbList[0])
	if err != nil {
		return
	}
	spendT2 := time.Since(startTime2).Seconds()
	fmt.Printf("'count(*)' query spend time:%f s\n\n", spendT2)

	var wg sync.WaitGroup
	startTime := time.Now()
	// 多客户端执行：select *
	for i := 0; i < T1; i++ {
		wg.Add(1)
		go func(db *sql.DB) {
			common.ExecQuery(db)
			wg.Done()
		}(dbList[i])
	}

	wg.Wait()
	spendT := time.Since(startTime).Seconds()
	fmt.Printf("'select *' (%d client concurrent query) spend time:%f s\n", T1, spendT)

	queryCount := count * T1
	records := float64(queryCount) / spendT
	fmt.Printf("query speed: %d/%f = %f records/second\n\n", queryCount, spendT, records)

	// 点查询
	startTime1 := time.Now()
	common.PointQuery(dbList[0], srConfig.PointQueryTsCondition)
	spendT1 := time.Since(startTime1).Seconds()
	fmt.Printf("'point query' spend time:%f s\n\n", spendT1)

	// 查询current平均值
	startTime3 := time.Now()
	common.QueryAvg(dbList[0])
	spendT3 := time.Since(startTime3).Seconds()
	fmt.Printf("'avg(current)' query spend time:%f s\n\n", spendT3)

	// 查询current总和
	startTime4 := time.Now()
	common.QuerySum(dbList[0])
	spendT4 := time.Since(startTime4).Seconds()
	fmt.Printf("'sum(current)' query spend time:%f s\n\n", spendT4)

	// 查询current字段最大值
	startTime5 := time.Now()
	common.QueryMax(dbList[0])
	spendT5 := time.Since(startTime5).Seconds()
	fmt.Printf("'max(current)' query spend time:%f s\n\n", spendT5)

	// 查询current字段最小值
	startTime6 := time.Now()
	common.QueryMin(dbList[0])
	spendT6 := time.Since(startTime6).Seconds()
	fmt.Printf("'min(current)' query spend time:%f s\n\n", spendT6)
}

func GetDbconn(T1 int, url string) (error, []*sql.DB) {
	var dbList []*sql.DB
	fmt.Printf("start create db conn, count:%d\n", T1)
	for i := 0; i < T1; i++ {
		db, err := sql.Open("mysql", url)
		// get connection
		if err != nil {
			fmt.Println("Database Connection Failed") //Connection failed
			return err, dbList
		} else {
			fmt.Println("Database Connection Succeed") //Connection succeed
			dbList = append(dbList, db)
		}
	}

	return nil, dbList
}
