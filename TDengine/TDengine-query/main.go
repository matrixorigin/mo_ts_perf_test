package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/taosdata/driver-go/v3/taosSql"
	"log"
	"os"
	"performance_testing/common"
	"strconv"
	"sync"
	"time"
)

var T string
var confirm string
var wg sync.WaitGroup
var dbConfig *common.DBConfig

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
	fmt.Printf("T=%d\n", T1)

	dbConfig, err = common.ReadDBFile("../conf/db.conf", common.TDengine)
	fmt.Printf("dbConfig:%v\n", *dbConfig)

	dsn := dbConfig.User + ":" + dbConfig.Password + "@tcp(" + dbConfig.Host + ":" + dbConfig.Port + ")/" + common.Database
	err, dbList := GetDbConn(T1, dsn)
	if err != nil {
		fmt.Printf("get dbconn fail:%v\n", err)
		return
	}

	defer func() {
		for _, dbConn := range dbList {
			dbConn.Close()
		}
	}()
	fmt.Printf("TDengine all clinet(%d thread) has ready!\n", T1)

	// 开始查询count总数
	startTime2 := time.Now()
	err, count := common.QueryCount(dbList[0])
	if err != nil {
		return
	}
	spendT2 := time.Since(startTime2).Seconds()
	fmt.Printf("'count(*)' query spend time:%f s\n\n", spendT2)

	f1 := func(db *sql.DB, wg1 *sync.WaitGroup) {
		defer wg1.Done()
		common.ExecQuery(db)
	}

	// 多客户端并发查询
	startTime := time.Now()
	for j := 0; j < T1; j++ {
		wg.Add(1)
		go f1(dbList[j], &wg)
		//fmt.Printf("clint(thread)%d started executing insert ……\n", j+1)
	}

	wg.Wait()
	spendT := time.Since(startTime).Seconds()
	fmt.Printf("'select *' (%d client concurrent query) spend time:%f s\n", T1, spendT)

	queryCount := count * T1
	records := float64(queryCount) / spendT
	fmt.Printf("query speed: %d/%f = %f records/second\n\n", queryCount, spendT, records)

	// 点查询
	startTime1 := time.Now()
	common.PointQuery(dbList[0], dbConfig.PointQueryTsCondition)
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

	startTime7 := time.Now()
	QueryTimeWindow(dbList[0])
	spendT7 := time.Since(startTime7).Seconds()
	fmt.Printf("TimeWindow query spend time:%f s\n", spendT7)

}

func GetDbConn(T1 int, dsn string) (error, []*sql.DB) {
	var dbList []*sql.DB
	fmt.Printf(" start create db conn, count:%d, dsn=%s\n", T1, dsn)
	for i := 0; i < T1; i++ {
		// 连接TDengine数据库
		conn, err := sql.Open("taosSql", dsn)
		if err != nil {
			fmt.Printf("TDengine connection[%d] failed\\n\", i+1", err)
			return err, dbList
		} else {
			fmt.Printf("TDengine connection[%d] created.\n", i+1) //Connection succeed
			dbList = append(dbList, conn)
		}
	}
	return nil, dbList
}

func QueryTimeWindow(db *sql.DB) {
	tbName := common.Database + "." + common.Table
	sql1 := fmt.Sprintf("select _wstart, _wend, max(current), min(current) from %s interval(60m) sliding(60m)", tbName)
	fmt.Printf("TimeWindow query sql:%s\n", sql1)
	rows, err := db.Query(sql1)
	if err != nil {
		fmt.Println(err)
		return
	}

	defer rows.Close()
	for rows.Next() {
		var (
			_wstart time.Time
			_wend   time.Time
			max1    float64
			min1    float64
		)
		err = rows.Scan(&_wstart, &_wend, &max1, &min1)
		if err != nil {
			log.Fatalln("scan error:\n", err)
			return
		}
		fmt.Println(_wstart, _wend, max1, min1)
	}
}
