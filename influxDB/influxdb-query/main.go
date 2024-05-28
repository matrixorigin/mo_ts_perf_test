package main

import (
	"encoding/json"
	"flag"
	"fmt"
	client "github.com/influxdata/influxdb/client/v2"
	"log"
	"os"
	"performance_testing/common"
	"strconv"
	"sync"
	"time"
)

var T string
var dbConfig *common.DBConfig
var confirm string

const (
	database = "test" //数据库名
	table    = "d0"
)

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

	dbConfig, err = common.ReadDBFile("../conf/db.conf", common.InfluxDB)
	fmt.Printf("dbConfig:%v\n", *dbConfig)

	err, dbList := GetDbconn(T1)
	if err != nil {
		fmt.Printf("get dbconn fail:%v\n", err)
		return
	}

	defer func() {
		for _, dbConn := range dbList {
			dbConn.Close()
		}
	}()
	fmt.Printf("influxdb all clinet(%d thread) has ready!\n", T1)

	// 开始查询count总数
	startTime2 := time.Now()
	err, count := QueryCount(dbList[0])
	if err != nil {
		return
	}
	spendT2 := time.Since(startTime2).Seconds()
	fmt.Printf("'count(*)' query spend time:%f s\n\n", spendT2)

	var wg sync.WaitGroup
	f1 := func(db client.Client, wg1 *sync.WaitGroup) {
		defer wg1.Done()
		//defer db.Close()
		execQuery(db)
	}

	//开始执行
	startTime := time.Now()
	for j := 0; j < T1; j++ {
		wg.Add(1)
		go f1(dbList[j], &wg)
	}

	wg.Wait()
	spendT := time.Since(startTime).Seconds()
	fmt.Printf("'select *' (%d client concurrent query) spend time:%f s\n", T1, spendT)

	queryCount := count * T1
	records := float64(queryCount) / spendT
	fmt.Printf("query speed: %d/%f = %f records/second\n\n", queryCount, spendT, records)

	// 开始执行
	startTime1 := time.Now()
	QuerySingle(dbList[0], dbConfig.PointQueryTsCondition)
	spendT1 := time.Since(startTime1).Seconds()
	fmt.Printf("'point query' spend time:%f s\n\n", spendT1)

	startTime3 := time.Now()
	QueryAvg(dbList[0])
	spendT3 := time.Since(startTime3).Seconds()
	fmt.Printf("'avg(current)' query spend time:%f s\n\n", spendT3)

	startTime4 := time.Now()
	QuerySum(dbList[0])
	spendT4 := time.Since(startTime4).Seconds()
	fmt.Printf("'sum(current)' query spend time:%f s\n\n", spendT4)

	startTime5 := time.Now()
	QueryMax(dbList[0])
	spendT5 := time.Since(startTime5).Seconds()
	fmt.Printf("'max(current)' query spend time:%f s\n\n", spendT5)

	startTime6 := time.Now()
	QueryMin(dbList[0])
	spendT6 := time.Since(startTime6).Seconds()
	fmt.Printf("'min(current)' query spend time:%f s\n\n", spendT6)

	startTime7 := time.Now()
	QueryTimeWindow(dbList[0])
	spendT7 := time.Since(startTime7).Seconds()
	fmt.Printf("TimeWindow query spend time:%f s\n", spendT7)
}

func GetDbconn(T1 int) (error, []client.Client) {
	var dbList []client.Client
	fmt.Printf("start create db conn, count:%d\n", T1)
	addr := fmt.Sprintf("http://%s:%s", dbConfig.Host, dbConfig.Port)
	for i := 0; i < T1; i++ {
		c, err := client.NewHTTPClient(client.HTTPConfig{
			Addr:     addr,
			Username: dbConfig.User,
			Password: dbConfig.Password,
		})

		if err != nil {
			fmt.Printf("database clinet[%d] failed:%v\n", i+1, err) //Connection failed
			return err, dbList
		} else {
			fmt.Printf("database clinet[%d] created.\n", i+1) //Connection succeed
			dbList = append(dbList, c)
		}
	}
	return nil, dbList
}

func execQuery(db client.Client) {
	sql := fmt.Sprintf("select * from %s", table)
	query := client.NewQuery(sql, database, "ns")
	_, err := db.Query(query)
	if err != nil {
		fmt.Println(err)
	}
}

func QuerySingle(db client.Client, tsCondition string) {
	sql := fmt.Sprintf("select * from %s where time=%s", table, tsCondition)
	query := client.NewQuery(sql, database, "ns")
	result, err := db.Query(query)
	if err != nil {
		fmt.Println(err)
		return
	}

	// 解析结果
	if result.Error() != nil {
		log.Fatal(result.Error())
	}

	// 遍历并打印结果
	for _, r := range result.Results {
		for _, v := range r.Series {
			//fmt.Printf("Series: %s\n", v.Name)
			for _, row := range v.Values {
				fmt.Printf(" point query result: %v"+"\n", row)
			}
		}
	}
}

func QueryCount(db client.Client) (error, int) {
	var count int
	query := client.NewQuery(fmt.Sprintf("select count(*) from %s", table), database, "ns")
	result, err := db.Query(query)
	if err != nil {
		fmt.Println(err)
		return err, count
	}

	// 解析结果
	if result.Error() != nil {
		log.Fatal(result.Error())
		return err, count
	}

	// 遍历并打印结果
	for _, r := range result.Results {
		for _, series := range r.Series {
			for _, values := range series.Values {
				numInt64, _ := values[1].(json.Number).Int64()
				count = int(numInt64)
				fmt.Printf("\n count value is:%d\n", count)
				return err, count
			}
		}
	}

	return nil, count

}

func QueryAvg(db client.Client) {
	query := client.NewQuery(fmt.Sprintf("select MEAN(current) from %s", table), database, "ns")
	result, err := db.Query(query)
	if err != nil {
		fmt.Println(err)
	}

	// 解析结果
	if result.Error() != nil {
		log.Fatal(result.Error())
	}

	// 遍历并打印结果
	for _, r := range result.Results {
		for _, series := range r.Series {
			for _, values := range series.Values {
				fmt.Printf("avg value is:%v\n", values[1])
				return
			}
		}
	}
}

func QuerySum(db client.Client) {
	query := client.NewQuery(fmt.Sprintf("select sum(current) from %s", table), database, "ns")
	result, err := db.Query(query)
	if err != nil {
		fmt.Println(err)
	}

	// 解析结果
	if result.Error() != nil {
		log.Fatal(result.Error())
	}

	// 遍历并打印结果
	for _, r := range result.Results {
		for _, series := range r.Series {
			for _, values := range series.Values {
				fmt.Println(" sum value is:", values[1])
				return
			}
		}
	}
}

func QueryMax(db client.Client) {
	query := client.NewQuery(fmt.Sprintf("select max(current) from %s", table), database, "ns")
	result, err := db.Query(query)
	if err != nil {
		fmt.Println(err)
	}

	// 解析结果
	if result.Error() != nil {
		log.Fatal(result.Error())
	}

	// 遍历并打印结果
	for _, r := range result.Results {
		for _, series := range r.Series {
			for _, values := range series.Values {
				fmt.Println(" max value is:", values[1])
				return
			}
		}
	}
}

func QueryMin(db client.Client) {
	query := client.NewQuery(fmt.Sprintf("select min(current) from %s", table), database, "ns")
	result, err := db.Query(query)
	if err != nil {
		fmt.Println(err)
	}

	// 解析结果
	if result.Error() != nil {
		log.Fatal(result.Error())
	}
	// 遍历并打印结果
	for _, r := range result.Results {
		for _, series := range r.Series {
			for _, values := range series.Values {
				fmt.Println(" min value is:", values[1])
				return
			}
		}
	}
}

func QueryTimeWindow(db client.Client) {
	sql1 := fmt.Sprintf("select max(current), min(current) from %s where time >= %s and time < %s group by time(60m)", table, dbConfig.InfluxdbTimeWindowStart, dbConfig.InfluxdbTimeWindowEnd)
	//sql1 := fmt.Sprintf("select max(current), min(current) from %s where time >= '2017-07-14 10:40:00.000' and time < '2017-07-14 13:26:39.999' group by time(60m)", table)
	//sql1 := fmt.Sprintf("select max(current), min(current) from %s where time >= 1500028800000000000 and time < 1500028800005000000 group by time(60m)", table)
	fmt.Printf("TimeWindow query sql:%s\n", sql1)
	query := client.NewQuery(sql1, database, "ns")
	result, err := db.Query(query)
	if err != nil {
		fmt.Println(err)
	}

	// 解析结果
	if result.Error() != nil {
		log.Fatal(result.Error())
	}

	// 遍历并打印结果
	for _, r := range result.Results {
		for _, _ = range r.Series {
			//for _, v := range r.Series {
			//fmt.Printf("Series: %s\n", v.Name)
			//for _, row := range v.Values {
			//	fmt.Printf("Row: %v\n", row)
			//}
		}
	}
}
