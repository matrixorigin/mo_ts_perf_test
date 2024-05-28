package main

import (
	"flag"
	"fmt"
	"github.com/influxdata/influxdb1-client/v2"
	"log"
	"math/rand"
	"os"
	"performance_testing/common"
	"strconv"
	"strings"
	"sync"
	"time"
)

var T, r, n, retry, mode string
var confirm string
var dbConfig *common.DBConfig

const (
	database = "test"
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
	flag.StringVar(&r, "r", "10000", "The number of records per request. By default is 10000.")
	flag.StringVar(&T, "T", "7", " The number of threads. By default use 7")
	flag.StringVar(&n, "n", "200000", "Number of records for each table, default is 200000")
	flag.StringVar(&retry, "retry", "1", "Test retry count, calculate the average value finally, default 1")
	flag.StringVar(&mode, "mode", "multi", "Import mode, value is multi|single, multi table import or single table import, default multi.")
	flag.CommandLine.Parse(os.Args[firstArgWithDash:])
}

func main() {
	err, r1, T1, n1, retry1 := common.GetIntArgs(r, T, n, retry)
	if err != nil {
		return
	}
	fmt.Printf("r=%d, T=%d, n=%d, mode=%s, retry=%d \n", r1, T1, n1, mode, retry1)

	// 校验mode值
	if mode != "multi" && mode != "single" {
		fmt.Printf("unrecognized mode value:%s, required to be either multi or single, default multi.\n", mode)
		return
	}

	dbConfig, err = common.ReadDBFile("../conf/db.conf", common.InfluxDB)
	fmt.Printf("dbConfig:%v\n", *dbConfig)

	// 获取 T 个数据库连接
	err, dbList := GetDbConn(T1)
	if err != nil {
		fmt.Printf("get dbconn fail:%v\n", err)
		return
	}

	defer func() {
		for _, conn := range dbList {
			conn.Close()
		}
	}()
	fmt.Printf("influxdb all clinet(%d thread) has ready!\n", T1)

	// 初始化数据表，导入数据前先删除test数据库，再创建数据库，后续向test库中写数据d0、d1……
	if err = InitTable(dbList[0]); err != nil {
		return
	}

	fmt.Printf("start preparing test data.\n")
	getDataT := time.Now()
	// 准备导入的数据
	err, dataList := GetData(T1, n1, r1)
	if err != nil {
		fmt.Printf("get testing data err:%v\n", err)
		return
	}
	dataSpendT := time.Since(getDataT).Seconds()
	fmt.Printf("spend time of prepare testing data:%f s\n", dataSpendT)

	var wg sync.WaitGroup
	f1 := func(db client.Client, pb []*client.BatchPoints, wg1 *sync.WaitGroup) {
		defer wg1.Done()
		for i := 0; i < len(pb); i++ {
			execInsert(db, pb[i])
		}
	}

	// 每个测试测 {retry} 轮，求平均值
	var sumRecord float64
	for m := 0; m < retry1; m++ {
		fmt.Printf("按 Y 或者 回车键,将开始插入数据,按 N 将退出, 开的第%d次测试 \n", m+1)

		fmt.Scanln(&confirm)
		confirm = strings.TrimSpace(strings.ToUpper(confirm))
		if confirm == "Y" || confirm == "" {
			fmt.Printf("start…….\n")
		} else if confirm != "N" {
			fmt.Printf("exist.\n")
			return
		}
		if m != 0 {
			if err = TruncateTables(dbList[0], T1); err != nil {
				return
			}

			time.Sleep(time.Second * 1)
			fmt.Printf("tables has truncated and start insert data ……\n")
		}

		// 开始执行
		startTime := time.Now()
		// 开启T1个协程模拟客户端，并行执行写入操作
		for j := 0; j < T1; j++ {
			wg.Add(1)
			go f1(dbList[j], dataList[j], &wg)
			fmt.Printf("clint(thread)%d started executing insert ……\n", j+1)
		}

		wg.Wait()
		spendT := time.Since(startTime).Seconds()
		fmt.Printf("第%d次测试: spend time:%f s\n", m+1, spendT)

		count := n1 * T1
		// 计算写入速度
		records := float64(count) / spendT
		fmt.Printf("%d/%f = %f records/second\n", count, spendT, records)

		fmt.Printf("第%d次测试结果: %d/%f = %f records/second\n", m+1, count, spendT, records)

		sumRecord += records
	}
	recordsLast := sumRecord / float64(retry1)
	fmt.Printf("======== avg test resoult: %f/%d = %f records/second ===========\n", sumRecord, retry1, recordsLast)

}

func GetDbConn(T1 int) (error, []client.Client) {
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

func execInsert(db client.Client, pc *client.BatchPoints) {
	rand.Seed(42)

	err := db.Write(*pc)
	if err != nil {
		log.Fatal(err)
	}
}

func GetData(T1, n1, r1 int) (error, [][]*client.BatchPoints) {
	var data [][]*client.BatchPoints
	var startTimestamp int64 = 1500000000000
	var tableName string
	layout := "2006-01-02 15:04:05.000"

	subNum := n1 / r1
	rem := n1 % r1
	if rem > 0 {
		subNum += 1
	}

	//fmt.Printf("subNum=%d, rem=%d\n 开始准备数据:\n", subNum, rem)

	for z := 0; z < T1; z++ {
		var tData []*client.BatchPoints
		dataSize := r1

		// 根据写入模式，multi为多表写入，表名动态生成，第一个客户端向d0表写，第二个客户端向d1表写……以此类推
		// single为单表写入模式。不管几个客户端，都向test.d0表写数据
		if mode == "multi" {
			tableName = dbConfig.TablePrefix + strconv.Itoa(z)
		} else {
			tableName = table
		}

		for j := 0; j < subNum; j++ {
			if j == subNum-1 && rem > 0 {
				dataSize = rem
			}

			bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
				Database: database,
				//Precision: "s",
			})
			for i := 0; i < dataSize; i++ {
				v0 := time.UnixMilli(startTimestamp).Format(layout)
				startTimestamp++
				sevenDigitRandomNumber := fmt.Sprintf("%.7f", rand.Float64())
				random1 := strconv.FormatInt(int64(rand.Intn(7)-3), 10)
				current, _ := strconv.ParseFloat(random1+sevenDigitRandomNumber[1:], 64)
				random2 := fmt.Sprintf("%.7f", -rand.Float64())
				phase, _ := strconv.ParseFloat(random2, 64)
				tags := map[string]string{
					//"cpu":    "cpu-total",
					//"host":   fmt.Sprintf("host%d", rand.Intn(1000)),
					//"region": regions[rand.Intn(len(regions))],
				}

				fields := map[string]interface{}{
					"current": current,
					"voltage": rand.Intn(20),
					"phase":   phase,
				}

				// 使用 Parse 将时间字符串转换为 Time 类型
				t1, err := time.Parse(layout, v0)
				if err != nil {
					fmt.Println("time parse fail:", err)
					return err, data
				}

				pt, err := client.NewPoint(tableName, tags, fields, t1)
				if err != nil {
					log.Fatalln("Error: ", err)
					return err, data
				}

				bp.AddPoint(pt)
			}
			tData = append(tData, &bp)
		}

		data = append(data, tData)
	}

	return nil, data
}

func InitTable(cli client.Client) error {
	q := `DROP DATABASE ` + database
	query := client.NewQuery(q, database, "ns")
	response, err := cli.Query(query)
	if err != nil {
		fmt.Printf(" drop database %s failed, err:%v\n", database, err)
		return err
	}

	// 处理响应
	err = response.Error()
	if err != nil {
		fmt.Printf(" response of drop database %s is failed, err:%v\n", database, err)
		return err
	}

	q = `CREATE DATABASE ` + database
	query = client.NewQuery(q, database, "ns")
	response, err = cli.Query(query)
	if err != nil {
		fmt.Printf(" create database %s failed, err:%v\n", database, err)
		return err
	}

	// 处理响应
	err = response.Error()
	if err != nil {
		fmt.Printf(" response of create database %s is failed, err:%v\n", database, err)
		return err
	}

	fmt.Printf("Initialize database completed.\n")
	return nil
}

func TruncateTables(cli client.Client, T1 int) error {
	if mode == "multi" {
		var tableName string
		for z := 0; z < T1; z++ {
			tableName = dbConfig.TablePrefix + strconv.Itoa(z)
			if err := TruncateTb(cli, tableName); err != nil {
				return err
			}
		}
	} else {
		// 执行删除表的操作 DROP measurement d0;
		if err := TruncateTb(cli, table); err != nil {
			return err
		}
	}
	return nil
}

func TruncateTb(cli client.Client, tableName string) error {
	q := `DROP measurement ` + tableName
	query := client.NewQuery(q, database, "ns")
	response, err := cli.Query(query)
	if err != nil {
		fmt.Printf(" drop table %s failed, err:%v\n", tableName, err)
		return err
	}

	// 处理响应
	if response.Error() != nil {
		fmt.Printf(" response of drop table %s is failed, err:%v\n", tableName, err)
		return response.Error()
	}
	return nil
}
