package main

import (
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/taosdata/driver-go/v3/taosSql"
	"math/rand"
	"os"
	"performance_testing/common"
	"strconv"
	"strings"
	"sync"
	"time"
)

var r, T, n, t, mode, retry string
var confirm string
var wg sync.WaitGroup
var dbConfig *common.DBConfig

const (
	multi  = "multi"
	meters = "meters"
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
	flag.StringVar(&n, "n", "10000", "Number of records for each table, default is 10000")
	flag.StringVar(&t, "t", "1000", "Number of tables written, default is 1000")
	flag.StringVar(&mode, "mode", "multi", "Import mode, value is multi|single, multi table import or single table import, default multi.")
	flag.StringVar(&retry, "retry", "1", "Test retry count, calculate the average value finally, default 1")
	flag.CommandLine.Parse(os.Args[firstArgWithDash:])
}

func main() {
	err, r1, T1, n1, retry1 := common.GetIntArgs(r, T, n, retry)
	if err != nil {
		return
	}

	if mode == multi {
		fmt.Printf("TDengine 多表写入测试: 可使用 taosdemo .\n")
		return
	}

	t1, err := strconv.Atoi(t)
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}
	fmt.Printf("r=%d, T=%d, n=%d,t=%d, retry=%d\n", r1, T1, n1, t1, retry1)

	dbConfig, err = common.ReadDBFile("../conf/db.conf", common.TDengine)
	fmt.Printf("dbConfig:%v\n", *dbConfig)

	dsn := dbConfig.User + ":" + dbConfig.Password + "@tcp(" + dbConfig.Host + ":" + dbConfig.Port + ")/"
	err, dbList := GetDbConn(T1, dsn)
	if err != nil {
		fmt.Printf("get dbconn fail:%v\n", err)
		return
	}

	defer func() {
		for _, conn := range dbList {
			conn.Close()
		}
	}()

	fmt.Printf("data of all clinet(%d thread) has ready!\n", T1)

	// 初始化数据库表，导入数据前先删除、新建test数据库，再创建表d0、d1、d2……
	if err = InitTable(dbList[0], t1); err != nil {
		return
	}

	fmt.Printf("start preparing test data.\n")

	startDate := time.Now()
	err, dataList := GetData(T1, n1, r1, t1)
	if err != nil {
		fmt.Printf("GetData err:%v\n", err)
		return
	}

	f1 := func(db *sql.DB, sqlData []string, wg1 *sync.WaitGroup) {
		defer wg1.Done()
		for i := 0; i < len(sqlData); i++ {
			if err = common.ExecSql(db, sqlData[i]); err != nil {
				fmt.Println(err)
				return
			}
		}
	}
	fmt.Printf(" cost-time:%f s\n", time.Since(startDate).Seconds())

	var sumRecord float64
	// 每个测试测 retry1 轮，求平均值
	for k := 0; k < retry1; k++ {
		fmt.Printf("按 Y 或者 回车键,将开始插入数据,按 N 将退出, 第 %d 次\n", k+1)
		fmt.Scanln(&confirm)
		confirm = strings.TrimSpace(strings.ToUpper(confirm))
		if confirm == "Y" || confirm == "" {
			fmt.Printf("start…….\n")
		} else if confirm != "N" {
			fmt.Printf("exist.\n")
			return
		}

		if k != 0 {
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
			//fmt.Printf("clint(thread)%d started executing insert ……\n", j+1)
		}

		wg.Wait()
		spendT := time.Since(startTime).Seconds()
		fmt.Printf(" spend time:%f s\n", spendT)

		count := n1 * T1
		if mode == multi {
			count = t1 * n1

		}
		// 计算写入速度
		records := float64(count) / spendT
		fmt.Printf("%d/%f = %f records/second\n", count, spendT, records)

		sumRecord += records
	}
	recordsLast := sumRecord / float64(retry1)
	fmt.Printf("======== avg test: %f/%f = %f records/second ===========\n", sumRecord, float64(retry1), recordsLast)

}

func GetDbConn(T1 int, dsn string) (error, []*sql.DB) {
	var dbList []*sql.DB
	fmt.Printf("start create db conn, count:%d, dsn=%s\n", T1, dsn)
	for i := 0; i < T1; i++ {
		// 连接TDengine数据库
		conn, err := sql.Open("taosSql", dsn)
		if err != nil {
			fmt.Printf("TDengine connection[%d] failed:%v\n", i+1, err)
			return err, dbList
		} else {
			fmt.Printf("TDengine connection[%d] created.\n", i+1) //Connection succeed
			dbList = append(dbList, conn)
		}
	}
	return nil, dbList
}

func GetData(T1, n1, r1, t1 int) (error, [][]string) {
	var data [][]string
	var startTimestamp int64 = 1500000000000
	var tableName string
	var startIndex int
	layout := "2006-01-02 15:04:05.000"

	//fmt.Printf("subNum=%d, rem=%d\n 开始准备数据:\n", subNum, rem)

	// 多表参数
	TbCount := t1 / T1
	ext := t1 % T1

	// 单表参数
	subNumS := n1 / r1
	remS := n1 % r1
	if remS > 0 {
		subNumS += 1
	}

	for z := 0; z < T1; z++ {
		var tData []string

		// 根据写入模式，multi为多表写入，将要写入的表平均分给所有客户端，共同写入
		// single为单表写入模式。不管几个客户端，都向test.d0表写数据
		if mode == multi {
			// 这些参数仅针对多表写入
			tbSize := TbCount
			if z+1 <= ext {
				tbSize++
			}
			endIndex := startIndex + tbSize

			subNum := tbSize
			rem := r1 % tbSize
			if rem > 0 {
				subNum++
			}
			for x1 := 0; x1 < subNum; x1++ {
				//var buffer strings.Builder
				var buffer bytes.Buffer
				var sqlPrefix string

				dataSize := r1 / tbSize
				if x1 == subNum-1 && rem > 0 {
					dataSize = rem
				}
				for x := startIndex; x < endIndex; x++ {
					tableName = common.Database + "." + dbConfig.TablePrefix + strconv.Itoa(x)
					if x == startIndex {
						//sqlPrefix = fmt.Sprintf("INSERT INTO %s USING test.meters TAGS('California.SanFrancisco', 2) VALUES", tableName)
						sqlPrefix = fmt.Sprintf("INSERT INTO %s VALUES", tableName)
					} else {
						sqlPrefix = fmt.Sprintf(" %s VALUES", tableName)
						//sqlPrefix = fmt.Sprintf(" %s USING test.meters TAGS('California.SanFrancisco', 2) VALUES", tableName)
					}
					buffer.WriteString(sqlPrefix)
					for i := 0; i < dataSize; i++ {
						tsValue := "'" + time.UnixMilli(startTimestamp).Format(layout) + "'"
						startTimestamp++
						sevenDigitRandomNumber := fmt.Sprintf("%.7f", rand.Float64())
						random1 := strconv.FormatInt(int64(rand.Intn(7)-3), 10)
						current := random1 + sevenDigitRandomNumber[1:]
						voltage := rand.Intn(20)
						phase := fmt.Sprintf("%.7f", -rand.Float64())

						buffer.WriteString(fmt.Sprintf(" (%s, %s, %d,%s)", tsValue, current, voltage, phase))
					}
				}
				tData = append(tData, buffer.String())
				//fmt.Printf("%s\n", buffer.String())
			}
			startIndex = endIndex
		} else {
			dataSize := r1
			for j := 0; j < subNumS; j++ {
				if j == subNumS-1 && remS > 0 {
					dataSize = remS
				}
				//var buffer strings.Builder
				var buffer bytes.Buffer
				var sqlPrefix string

				tableName = common.Database + "." + common.Table
				sqlPrefix = fmt.Sprintf("INSERT INTO %s VALUES", tableName)
				buffer.WriteString(sqlPrefix)
				for i := 0; i < dataSize; i++ {
					tsValue := "'" + time.UnixMilli(startTimestamp).Format(layout) + "'"
					startTimestamp++
					sevenDigitRandomNumber := fmt.Sprintf("%.7f", rand.Float64())
					random1 := strconv.FormatInt(int64(rand.Intn(7)-3), 10)
					current := random1 + sevenDigitRandomNumber[1:]
					voltage := rand.Intn(20)
					phase := fmt.Sprintf("%.7f", -rand.Float64())

					buffer.WriteString(fmt.Sprintf(" (%s, %s, %d,%s)", tsValue, current, voltage, phase))
				}
				tData = append(tData, buffer.String())
			}
		}
		data = append(data, tData)

	}

	return nil, data
}

func InitTable(db *sql.DB, t1 int) error {
	dropDatabaseSQL := fmt.Sprintf("DROP DATABASE IF EXISTS %s", common.Database)
	_, err := db.Exec(dropDatabaseSQL)
	if err != nil {
		fmt.Printf("drop database  %s fail:%v \n", common.Database, err)
		return err
	}

	ctDatabaseSQL := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", common.Database)
	_, err = db.Exec(ctDatabaseSQL)
	if err != nil {
		fmt.Printf("create database  %s fail:%v \n", common.Database, err)
		return err
	}

	STableName := common.Database + "." + meters
	ctSTable := fmt.Sprintf("CREATE STABLE if not exists %s (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);", STableName)
	_, err = db.Exec(ctSTable)
	if err != nil {
		fmt.Printf("create STable  %s fail:%v \n", ctSTable, err)
		return err
	}

	dsn1 := dbConfig.User + ":" + dbConfig.Password + "@tcp(" + dbConfig.Host + ":" + dbConfig.Port + ")/" + common.Database
	err, dbList2 := GetDbConn(1, dsn1)
	if err != nil {
		fmt.Printf("get dbconn fail:%v\n", err)
		return err
	}

	defer func() {
		for _, conn := range dbList2 {
			conn.Close()
		}
	}()

	ctTableTempte := "CREATE TABLE IF NOT EXISTS %s USING " + meters + " TAGS (\"test\", %d);"
	f := func(tableName string, groupId int) error {
		ctTableSQL := fmt.Sprintf(ctTableTempte, tableName, groupId)
		_, err = dbList2[0].Exec(ctTableSQL)
		if err != nil {
			fmt.Printf("create table %s fail:%v \n", tableName, err)
			return err
		}
		return nil
	}

	// 根据写入模式，multi为多表写入，表名动态生成，第一个客户端向d0表写，第二个客户端向d1表写……以此类推
	// single为单表写入模式。不管几个客户端，都向test.d0表写数据
	var tableName string
	if mode == multi {
		for z := 0; z < t1; z++ {
			tableName = common.Database + "." + dbConfig.TablePrefix + strconv.Itoa(z)
			if err = f(tableName, z); err != nil {
				return err
			}
		}
	} else {
		tableName = common.Database + "." + common.Table
		if err = f(tableName, 0); err != nil {
			return err
		}
	}

	fmt.Printf("Initialize database and table completed.\n")
	return nil

}

func TruncateTables(db *sql.DB, T1 int) error {
	var tableName, delSql string
	if mode == multi {
		for z := 0; z < T1; z++ {
			tableName = common.Database + "." + dbConfig.TablePrefix + strconv.Itoa(z)
			delSql = fmt.Sprintf("delete from %s", tableName)
			if err := common.ExecSql(db, delSql); err != nil {
				fmt.Printf("truncate table %s fail:%v \n", tableName, err)
				return err
			}
		}
	} else {
		// 执行删除表的操作 DROP measurement d0;
		tableName = common.Database + "." + common.Table
		delSql = fmt.Sprintf("delete from %s", tableName)
		if err := common.ExecSql(db, delSql); err != nil {
			if err != nil {
				fmt.Printf("truncate table %s fail:%v \n", tableName, err)
			}
			return err
		}
	}
	return nil
}
