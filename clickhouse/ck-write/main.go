package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"math/rand"
	"os"
	"performance_testing/common"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var r, T, n, retry string
var confirm, mode string

var dbConfig *common.DBConfig

const (
	multi  = "multi"
	single = "single"
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
	flag.StringVar(&n, "n", "500000", "Number of records for each table, default is 500000")
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
	if mode != multi && mode != single {
		fmt.Printf("unrecognized mode value:%s, required to be either multi or single, default multi.\n", mode)
		return
	}

	dbConfig, err = common.ReadDBFile("../conf/db.conf", common.CK)
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

	fmt.Printf("all clinet(%d thread) has ready!\n", T1)

	//初始化数据库表，导入数据前先删除、新建test数据库，再创建表d0、d1、d2……
	if err = InitTable(T1); err != nil {
		return
	}

	subNum := n1 / r1
	rem := n1 % r1
	if rem > 0 {
		subNum += 1
	}

	var startTimestamp int64 = 1500000000000
	layout := "2006-01-02 15:04:05.000"

	var wg sync.WaitGroup
	f1 := func(conn driver.Conn, wg1 *sync.WaitGroup, j int) {
		defer wg1.Done()
		var tableName string
		if mode == multi {
			tableName = common.Database + "." + "d" + strconv.Itoa(j)
			//tableName = common.Database + "." + dbConfig.TablePrefix + strconv.Itoa(j)
		} else {
			tableName = common.Database + "." + common.Table
		}

		dataSize := r1
		for i := 0; i < subNum; i++ {
			if i == subNum-1 && rem > 0 {
				dataSize = rem
			}

			sql := fmt.Sprintf("INSERT INTO %s (ts,current,voltage,phase) VALUES", tableName)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			batch, err1 := conn.PrepareBatch(ctx, sql)
			if err1 != nil {
				fmt.Printf("clickhouse PrepareBatch err:%v \n", err)
				return
			}

			for z := 0; z < dataSize; z++ {
				t := atomic.LoadInt64(&startTimestamp)
				tsValue := time.UnixMilli(t).Format(layout)
				atomic.AddInt64(&startTimestamp, 1)
				current := rand.Float64() * 0.101
				voltage := rand.Intn(20)
				phase := -rand.Float64()

				err = batch.Append(tsValue, current, voltage, phase)
				if err != nil {
					fmt.Printf("clickhouse batch append err:%v \n", err)
					return
				}
			}

			if err := batch.Send(); err != nil {
				fmt.Printf("send to clickhouse err:%v \n", err)
				return
			}
		}
	}

	var sumRecord float64
	// 每个测试测 retry 轮，求平均值
	for k := 0; k < retry1; k++ {
		fmt.Printf("按 Y 或者 回车键,将开始插入数据,按 N 将退出, 开的第%d次测试\n", k+1)
		fmt.Scanln(&confirm)
		confirm = strings.TrimSpace(strings.ToUpper(confirm))
		if confirm == "Y" || confirm == "" {
			fmt.Printf("start test %d …….\n", k+1)
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
			go f1(dbList[j], &wg, j)
			fmt.Printf("client(thread)%d started executing insert table ……\n", j+1)
		}

		wg.Wait()
		spendT := time.Since(startTime).Seconds()
		fmt.Printf(" spend time:%f s\n", spendT)

		count := n1 * T1
		records := float64(count) / spendT
		fmt.Printf("%d/%f = %f records/second\n", count, spendT, records)
		sumRecord += records
	}
	recordsLast := sumRecord / float64(retry1)
	fmt.Printf("======== avg test: %f/%d = %f records/second ===========\n", sumRecord, retry1, recordsLast)

}

func GetDbConn(T1 int) (error, []driver.Conn) {
	var dbList []driver.Conn

	fmt.Printf(" start create db conn, count:%d\n", T1)

	url := dbConfig.Host + ":" + dbConfig.Port
	for i := 0; i < T1; i++ {
		conn, err := clickhouse.Open(&clickhouse.Options{
			Addr: []string{url},
			Auth: clickhouse.Auth{
				Database: common.Database,
				Username: dbConfig.User,
				Password: dbConfig.Password,
			},
			Settings: clickhouse.Settings{
				"max_execution_time": 60,
			},
			DialTimeout: 5 * time.Second,
			Compression: &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			},
		})
		if err != nil {
			return err, nil
		}

		dbList = append(dbList, conn)
	}
	return nil, dbList
}

func InitTable(T1 int) error {
	dsn := "tcp://" + dbConfig.Host + ":" + dbConfig.Port + "?username=" + dbConfig.User + "&password=" + dbConfig.Password
	err, db := GetCKConn(dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	dropDatabaseSQL := fmt.Sprintf("DROP DATABASE IF EXISTS %s", common.Database)
	_, err = db.Exec(dropDatabaseSQL)
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

	ctTableTempte := "CREATE TABLE %s (`ts` DateTime(3) NOT NULL,`current` Float32 NOT NULL,`voltage` UInt8 NOT NULL,`phase` Float32 NOT NULL) ENGINE = MergeTree() ORDER BY ts;"

	f := func(tableName string) error {
		ctTableSQL := fmt.Sprintf(ctTableTempte, tableName)
		_, err = db.Exec(ctTableSQL)
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
		for z := 0; z < T1; z++ {
			tableName = common.Database + "." + dbConfig.TablePrefix + strconv.Itoa(z)
			if err = f(tableName); err != nil {
				return err
			}
		}
	} else {
		tableName = common.Database + "." + common.Table
		if err = f(tableName); err != nil {
			return err
		}
	}

	fmt.Printf("Initialize database and table completed.\n")

	return nil
}

func GetCKConn(dsn string) (error, *sql.DB) {
	//fmt.Printf("dsn=%s\n", dsn)
	conn, err := sql.Open("clickhouse", dsn)
	if err != nil {
		fmt.Printf("clickhouse connection failed:%v\n", err)
		return err, conn
	} else {
		fmt.Printf("TDengine connection created.\n") //Connection succeed
	}
	return nil, conn
}

func TruncateTables(ckDB driver.Conn, T1 int) error {
	var tableName, delSql string

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if mode == multi {
		for z := 0; z < T1; z++ {
			tableName = common.Database + "." + "d" + strconv.Itoa(z)
			delSql = fmt.Sprintf("truncate table %s", tableName)
			if err := ckDB.Exec(ctx, delSql); err != nil {
				fmt.Printf("truncate table %s fail:%v \n", tableName, err)
				return err
			}
		}
	} else {
		// 执行删除表的操作 DROP measurement d0;
		tableName = common.Database + "." + common.Table
		delSql = fmt.Sprintf("truncate table %s", tableName)
		if err := ckDB.Exec(ctx, delSql); err != nil {
			if err != nil {
				fmt.Printf("truncate table %s fail:%v \n", tableName, err)
			}
			return err
		}
	}
	return nil
}
