package main

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"math/rand"
	"os"
	"os/exec"
	"performance_testing/common"
	"sync"
	"time"
)

var r string
var T string
var n string

func init() {
	firstArgWithDash := 1

	for i := 1; i < len(os.Args); i++ {
		firstArgWithDash = i
		if len(os.Args[i]) > 0 && os.Args[i][0] == '-' {
			break
		}
	}
	flag.StringVar(&r, "r", "100000", "The number of records per request. By default is 10000000.")
	flag.StringVar(&T, "T", "1", " The number of threads. By default use 1")
	flag.StringVar(&n, "n", "100000", "Number of records for each table, default is 10000000")
	flag.CommandLine.Parse(os.Args[firstArgWithDash:])
}

func main() {
	err, r1, T1, n1, _ := common.GetIntArgs(r, T, n, "0")
	if err != nil {
		return
	}
	fmt.Printf("r=%d, T=%d, n=%d \n", r1, T1, n1)

	// 输入的 r 值是多少，就导入{r}.csv文件, 要求n必须是r值的的整倍数
	filePath := fmt.Sprintf("../data/%d.csv", r1)
	if err = preCheck(filePath, r1, n1); err != nil {
		return
	}

	srConfig, err := common.ReadSRFile("../conf/db.conf")
	url := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?loc=UTC&parseTime=true", srConfig.User, srConfig.Password, srConfig.Host, srConfig.JdbcPort, "")
	fmt.Printf("url:%s\n", url)

	// 获取 T 个数据库连接
	err, dbList := common.GetDbConn(1, url)
	if err != nil {
		fmt.Printf("get dbconn fail:%v\n", err)
		return
	}

	// 导入数据前先初始化表，创建数据库、表，已创建的话，先清空表数据
	if err = InitTable(srConfig, dbList[0]); err != nil {
		return
	}

	for _, conn := range dbList {
		conn.Close()
	}

	logFile, err := os.OpenFile("log.txt", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0755)
	if err != nil {
		fmt.Printf("open file err:%v", err)
		return
	}

	var wg sync.WaitGroup
	rand.Seed(time.Now().Unix())
	f1 := func(wg1 *sync.WaitGroup, T1 int) {
		defer wg1.Done()

		for i := 0; i < (n1 / r1); i++ {
			command := fmt.Sprintf("curl --location-trusted -u %s:%s -H \"column_separator:,\" -T %s http://%s:%s/api/%s/%s/_stream_load",
				srConfig.User, srConfig.Password, filePath, srConfig.Host, srConfig.HttpPort, srConfig.Database, srConfig.Table)
			//fmt.Printf("client(thread)%d: %s\n", T1, command)
			cmd := exec.Command("bash", "-c", command)
			cmd.Stdout = logFile
			cmd.Stderr = logFile
			err = cmd.Run()
			if err != nil {
				fmt.Printf("run err:%v\n", err)
				return
			}
		}

	}

	// 开始执行
	startTime := time.Now()
	// 开启T1个协程模拟客户端，并行执行写入操作
	for j := 0; j < T1; j++ {
		wg.Add(1)
		go f1(&wg, T1)
		fmt.Printf("client(thread)%d started executing insert table ……\n", j+1)
	}

	wg.Wait()
	spendT := time.Since(startTime).Seconds()
	fmt.Printf("spend time:%f s\n", spendT)

	count := n1 * T1
	records := float64(count) / spendT
	fmt.Printf("%d/%f = %f records/second\n", count, spendT, records)
}

func preCheck(filePath string, r1, n1 int) error {
	var err error
	// 检查{r}.csv文件是否存在
	if _, err = os.Stat(filePath); err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("data file '%s' does not exist, start create data …… .\n", filePath)
			if err = common.CreateData(filePath, r1); err != nil {
				return err
			}
			fmt.Printf("data file '%s' created completed.\n", filePath)
		} else {
			fmt.Printf("search file %s fail, err:%v.\n", filePath, err)
			return err
		}
	} else {
		fmt.Printf("data file '%s' exist, skip create.\n", filePath)
	}

	// 要求n必须是r值的的整倍数
	a := n1 / r1
	b := n1 % r1
	if a <= 0 || b != 0 {
		err = errors.New("输入错误参数: 要求n必须是r的整倍数")
		fmt.Println(err)
		return err
	}
	return nil
}

func InitTable(srConfig *common.SRConfig, db *sql.DB) error {
	database := srConfig.Database
	table := srConfig.Table
	tableName := database + "." + table
	var err error
	ctDatabaseSQL := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", database)
	_, err = db.Exec(ctDatabaseSQL)
	if err != nil {
		fmt.Printf("create database  %s fail:%v \n", database, err)
		return err
	}

	ctTableSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s(ts DATETIME not null,`current` FLOAT not null,voltage int not null, phase FLOAT not null) DISTRIBUTED BY HASH(`ts`) BUCKETS 1 PROPERTIES ( \"replication_num\" = \"1\");", tableName)
	_, err = db.Exec(ctTableSQL)
	if err != nil {
		fmt.Printf("create table  %s fail:%v \n", tableName, err)
		return err
	}

	delSql := fmt.Sprintf("truncate table %s", tableName)
	//fmt.Printf("delSql:%s\n", delSql)
	_, err = db.Exec(delSql)
	if err != nil {
		fmt.Printf("truncate table %s fail:%v \n", tableName, err)
		return err
	}

	fmt.Printf("Initialize data table completed.\n")
	return nil
}
