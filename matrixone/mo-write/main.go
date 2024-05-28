package main

import (
	"bytes"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"math/rand"
	"net/url"
	"os"
	"performance_testing/common"
	"strconv"
	"strings"
	"sync"
	"time"
)

var T, r, n, retry, mode, txc, tType, wType string
var confirm string
var dbConfig *common.DBConfig

const (
	database = "test"
	table    = "d0"
	intPK    = "intPK"
	tsPK     = "tsPK"
	ts       = "ts"
	multi    = "multi"
	single   = "single"
	loadLine = "loadLine"
	loadFile = "loadFile"
	insert   = "insert"
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
	flag.StringVar(&txc, "txc", "0", "The number of writes committed per transaction. 0 means not opening transactions. default 0.")
	flag.StringVar(&tType, "tType", "ts", "default ts, ts|tsPK|intPK, ts: time series table without primary key.")
	flag.StringVar(&wType, "wType", "loadLine", "insert|loadLine|loadFile, default loadLine, insert: write data by 'insert into values', loadLine: write data through 'load data INLINE', loadFile: write data through 'load data INFILE'.")
	flag.CommandLine.Parse(os.Args[firstArgWithDash:])
}

func main() {
	// 转化公共参数
	err, r1, T1, n1, retry1 := common.GetIntArgs(r, T, n, retry)
	if err != nil {
		return
	}
	err, txc1 := GetMoIntArgs()
	if err != nil {
		return
	}
	fmt.Printf("r=%d, T=%d, n=%d, mode=%s, retry=%d, txc=%d, tType=%s, wType=%s \n", r1, T1, n1, mode, retry1, txc1, tType, wType)

	dbConfig, err = common.ReadDBFile("../conf/db.conf", common.MO)
	fmt.Printf("dbConfig:%v\n", *dbConfig)

	// 校验输入参数
	if err = CheckMoArgs(r1, n1); err != nil {
		return
	}

	encodedUsername := url.QueryEscape(dbConfig.User)
	dsn := encodedUsername + ":" + dbConfig.Password + "@tcp(" + dbConfig.Host + ":" + dbConfig.Port + ")/"
	// 获取 T 个数据库连接
	err, dbList := common.GetDbConn(T1, dsn)
	if err != nil {
		fmt.Printf("get dbconn fail:%v\n", err)
		return
	}

	defer func() {
		for _, conn := range dbList {
			conn.Close()
		}
	}()
	fmt.Printf("mo-data of all clinet(%d thread) has ready!\n", T1)

	// 初始化数据库表，导入数据前先删除、新建test数据库，再创建表d0、d1、d2……
	if err = InitTable(dbList[0], T1); err != nil {
		return
	}

	fmt.Printf("start preparing test data.\n")
	getDataT := time.Now()
	var dataList [][]string
	switch wType {
	case loadFile:
		err, dataList = GetLoadFileData(T1, n1, r1)
		if err != nil {
			return
		}
	case loadLine, insert:
		err, dataList = GetData(T1, n1, r1)
		if err != nil {
			return
		}
	default:
		fmt.Printf("unrecognized wType value:%s\n", wType)
		return
	}

	dataSpendT := time.Since(getDataT).Seconds()
	fmt.Printf("spend time of prepare testing data:%f s\n", dataSpendT)

	var sumRecord float64
	// 每个测试测 retry 轮，求平均值
	for k := 0; k < retry1; k++ {
		fmt.Printf("按 Y 或者 回车键,将开始插入数据,按 N 将退出, 开的第%d次测试, txc=%d \n", k+1, txc1)

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

		var wg sync.WaitGroup
		f1 := func(db *sql.DB, sqlData []string, wg1 *sync.WaitGroup) {
			defer wg1.Done()
			counter := 1

			var tx *sql.Tx
			for i := 0; i < len(sqlData); i++ {
				// 当txc值大于0时，则开启事务提交写入，否则不使用事务
				if txc1 > 0 {
					if counter == 1 {
						tx, err = db.Begin()
						if err != nil {
							fmt.Printf("begin 'insert into' tx err: %v \n", err)
							return
						}
					}
					// 执行事务写入
					if err = common.TxExecSql(tx, sqlData[i]); err != nil {
						fmt.Println(err)
						return
					}
					// 累计写入txc次后，提交事务
					if counter == txc1 || i == len(sqlData)-1 {
						tx.Commit()
						counter = 0
					}
					counter++
				} else {
					// 否则普通写入
					if err = common.ExecSql(db, sqlData[i]); err != nil {
						fmt.Println(err)
						return
					}
				}
			}
		}

		if txc1 > 0 {
			fmt.Printf("开始事务提交写入, 一次事务提交的写入: %d\n", txc1)
		}

		// 开始执行
		startTime := time.Now()
		// 开启T1个协程模拟客户端，并行执行写入操作
		for j := 0; j < T1; j++ {
			wg.Add(1)
			go f1(dbList[j], dataList[j], &wg)
			//fmt.Printf("clint(thread)%d started executing insert into ……\n", j+1)
		}

		wg.Wait()
		spendT := time.Since(startTime).Seconds()
		fmt.Printf("spend time:%f s\n", spendT)

		count := n1 * T1
		records := float64(count) / spendT
		fmt.Printf("%d test: %d/%f = %f records/second\n", k+1, count, spendT, records)
		sumRecord += records
	}
	recordsLast := sumRecord / float64(retry1)
	fmt.Printf("======== avg test: %f/%d = %f records/second txc=%d ===========\n", sumRecord, retry1, recordsLast, txc1)

}

func GetData(T1, n1, r1 int) (error, [][]string) {
	var data [][]string
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
		var tData []string
		dataSize := r1

		// 根据写入模式，multi为多表写入，表名动态生成，第一个客户端向d0表写，第二个客户端向d1表写……以此类推
		// single为单表写入模式。不管几个客户端，都向test.d0表写数据
		if mode == multi {
			tableName = database + "." + dbConfig.TablePrefix + strconv.Itoa(z)
		} else {
			tableName = database + "." + table
		}

		var sqlPrefix string // 写入sql前缀
		// 定义 insert、loadLine 两种不同的写数据sql语句前缀
		if wType == insert {
			sqlPrefix = fmt.Sprintf("INSERT INTO %s VALUES", tableName)
		} else {
			sqlPrefix = "load data inline format='csv',data=$XXX$"
		}

		for j := 0; j < subNum; j++ {
			if j == subNum-1 && rem > 0 {
				dataSize = rem
			}

			//var buffer strings.Builder
			var buffer bytes.Buffer
			buffer.WriteString(sqlPrefix)

			for i := 0; i < dataSize; i++ {
				// 当表为主键为int类型的普通表时，ts值取时间戳
				var tsValue string
				if tType == intPK {
					tsValue = strconv.FormatInt(startTimestamp, 10)
				} else {
					if wType == insert {
						tsValue = "'" + time.UnixMilli(startTimestamp).Format(layout) + "'"
					} else {
						tsValue = time.UnixMilli(startTimestamp).Format(layout)
					}
				}
				startTimestamp++
				sevenDigitRandomNumber := fmt.Sprintf("%.7f", rand.Float64())
				random1 := strconv.FormatInt(int64(rand.Intn(7)-3), 10)
				current := random1 + sevenDigitRandomNumber[1:]
				voltage := rand.Intn(20)
				phase := fmt.Sprintf("%.7f", -rand.Float64())

				// 拼接 insert、loadLine 两种不同的写数据sql
				if wType == insert {
					if i == 0 {
						buffer.WriteString(fmt.Sprintf("(%s, %s, %d, %s)", tsValue, current, voltage, phase))
					} else {
						buffer.WriteString(fmt.Sprintf(",(%s, %s, %d, %s)", tsValue, current, voltage, phase))
					}
				} else {
					if i == 0 {
						buffer.WriteString(fmt.Sprintf("%s, %s, %d,%s", tsValue, current, voltage, phase))
					} else {
						buffer.WriteString(fmt.Sprintf(" \n %s, %s, %d,%s", tsValue, current, voltage, phase))
					}
				}

			}

			if wType == loadLine {
				sqlSuffix := fmt.Sprintf(" $XXX$ into table %s;", tableName)
				buffer.WriteString(sqlSuffix)
			}
			tData = append(tData, buffer.String())
		}
		data = append(data, tData)
	}

	return nil, data
}

func GetLoadFileData(T1, n1, r1 int) (error, [][]string) {
	subNum := n1 / r1
	//fmt.Printf("subNum=%d, 开始准备数据:\n", subNum)

	var data [][]string
	for z := 0; z < T1; z++ {
		var tData []string
		var tableName string
		// 根据写入模式，multi为多表写入，表名动态生成，第一个客户端向d0表写，第二个客户端向d1表写……以此类推
		// single为单表写入模式。不管几个客户端，都向test.d0表写数据
		if mode == multi {
			tableName = database + "." + dbConfig.TablePrefix + strconv.Itoa(z)
		} else {
			tableName = database + "." + table
		}
		for j := 0; j < subNum; j++ {
			// LOAD DATA INFILE 'yourfilepath' INTO TABLE xx CHARACTER SET utf8;
			sql := fmt.Sprintf("LOAD DATA INFILE '%s%d.csv' INTO TABLE %s;", dbConfig.LoadFilePath, r1, tableName)
			tData = append(tData, sql)
		}
		data = append(data, tData)
	}

	return nil, data
}

func GetMoIntArgs() (err error, txc1 int) {
	txc1, err = strconv.Atoi(txc)
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}
	return
}

func CheckMoArgs(r1, n1 int) error {
	var err error
	// 校验mode值
	if mode != multi && mode != single {
		err = errors.New(fmt.Sprintf("unrecognized mode value:%s, required to be either multi or single, default multi", mode))
		fmt.Printf("%v\n", err)
		return err
	}
	// 校验 type值：ts|tsPK|intPK
	if tType != ts && tType != tsPK && tType != intPK {
		err = errors.New(fmt.Sprintf("unrecognized tType value:%s, required to be ts|tsPK|intPK, default ts", tType))
		fmt.Printf("%v\n", err)
		return err
	}

	// 校验 wType ：insert|loadLine|loadFile
	if wType != insert && wType != loadLine && wType != loadFile {
		err = errors.New(fmt.Sprintf("unrecognized wType value:%s, required to be insert|loadLine|loadFile, default loadLine", wType))
		fmt.Printf("%v\n", err)
		return err
	}

	if wType == loadFile {
		if dbConfig.LoadFilePath == "" {
			err = errors.New(fmt.Sprintf("loadFilePath cannot be empty, when wType is loadFile"))
			fmt.Printf("%v\n", err)
			return err
		} else {
			// 结尾加上/
			dbConfig.LoadFilePath = strings.TrimSuffix(dbConfig.LoadFilePath, "/") + "/"
			// 要求n必须是r值的的整倍数
			a := n1 / r1
			b := n1 % r1
			if a <= 0 || b != 0 {
				err = errors.New("loadFile 写入模式下, 要求n必须是r的整倍数")
				fmt.Println(err)
				return err
			}
		}
	}

	return nil
}

func InitTable(db *sql.DB, T1 int) error {
	var err error
	dropDatabaseSQL := fmt.Sprintf("DROP DATABASE IF EXISTS %s", database)
	_, err = db.Exec(dropDatabaseSQL)
	if err != nil {
		fmt.Printf("drop database  %s fail:%v \n", database, err)
		return err
	}

	ctDatabaseSQL := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", database)
	_, err = db.Exec(ctDatabaseSQL)
	if err != nil {
		fmt.Printf("create database  %s fail:%v \n", database, err)
		return err
	}

	var ctTableTempte string
	// ts：表示无主键时序表，tsPK：表示有主键时序表，intPK：表示主键为int类型的普通表
	switch tType {
	case ts:
		ctTableTempte = "create table if not exists %s (ts TIMESTAMP(3) not null, current FLOAT not null, voltage int not null, phase FLOAT not null);"
	case tsPK:
		ctTableTempte = "create table if not exists %s (ts TIMESTAMP(3) not null, current FLOAT not null, voltage int not null, phase FLOAT not null, PRIMARY KEY (ts));"
	case intPK:
		ctTableTempte = "create table if not exists %s (ts bigint not null, current FLOAT not null, voltage int not null, phase FLOAT not null, PRIMARY KEY (ts));"
	default:
		err = errors.New(fmt.Sprintf("invalid tType value:%s, required to be ts|tsPK|intPK", tType))
		fmt.Printf("%v\n", err)
		return err
	}

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
	if mode == "multi" {
		for z := 0; z < T1; z++ {
			tableName = database + "." + dbConfig.TablePrefix + strconv.Itoa(z)
			if err = f(tableName); err != nil {
				return err
			}
		}
	} else {
		tableName = database + "." + table
		if err = f(tableName); err != nil {
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
			tableName = database + "." + dbConfig.TablePrefix + strconv.Itoa(z)
			delSql = fmt.Sprintf("truncate table %s", tableName)
			if err := common.ExecSql(db, delSql); err != nil {
				fmt.Printf("truncate table %s fail:%v \n", tableName, err)
				return err
			}
		}
	} else {
		// 执行删除表的操作 DROP measurement d0;
		tableName = database + "." + table
		delSql = fmt.Sprintf("truncate table %s", tableName)
		if err := common.ExecSql(db, delSql); err != nil {
			if err != nil {
				fmt.Printf("truncate table %s fail:%v \n", tableName, err)
			}
			return err
		}
	}
	return nil
}
