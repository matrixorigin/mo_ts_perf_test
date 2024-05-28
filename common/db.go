package common

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"time"
)

func GetDbConn(T1 int, dsn string) (error, []*sql.DB) {
	var dbList []*sql.DB

	fmt.Printf("start create db conn, count:%d\n", T1)

	for i := 0; i < T1; i++ {
		db, err := sql.Open("mysql", dsn) // Set database connection
		if err != nil {
			fmt.Println("open database fail:", err)
			return err, dbList
		}
		err = db.Ping() //Connect to DB
		if err != nil {
			fmt.Printf("db connection[%d] failed:%v\n", i+1, err)
			return err, dbList
		} else {
			fmt.Printf("db connection[%d] created.\n", i+1)
			dbList = append(dbList, db)
		}
	}
	return nil, dbList
}

func TxExecSql(db *sql.Tx, sql1 string) error {
	_, err := db.Exec(sql1)
	if err != nil {
		return err
	}
	return nil
}

func ExecSql(db *sql.DB, sql1 string) error {
	_, err := db.Exec(sql1)
	if err != nil {
		return err
	}
	return nil
}

func ExecQuery(db *sql.DB) {
	rows, err := db.Query(fmt.Sprintf("select * from %s", Database+"."+Table))
	if err != nil {
		fmt.Println(err)
		return
	}
	var (
		ts      time.Time
		current float32
		voltage int
		phase   float32
	)

	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&ts, &current, &voltage, &phase)
		if err != nil {
			log.Fatalln("scan error:\n", err)
			return
		}
		//fmt.Println(ts, current, voltage, phase)
	}
}

func PointQuery(db *sql.DB, tsCondition string) {
	// "select * from %s where ts='2015-01-25 01:45:56'"
	sql1 := fmt.Sprintf("select * from %s where ts=%s", Database+"."+Table, tsCondition)
	fmt.Printf(" point query sql: %s\n", sql1)
	rows, err := db.Query(sql1)
	if err != nil {
		fmt.Println(err)
		return
	}

	var (
		ts      time.Time
		current float32
		voltage int
		phase   float32
	)

	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&ts, &current, &voltage, &phase)
		if err != nil {
			log.Fatalln("scan error:\n", err)
			return
		}
		fmt.Println(" point query result: <", ts, current, voltage, phase, ">")
	}
}

func QueryCount(db *sql.DB) (error, int) {
	var count int
	rows, err := db.Query(fmt.Sprintf("select count(*) from %s", Database+"."+Table))
	if err != nil {
		fmt.Println(err)
		return err, count
	}

	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&count)
		if err != nil {
			log.Fatalln("scan error:\n", err)
			return err, count
		}
		fmt.Printf("\n count value is:%d\n", count)
	}
	return nil, count
}

func QueryAvg(db *sql.DB) {
	rows, err := db.Query(fmt.Sprintf("select avg(`current`) from %s", Database+"."+Table))
	if err != nil {
		fmt.Println(err)
		return
	}

	defer rows.Close()
	for rows.Next() {
		var (
			avg float64
		)
		err = rows.Scan(&avg)
		if err != nil {
			log.Fatalln("scan error:\n", err)
			return
		}
		fmt.Println(" avg value is:", avg)
	}
}

func QuerySum(db *sql.DB) {
	rows, err := db.Query(fmt.Sprintf("select sum(`current`) from %s", Database+"."+Table))
	if err != nil {
		fmt.Println(err)
		return
	}

	defer rows.Close()
	for rows.Next() {
		var (
			sum float64
		)
		err = rows.Scan(&sum)
		if err != nil {
			log.Fatalln("scan error:\n", err)
			return
		}
		fmt.Println(" sum value is:", sum)
	}
}

func QueryMax(db *sql.DB) {
	rows, err := db.Query(fmt.Sprintf("select max(`current`) from %s", Database+"."+Table))
	if err != nil {
		fmt.Println(err)
		return
	}

	defer rows.Close()
	for rows.Next() {
		var (
			max1 float64
		)
		err = rows.Scan(&max1)
		if err != nil {
			log.Fatalln("scan error:\n", err)
			return
		}
		fmt.Println(" max value is:", max1)
	}
}

func QueryMin(db *sql.DB) {
	rows, err := db.Query(fmt.Sprintf("select min(`current`) from %s", Database+"."+Table))
	if err != nil {
		fmt.Println(err)
		return
	}

	defer rows.Close()
	for rows.Next() {
		var (
			min1 float64
		)
		err = rows.Scan(&min1)
		if err != nil {
			log.Fatalln("scan error:\n", err)
			return
		}
		fmt.Println(" min value is:", min1)
	}
}
