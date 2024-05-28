package common

import (
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"
)

func GetIntArgs(r, T, n, retry string) (err error, r1, T1, n1, retry1 int) {
	r1, err = strconv.Atoi(r)
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}

	T1, err = strconv.Atoi(T)
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}

	n1, err = strconv.Atoi(n)
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}

	retry1, err = strconv.Atoi(retry)
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}

	return
}

func CreateData(fileName string, r1 int) error {
	var startTimestamp int64 = 1500000000000
	layout := "2006-01-02 15:04:05.000"

	// 创建 csv 文件
	f, err := os.Create(fileName)
	if err != nil {
		fmt.Printf("create file fail, err:%v\n", err)
		return err
	}
	defer f.Close()

	// 创建 csv 写入器
	w := csv.NewWriter(f)

	for i := 0; i < r1; i++ {
		var record []string
		record = append(record, time.UnixMilli(startTimestamp).Format(layout))
		startTimestamp++
		sevenDigitRandomNumber := fmt.Sprintf("%.7f", rand.Float64())
		random1 := strconv.FormatInt(int64(rand.Intn(7)-3), 10)
		current := random1 + sevenDigitRandomNumber[1:]
		voltage := strconv.FormatInt(int64(rand.Intn(20)), 10)
		phase := fmt.Sprintf("%.7f", -rand.Float64())
		record = append(record, current)
		record = append(record, voltage)
		record = append(record, phase)
		err = w.Write(record)
		if err != nil {
			fmt.Printf("Write data to %s fail, err:%v\n", fileName, err)
			return err
		}
	}
	// 将缓冲写入CSV文件
	w.Flush()
	if err = w.Error(); err != nil {
		fmt.Printf("flush data fail, err:%v\n", err)
		return err
	}
	return nil
}
