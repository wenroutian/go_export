package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"io"
	"os"
	"strings"
	"time"
)

func main() {
	conf := parseConf()

	if _, ok := conf["db"]; !ok {
		panic("not get the mysql conf")
	}

	db := connectMysql(conf["db"])

	if _, ok := conf["sql"]; !ok {
		panic("not get the sql")
	}
	d := db.Raw(conf["sql"])

	f, err := os.OpenFile("_data.csv", os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		panic("open write_data_fail")
	}
	defer f.Close()
	writeUtf8(f)
	csv := csv.NewWriter(f)
	// todo write the head and the content
	err = DataCallBack(d, csv)
	if err != nil {
		panic(err)
	}
}

func DataCallBack(rep *gorm.DB, f *csv.Writer) error {
	res, err := rep.Rows()

	fmt.Println("common")
	if err != nil {
		return err
	}

	defer res.Close()

	columns, err := res.Columns()

	fmt.Println(columns)

	if err != nil {
		return err
	}
	f.Write(columns)

	values := make([]sql.RawBytes, len(columns))
	scans := make([]interface{}, len(columns))

	for i := range values {
		scans[i] = &values[i]
	}
	i := 0
	for res.Next() {
		_ = res.Scan(scans...)
		var each []string
		for _, col := range values {
			each = append(each, string(col))
		}
		if i%1000 == 0 {
			//	xlcw.Info("the task is process", each)
		}
		err := f.Write(each)
		if err != nil {
			panic("write data error")
		}
		if i == 10000 {
			f.Flush()
		}
		i++
	}
	f.Flush()

	return nil
}

func parseConf() map[string]string {
	f, err := os.Open(".env")
	if err != nil {
		panic("not found the .env conf file")
	}
	buf := bufio.NewReader(f)
	conf := make(map[string]string)
	for {
		line, _, err := buf.ReadLine()
		if err == io.EOF {
			break
		}
		content := string(line)
		keyValue := strings.SplitN(content, "=", 2)
		if len(keyValue) == 2 {
			conf[keyValue[0]] = keyValue[1]
		}
	}
	return conf
}

func connectMysql(dns string) *gorm.DB {
	db, err := gorm.Open(
		"mysql",
		dns,
	)
	if err != nil {
		panic("mysql connect error")
	}
	if os.Getenv("DB_DEBUG") == "true" {
		db.LogMode(true)
	}
	db.DB().SetMaxIdleConns(0)
	db.DB().SetConnMaxLifetime(10 * time.Second)
	return db
}

func writeUtf8(f *os.File) (int, error) {
	// 写入UTF-8 BOM，防止中文乱码
	return f.WriteString("\xEF\xBB\xBF")
}
