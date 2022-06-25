package main

import (
	"github.com/juju/errors"
	"github.com/obgnail/mysql-flashback"
	"log"
)

func main() {
	mysqlUri := "root:root@tcp(127.0.0.1:3306)/es_river"
	startLog := "/Users/heyingliang/volume/mysql/data/mysql-bin.000023"
	startPos := 0
	startTime := ""
	stopLog := ""
	stopPos := 0
	stopTime := ""
	gtidRegexp := ""
	database := "es_river"
	onlyTable := []string{}
	onlySqlType := []string{"INSERT", "UPDATE", "DELETE"}
	filterTx := false
	onlyDML := false
	output := "./output.sql"
	//output := "stdout"
	onPrimaryKey := false
	flashback := true

	fb := mysql_flashback.NewFlashback(
		mysqlUri, startLog, uint32(startPos), startTime,
		stopLog, uint32(stopPos), stopTime, gtidRegexp,
		database, onlyTable, onlySqlType, onlyDML,
		filterTx, output, onPrimaryKey, flashback,
	)
	err := fb.Flashback(mysqlUri, startLog, uint32(startPos))
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}
}
