package main

import (
	"github.com/juju/errors"
	"github.com/obgnail/mysql-flashback"
	"log"
)

func main() {
	mysqlUri := mysql_flashback.MysqlURI
	startLog := mysql_flashback.StartFile
	startPos := mysql_flashback.StartPosition
	startTime := mysql_flashback.StartTime
	stopLog := mysql_flashback.StopFile
	stopPos := mysql_flashback.StopPosition
	stopTime := mysql_flashback.StopTime
	gtidRegexp := mysql_flashback.GtidRegexp
	database := mysql_flashback.Database
	onlyTable := mysql_flashback.OnlyTablesList
	onlySqlType := mysql_flashback.OnlySqlTypeList
	onlyDML := mysql_flashback.OnlyDML
	filterTx := mysql_flashback.FilterTx
	output := mysql_flashback.OutputFile
	flashback := mysql_flashback.Rollback

	fb := mysql_flashback.NewFlashback(
		mysqlUri, startLog, uint32(startPos), startTime,
		stopLog, uint32(stopPos), stopTime, gtidRegexp,
		database, onlyTable, onlySqlType, onlyDML,
		filterTx, output, flashback,
	)
	err := fb.Flashback(mysqlUri, startLog, uint32(startPos))
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}
}
