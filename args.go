package mysql_flashback

import (
	"flag"
	"fmt"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
)



var (
	host          string
	port          int64
	user          string
	password      string
	StartFile     string
	StartPosition int64
	StartTime    string
	StopFile     string
	StopPosition int64
	GtidRegexp   string
	StopTime     string
	Database     string
	onlyTables   string
	onlySqlType  string
	OnlyDML      bool
	FilterTx     bool
	OutputFile   string
	Rollback     bool
)

var (
	OnlyTablesList  []string
	OnlySqlTypeList []string
	MysqlURI        string
)

func initVar() {
	flag.StringVar(&host, "h", "127.0.0.1", "mysql host")
	flag.Int64Var(&port, "P", 3306, "mysql port")
	flag.StringVar(&user, "u", "root", "mysql user")
	flag.StringVar(&password, "p", "root", "mysql user password")
	flag.StringVar(&Database, "d", "", "database you want")
	flag.StringVar(&onlyTables, "t", "", "table you want")
	flag.StringVar(&StartFile, "start-file", "", "start binlog file, fomat: mysql-bin.000001")
	flag.Int64Var(&StartPosition, "start-pos", 0, "start position in binlog file")
	flag.StringVar(&StartTime, "start-time", "", "start time in binlog file, format: 2006-01-02 15:04:05")
	flag.StringVar(&StopFile, "stop-file", "", "stop binlog file")
	flag.Int64Var(&StopPosition, "stop-pos", 0, "stop position in binlog file")
	flag.StringVar(&GtidRegexp, "gtid-regexp", "", "gitd regexp")
	flag.StringVar(&StopTime, "stop-time", "", "stop time in binlog file")
	flag.StringVar(&onlySqlType, "only-sql-type", "INSERT,UPDATE,DELETE", "sql type you want")
	flag.BoolVar(&OnlyDML, "only-DML", true, "ignore ddl")
	flag.BoolVar(&FilterTx, "filter-tx", true, "filter transition")
	flag.StringVar(&OutputFile, "output", stdout, "output file")
	flag.BoolVar(&Rollback, "rollback", false, "rollback")
	flag.Parse()
}

func verifyBinlogFile(file string) bool {
	list := strings.Split(file, ".")
	if len(list) < 2 {
		return false
	}
	index := list[len(list)-1]
	_, err := strconv.Atoi(index)
	return err == nil
}

func verifyVar() {
	if len(StartFile) == 0 {
		log.Fatal("start file is empty")
	}
	if !verifyBinlogFile(StartFile) {
		log.Fatal("start file format is illegal")
	}
	if len(StopFile) != 0 && !verifyBinlogFile(StopFile) {
		log.Fatal("stop file format is illegal")
	}
	if StartPosition < 4 {
		StartPosition = 4
	}
	if len(OutputFile) == 0 {
		if !Rollback {
			OutputFile = "raw.sql"
		} else {
			OutputFile = "rollback.sql"
		}
	}
}

func splitVar(listVar string, f func(string) string) []string {
	var res []string
	if len(listVar) != 0 {
		for _, v := range strings.Split(listVar, ",") {
			v = strings.TrimSpace(v)
			if f != nil {
				v = f(v)
			}
			res = append(res, v)
		}
	}
	return res
}

func globalVar() {
	MysqlURI = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", user, password, host, port, Database)
	OnlyTablesList = splitVar(onlyTables, nil)
	OnlySqlTypeList = splitVar(onlySqlType, nil)
}

func init() {
	initVar()
	verifyVar()
	globalVar()
}
