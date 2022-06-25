package mysql_flashback

import (
	"flag"
	"fmt"
	log "github.com/sirupsen/logrus"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	layout = "2006-01-02 15:04:05"
)

const (
	stdout = "stdout"
)

var (
	host          string
	port          int64
	user          string
	password      string
	startFile     string
	startPosition int64
	startTime     string
	stopFile      string
	stopPosition  int64
	gtidRegexp    string
	stopTime      string
	database      string
	onlyTables    string
	onlySqlType   string
	onlyDML       bool
	outputFile    string
	noPrimaryKey  bool
	flashback     bool
)

var (
	binlogStartTime time.Time
	binlogStopTime  time.Time
	GTIDRegexp      *regexp.Regexp
	onlyTablesList  []string
	onlySqlTypeList []string
	MysqlURI        string
)

func initVar() {
	flag.StringVar(&host, "host", "127.0.0.1", "mysql host")
	flag.Int64Var(&port, "port", 3306, "mysql port")
	flag.StringVar(&user, "user", "root", "mysql user")
	flag.StringVar(&password, "password", "root", "mysql user password")
	flag.StringVar(&startFile, "startFile", "", "start binlog file, fomat: mysql-bin.000001")
	flag.Int64Var(&startPosition, "startPos", 0, "start position in binlog file")
	flag.StringVar(&startTime, "startTime", "", "start time in binlog file, format: 2006-01-02 15:04:05")
	flag.StringVar(&stopFile, "stopFile", "", "stop binlog file")
	flag.Int64Var(&stopPosition, "stopPos", 0, "stop position in binlog file")
	flag.StringVar(&gtidRegexp, "gtidRegexp", "", "gitd regexp")
	flag.StringVar(&stopTime, "stopTime", "2022-06-20 15:05:37", "stop time in binlog file")
	flag.StringVar(&database, "database", "", "database you want")
	flag.StringVar(&onlyTables, "onlyTables", "", "table you want")
	flag.StringVar(&onlySqlType, "onlySqlType", "INSERT,UPDATE,DELETE", "sql type you want")
	flag.BoolVar(&onlyDML, "onlyDML", true, "ignore ddl")
	flag.StringVar(&outputFile, "outputFile", stdout, "output file")
	flag.BoolVar(&noPrimaryKey, "noPrimaryKey", false, "no primary key")
	flag.BoolVar(&flashback, "flashback", false, "flashback")
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
	if len(startFile) == 0 {
		log.Fatal("start file is empty")
	}
	if !verifyBinlogFile(startFile) {
		log.Fatal("start file format is illegal")
	}
	if len(stopFile) != 0 && !verifyBinlogFile(stopFile) {
		log.Fatal("stop file format is illegal")
	}
	if startPosition < 4 {
		startPosition = 4
	}
	if len(outputFile) == 0 {
		if !flashback {
			outputFile = "raw.sql"
		} else {
			outputFile = "flashback.sql"
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
	MysqlURI = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", user, password, host, port, database)
	onlyTablesList = splitVar(onlyTables, nil)
	onlySqlTypeList = splitVar(onlySqlType, nil)

	var err error
	if len(startTime) != 0 {
		binlogStartTime, err = time.Parse(layout, startTime)
		if err != nil {
			log.Fatal("start time format is illegal")
		}
	}
	if len(stopTime) != 0 {
		binlogStopTime, err = time.Parse(layout, stopTime)
		if err != nil {
			log.Fatal("stop time format is illegal")
		}
	}
	if len(gtidRegexp) != 0 {
		GTIDRegexp = regexp.MustCompile(gtidRegexp)
	}
}

func init() {
	initVar()
	verifyVar()
	globalVar()
}
