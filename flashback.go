package mysql_flashback

import (
	"bytes"
	"fmt"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/juju/errors"
	"io"
	"os"
	"reflect"
	"regexp"
	"strings"
	"time"
)

const (
	SqlRowFormat    = "%s /* ROW -> binlog: %s | pos: (%d, %d) | time: %s */"
	SqlDDLFormat    = "%s /* DDL -> binlog: %s | pos: (%d, %d) | time: %s */\n"
	SqlGtidFormat   = "/* GTID -> %s | binlog: %s | pos: (%d, %d) | time: %s */"
	SqlBeginFormat  = "/* BEGIN -> %s | binlog: %s | pos: (%d, %d) | time: %s */"
	SqlCommitFormat = "/* COMMIT -> %s | binlog: %s | pos: (%d, %d) | time: %s */\n"

	SqlInsertFormat = "INSERT INTO `%s`.`%s`(%s) VALUES (%s);"
	SqlUpdateFormat = "UPDATE `%s`.`%s` SET %s WHERE %s LIMIT 1;"
	SqlDeleteFormat = "DELETE FROM `%s`.`%s` WHERE %s LIMIT 1;"
)

const (
	stdout = "stdout"
	layout = "2006-01-02 15:04:05"
)

type Flashback struct {
	mysqlUri string

	// filter binlog args
	startFile string
	startPos  uint32
	startTime uint32
	stopFile  string
	stopPos   uint32
	stopTime  uint32

	// filter event args
	database    string
	gtidRegexp  *regexp.Regexp
	onlyTables  map[string]struct{}                // map[tableName]struct{}
	onlySqlType map[replication.EventType]struct{} // INSERT, UPDATE, DELETE
	filterTx    bool                               // filter Transaction event
	onlyDML     bool                               // ignore ddl

	// output args
	outputFile string
	flashback  bool

	// assist field
	allLogs           map[string]int        // map[filePath]index
	gtidEventType     replication.EventType // QUERY when gtid mode on, otherwise ANONYMOUS_GTID
	gtidEventStartPos uint32                // fix CUD event's start pos
	outputChan        chan string
	exitChan          chan struct{}
}

func NewFlashback(
	mysqlUri string, startFile string, startPos uint32, startTime string,
	stopFile string, stopPos uint32, stopTime string, gtidRegexp string,
	database string, onlyTables []string, onlySqlType []string, onlyDML bool,
	filterTx bool, outputFile string, flashback bool,
) *Flashback {
	if startFile == "" {
		panic("err: start File must exist")
	}
	if stopFile == "" && stopPos != 0 {
		panic("err: stopFile is empty but stopPos not")
	}

	var startT, stopT uint32
	if startTime != "" {
		start, err := time.ParseInLocation(layout, startTime, time.Local)
		if err != nil {
			panic("start time format is illegal")
		}
		startT = uint32(start.Unix())
	}
	if stopTime != "" {
		stop, err := time.ParseInLocation(layout, stopTime, time.Local)
		if err != nil {
			panic("start time format is illegal")
		}
		stopT = uint32(stop.Unix())
	}

	var GTIDRegexp *regexp.Regexp
	if len(gtidRegexp) != 0 {
		GTIDRegexp = regexp.MustCompile(gtidRegexp)
	}

	dbm, err := LinkDB(mysqlUri)
	if err != nil {
		panic(err)
	}
	logs, err := filterBinlog(dbm, startFile, 0)
	if err != nil {
		panic(err)
	}
	allLogs := make(map[string]int, len(logs))
	for idx, log := range logs {
		allLogs[log.path] = idx
	}

	tables := make(map[string]struct{}, len(onlyTables))
	for _, table := range onlyTables {
		tables[table] = struct{}{}
	}

	if len(onlySqlType) == 0 {
		onlySqlType = []string{"INSERT", "UPDATE", "DELETE"}
	}
	types := make(map[replication.EventType]struct{}, len(onlySqlType))
	for _, t := range onlySqlType {
		switch strings.ToUpper(t) {
		case "INSERT":
			types[replication.WRITE_ROWS_EVENTv1] = struct{}{}
			types[replication.WRITE_ROWS_EVENTv2] = struct{}{}
		case "UPDATE":
			types[replication.UPDATE_ROWS_EVENTv1] = struct{}{}
			types[replication.UPDATE_ROWS_EVENTv2] = struct{}{}
		case "DELETE":
			types[replication.DELETE_ROWS_EVENTv1] = struct{}{}
			types[replication.DELETE_ROWS_EVENTv2] = struct{}{}
		}
	}

	gitdModeOn, err := getGitdModeFromDb(dbm.db)
	if err != nil {
		panic(err)
	}
	gitdEventType := replication.ANONYMOUS_GTID_EVENT
	if gitdModeOn {
		gitdEventType = replication.QUERY_EVENT
	}

	if outputFile == "" {
		outputFile = stdout
	}

	// 使用flashback功能将自动修改以下属性
	if flashback {
		filterTx = true
		onlyDML = true
	}

	fb := &Flashback{
		mysqlUri:      mysqlUri,
		startFile:     startFile,
		startPos:      startPos,
		startTime:     startT,
		stopFile:      stopFile,
		stopPos:       stopPos,
		stopTime:      stopT,
		gtidRegexp:    GTIDRegexp,
		database:      database,
		onlyTables:    tables,
		onlySqlType:   types,
		filterTx:      filterTx,
		onlyDML:       onlyDML,
		outputFile:    outputFile,
		flashback:     flashback,
		allLogs:       allLogs,
		gtidEventType: gitdEventType,
		outputChan:    make(chan string, 2<<10),
		exitChan:      make(chan struct{}, 1),
	}
	go fb.output()
	return fb
}

func (fb *Flashback) Flashback(mysqlUri string, binlog string, position uint32) error {
	err := BinlogStream(mysqlUri, binlog, position, fb.flashbackFunc)
	close(fb.outputChan)
	<-fb.exitChan
	return errors.Trace(err)
}

// err: nil/StopErr
// 当e被过滤, return nil, nil
// 当e没被过滤, return e, nil
// 当中止解析时, return nil, StopErr
func (fb *Flashback) filterEvent(binlog *BinlogInfo, e *replication.BinlogEvent) (event *replication.BinlogEvent, stopErr error) {
	if fb.startTime != 0 && e.Header.Timestamp < fb.startTime {
		return
	}
	if fb.stopTime != 0 && e.Header.Timestamp > fb.stopTime {
		return nil, StopError
	}

	// 只解析一个文件的情况
	if fb.startFile == fb.stopFile && fb.startPos != 0 && e.Header.LogPos < fb.startPos {
		return
	}

	if fb.stopFile != "" {
		// 解析到结束位置的情况
		if fb.stopPos != 0 && binlog.path == fb.stopFile && e.Header.LogPos > fb.stopPos {
			return nil, StopError
		}

		// 解析到结束文件的情况
		if logIdx, ok := fb.allLogs[binlog.path]; !ok || logIdx > fb.allLogs[fb.stopFile] {
			return nil, StopError
		}
	}

	if fb.gtidRegexp != nil && e.Header.EventType == replication.GTID_EVENT {
		gtidEvent := e.Event.(*replication.GTIDEvent)
		if ok := fb.gtidRegexp.Match(gtidEvent.SID); !ok {
			return
		}
	}

	switch e.Header.EventType {
	case replication.QUERY_EVENT:
		queryEvent := e.Event.(*replication.QueryEvent)
		if ok := string(queryEvent.Schema) == fb.database; !ok {
			return
		}
		// len(queryEvent.Query) != 5: 优化一点性能
		if fb.onlyDML && len(queryEvent.Query) != 5 && string(queryEvent.Query) != "BEGIN" {
			return
		}
	case replication.TABLE_MAP_EVENT:
		tableMapEvent := e.Event.(*replication.TableMapEvent)
		schema := string(tableMapEvent.Schema)
		table := string(tableMapEvent.Table)
		if ok := schema == fb.database; !ok {
			return
		}
		if len(fb.onlyTables) != 0 {
			if _, ok := fb.onlyTables[table]; !ok {
				return
			}
		}

	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2,
		replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2,
		replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		if _, ok := fb.onlySqlType[e.Header.EventType]; !ok {
			return
		}
		rowsEvent := e.Event.(*replication.RowsEvent)
		schema := string(rowsEvent.Table.Schema)
		if ok := schema == fb.database; !ok {
			return
		}
	}
	return e, nil
}

func (fb *Flashback) prepare(dbm *DBMap, e *replication.BinlogEvent) error {
	switch e.Header.EventType {
	// binlog本身不包含table field 数据,因此需要去数据库里拿
	case replication.TABLE_MAP_EVENT:
		tableMapEvent := e.Event.(*replication.TableMapEvent)
		tableId := tableMapEvent.TableID
		schema := string(tableMapEvent.Schema)
		table := string(tableMapEvent.Table)
		if err := dbm.Add(tableId, schema, table); err != nil {
			return errors.Trace(err)
		}
	// CUD操作是放在事务里的,因此这些event的start pos应该为:
	//   - 若没开启gitd, 为anonymousGitdEvent的值
	//   - 若开启gitd, 为QueryEvent的值
	case fb.gtidEventType:
		fb.gtidEventStartPos = e.Header.LogPos - e.Header.EventSize
	}
	return nil
}

func (fb *Flashback) flashbackFunc(dbm *DBMap, binlog *BinlogInfo, event *replication.BinlogEvent) (err error) {
	if err = fb.prepare(dbm, event); err != nil {
		return errors.Trace(err)
	}
	event, err = fb.filterEvent(binlog, event)
	if err != nil {
		return StopError
	}
	if event == nil {
		return nil // 已经被过滤
	}
	if err = fb.outputSql(dbm, binlog, event); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (fb *Flashback) outputSql(dbm *DBMap, binlog *BinlogInfo, e *replication.BinlogEvent) (err error) {
	var content string
	var outputFormat string
	startPos := e.Header.LogPos - e.Header.EventSize

	switch e.Header.EventType {
	case replication.QUERY_EVENT:
		queryEvent := e.Event.(*replication.QueryEvent)
		query := string(queryEvent.Query)
		if query == "BEGIN" {
			if !fb.filterTx {
				outputFormat = SqlBeginFormat
				content = "Transaction BEGIN"
			}
		} else {
			outputFormat = SqlDDLFormat
			content = query
		}

	case replication.ANONYMOUS_GTID_EVENT:
		if !fb.filterTx {
			outputFormat = SqlGtidFormat
			content = "Transaction Group"
		}

	case replication.XID_EVENT:
		if !fb.filterTx {
			outputFormat = SqlCommitFormat
			xidEvent := e.Event.(*replication.XIDEvent)
			xId := xidEvent.XID
			content = fmt.Sprintf("Transaction COMMIT | xid: %d", xId)
		}

	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2,
		replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2,
		replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:

		outputFormat = SqlRowFormat
		startPos = fb.gtidEventStartPos

		rowsEvent := e.Event.(*replication.RowsEvent)
		tableId := rowsEvent.TableID
		tableMetadata, ok := dbm.LookupTableMetadata(tableId)
		if !ok {
			return fmt.Errorf("search table error: %s:%d", rowsEvent.Table.Schema, tableId)
		}

		switch e.Header.EventType {
		case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
			if fb.flashback {
				content = genDeleteSql(tableMetadata, rowsEvent)
			} else {
				content = genInsertSql(tableMetadata, rowsEvent)
			}

		case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
			if fb.flashback {
				content = genUpdateSql(tableMetadata, rowsEvent, true)
			} else {
				content = genUpdateSql(tableMetadata, rowsEvent, false)
			}

		case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
			if fb.flashback {
				content = genInsertSql(tableMetadata, rowsEvent)
			} else {
				content = genDeleteSql(tableMetadata, rowsEvent)
			}
		}
	default:
		return nil
	}

	if content == "" || outputFormat == "" {
		return nil
	}

	output := fmt.Sprintf(
		outputFormat,
		content,
		binlog.name,
		startPos,
		e.Header.LogPos,
		time.Unix(int64(e.Header.Timestamp), 0).Format(layout),
	)
	fb.outputChan <- output
	return nil
}

func (fb *Flashback) output() {
	var writer io.Writer
	var file *os.File

	if fb.outputFile == stdout {
		writer = os.Stdout

		// 因为要倒序生成,所以必须先输出到文件中
		if fb.flashback {
			fileName := fmt.Sprintf("rollback_%d.sql", time.Now().Unix())
			file = MustOpen(fileName, os.O_TRUNC|os.O_CREATE|os.O_RDWR)
			writer = io.MultiWriter(writer, file)
			fb.outputFile = fileName
		}
	} else {
		file = MustOpen(fb.outputFile, os.O_TRUNC|os.O_CREATE|os.O_RDWR)
		defer file.Close()
		writer = file
	}

	for output := range fb.outputChan {
		if _, err := fmt.Fprintln(writer, output); err != nil {
			panic(err)
		}
	}
	if file != nil {
		file.Close()
	}
	if fb.flashback {
		reverseFile(fb.outputFile)
	}

	fb.exitChan <- struct{}{}
}

func buildSqlFieldValue(value interface{}) string {
	if value == nil {
		return "NULL"
	}

	fieldType := reflect.TypeOf(value)
	switch fieldType.Kind() {
	case reflect.String:
		return fmt.Sprintf("'%v'", value)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return fmt.Sprintf("%v", value)
	case reflect.Float32, reflect.Float64:
		return fmt.Sprintf("%v", value)
	case reflect.Bool:
		return fmt.Sprintf("%v", value)
	case reflect.Uintptr, reflect.Complex64, reflect.Complex128, reflect.Array, reflect.Chan,
		reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice,
		reflect.Struct, reflect.UnsafePointer, reflect.Invalid:
		return ""
	default:
		return ""
	}
}

func buildEqualExp(key, value string) string {
	// if v is NULL, may need to process
	if value == "NULL" {
		return fmt.Sprintf("`%s` IS %s", key, value)
	}
	return fmt.Sprintf("`%s`=%s", key, value)
}

func buildSqlFieldsExp(mapIdxToFieldName map[int]string, fields []interface{}) []string {
	res := make([]string, len(fields))
	for idx, field := range fields {
		key := mapIdxToFieldName[idx]
		value := buildSqlFieldValue(field)
		res[idx] = buildEqualExp(key, value)
	}
	return res
}

func genInsertSql(tableMetadata *TableMetadata, rowsEvent *replication.RowsEvent) string {
	fields := make([]string, len(rowsEvent.Rows[0]))
	values := make([]string, len(rowsEvent.Rows[0]))
	for idx, field := range rowsEvent.Rows[0] {
		fields[idx] = fmt.Sprintf("`%s`", tableMetadata.Fields[idx])
		values[idx] = buildSqlFieldValue(field)
	}
	content := fmt.Sprintf(
		SqlInsertFormat,
		tableMetadata.Schema,
		tableMetadata.Table,
		strings.Join(fields, ", "),
		strings.Join(values, ", "),
	)
	return content
}

func genUpdateSql(tableMetadata *TableMetadata, rowsEvent *replication.RowsEvent, reverse bool) string {
	idx1, idx2 := 0, 1
	if reverse {
		idx1, idx2 = idx2, idx1
	}

	beforeFields := buildSqlFieldsExp(tableMetadata.Fields, rowsEvent.Rows[idx1])
	updatedFields := buildSqlFieldsExp(tableMetadata.Fields, rowsEvent.Rows[idx2])
	content := fmt.Sprintf(
		SqlUpdateFormat,
		tableMetadata.Schema,
		tableMetadata.Table,
		strings.Join(updatedFields, ", "),
		strings.Join(beforeFields, " AND "),
	)
	return content
}

func genDeleteSql(tableMetadata *TableMetadata, rowsEvent *replication.RowsEvent) string {
	fields := buildSqlFieldsExp(tableMetadata.Fields, rowsEvent.Rows[0])
	content := fmt.Sprintf(
		SqlDeleteFormat,
		tableMetadata.Schema,
		tableMetadata.Table,
		strings.Join(fields, " AND "),
	)
	return content
}

// flashback应该将sql逆序执行
func reverseFile(file string) {
	tempName := file + ".temp"

	originFile := MustOpen(file, os.O_RDONLY)
	newFile := MustOpen(tempName, os.O_TRUNC|os.O_CREATE|os.O_RDWR)
	defer originFile.Close()
	defer newFile.Close()

	buff := &bytes.Buffer{}
	char := make([]byte, 1)

	stat, _ := originFile.Stat()
	filesize := stat.Size()

	var cursor int64 = 0
	for {
		cursor -= 1
		if _, err := originFile.Seek(cursor, io.SeekEnd); err != nil {
			panic(err)
		}
		if _, err := originFile.Read(char); err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}

		buff.WriteByte(char[0])

		if char[0] == '\n' {
			if buff.Len() > 0 {
				data := reverse(buff.Bytes())
				if _, err := newFile.Write(data); err != nil {
					panic(err)
				}
			}
			buff.Reset()
		}

		if cursor == -filesize {
			break
		}
	}
	if err := os.Rename(tempName, file); err != nil {
		panic(err)
	}
}

func reverse(s []byte) []byte {
	var b bytes.Buffer
	b.Grow(len(s))
	for i := len(s) - 1; i >= 0; i-- {
		b.WriteByte(s[i])
	}
	return b.Bytes()
}

func MustOpen(file string, flag int) *os.File {
	f, err := os.OpenFile(file, flag, 0644)
	if err != nil {
		panic(err)
	}
	return f
}
