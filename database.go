package mysql_flashback

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"strings"
)

func LinkDB(uri string) (*DBMap, error) {
	db, err := sql.Open("mysql", uri)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err := db.Ping(); err != nil {
		return nil, errors.Trace(err)
	}

	return NewDBMap(db), nil
}

type TableMetadata struct {
	Schema string
	Table  string
	Fields map[int]string // map[idx]columnName （idx: field在table中的idx）
}

type DBMap struct {
	tableMetadataMap map[uint64]*TableMetadata
	fieldsCache      map[string]map[int]string // map[schema_table]Fields
	db               *sql.DB
}

func NewDBMap(db *sql.DB) *DBMap {
	return &DBMap{
		db:               db,
		tableMetadataMap: make(map[uint64]*TableMetadata),
		fieldsCache:      make(map[string]map[int]string),
	}
}

func (m *DBMap) LookupTableMetadata(id uint64) (*TableMetadata, bool) {
	val, ok := m.tableMetadataMap[id]
	return val, ok
}

func (m *DBMap) Add(id uint64, schema, table string) error {
	fields, err := m.getFields(schema, table)
	if err != nil {
		return errors.Trace(err)
	}

	m.tableMetadataMap[id] = &TableMetadata{schema, table, fields}
	return nil
}

func (m *DBMap) getFields(schema, table string) (map[int]string, error) {
	cacheKey := fmt.Sprintf("%s_%s", schema, table)
	if cachedFields, ok := m.fieldsCache[cacheKey]; ok {
		return cachedFields, nil
	}

	fields, err := getFieldsFromDb(m.db, schema, table)
	m.fieldsCache[cacheKey] = fields
	if err != nil {
		return nil, errors.Trace(err)
	}

	return fields, nil
}

//  map[idx]columnName
func getFieldsFromDb(db *sql.DB, schema string, table string) (map[int]string, error) {
	sql := "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION"
	rows, err := db.Query(sql, schema, table)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	fields := make(map[int]string)
	i := 0

	var columnName string
	for rows.Next() {
		if err := rows.Scan(&columnName); err != nil {
			return nil, errors.Trace(err)
		}

		fields[i] = columnName
		i++
	}

	return fields, nil
}

type BinlogInfo struct {
	name string
	size uint32

	path     string
	startPos uint32
}

func getBinlogFromDb(db *sql.DB) ([]*BinlogInfo, error) {
	sql := "SHOW MASTER LOGS;"
	rows, err := db.Query(sql)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	var result []*BinlogInfo
	var fileName string
	var fileSize uint32
	for rows.Next() {
		if err := rows.Scan(&fileName, &fileSize); err != nil {
			return nil, errors.Trace(err)
		}
		result = append(result, &BinlogInfo{
			name:     fileName,
			size:     fileSize,
			startPos: 0,
		})
	}
	return result, nil
}

func getGitdModeFromDb(db *sql.DB) (ok bool, err error) {
	sql := "SELECT @@GLOBAL.GTID_MODE;"
	rows, err := db.Query(sql)
	if err != nil {
		return false, errors.Trace(err)
	}
	defer rows.Close()
	var status string
	for rows.Next() {
		if err := rows.Scan(&status); err != nil {
			return false, errors.Trace(err)
		}
	}
	return strings.ToUpper(status) == "ON", nil
}

func getBinlogDirFromDb(db *sql.DB) (dirname string, err error) {
	sql := `SHOW variables WHERE Variable_name = "log_bin_basename";`
	rows, err := db.Query(sql)
	if err != nil {
		return "", errors.Trace(err)
	}
	defer rows.Close()
	for rows.Next() {
		if err = rows.Scan(&dirname); err != nil {
			return "", errors.Trace(err)
		}
	}
	return
}
