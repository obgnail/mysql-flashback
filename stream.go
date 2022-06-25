package mysql_flashback

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/errors"
	"os"
	"path"
	"strings"
)

const StopErrorPayload = "__stop_parse_binlog__"

var StopError = fmt.Errorf(StopErrorPayload)

// dbm下的binlog下的event
// 若要提前终止解析,请返回StopError
type SteamFunc func(dbm *DBMap, binlog *BinlogInfo, event *replication.BinlogEvent) (err error)

func BinlogStream(mysqlUri string, binlog string, position uint32, streamFunc SteamFunc) error {
	dbm, err := LinkDB(mysqlUri)
	if err != nil {
		return errors.Trace(err)
	}
	logs, err := filterBinlog(dbm, binlog, position)
	if err != nil {
		return errors.Trace(err)
	}

	for _, log := range logs {
		if _, err := os.Stat(binlog); os.IsNotExist(err) {
			return errors.Trace(err)
		}
		err = ParseFile(dbm, log, streamFunc)
		return errors.Trace(err)
	}
	return nil
}

func DefaultStream(mysqlUri string, path string, streamFunc SteamFunc) error {
	err := BinlogStream(mysqlUri, path, 0, streamFunc)
	return errors.Trace(err)
}

func ParseFile(dbm *DBMap, log *BinlogInfo, streamFunc SteamFunc) error {
	p := replication.NewBinlogParser()
	err := p.ParseFile(log.path, int64(log.startPos), func(event *replication.BinlogEvent) error {
		err := streamFunc(dbm, log, event)
		if err != nil {
			if err == StopError {
				return StopError
			}
			return errors.Trace(err)
		}
		return nil
	})
	if err != nil && strings.Contains(err.Error(), StopErrorPayload) {
		return nil
	}
	return errors.Trace(err)
}

func filterBinlog(DBMap *DBMap, binlog string, position uint32) ([]*BinlogInfo, error) {
	if _, err := os.Stat(binlog); os.IsNotExist(err) {
		return nil, errors.Trace(err)
	}

	logs, err := getBinlogFromDb(DBMap.db)
	if err != nil {
		return nil, errors.Trace(err)
	}

	dir := path.Dir(binlog)
	name := path.Base(binlog)
	for index, binlog := range logs {
		logs[index].path = path.Join(dir, binlog.name)
		if binlog.name == name {
			if binlog.size < position {
				return nil, fmt.Errorf("position err. range(0, %d), get: %d", binlog.size, position)
			}
			logs[index].startPos = position
			return logs[index:], nil
		}
	}
	return nil, fmt.Errorf("binlog no found: %s", binlog)
}
