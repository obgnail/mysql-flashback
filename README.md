# mysql-flashback

## 简介

基于解析 binlog 的 Mysql 数据库闪回（flashback）工具。实现数据库的快速回滚。

- 标准模式下，通过不同的参数生成某个范围内执行的 CUD 操作 SQL。
- 回滚模式下，通过不同的参数生成某个范围内执行的 CUD 操作的回滚 SQL。

利用 binlog 闪回，**binlog 格式必须将设置为 row**。



## 版本

MySQL 5.6+



## 使用

### 输出标准 SQL

```bash
./mysql-flashback -h=127.0.0.1 -P=3306 -u=root -p=root -d=es_river -start-file="/Users/XXX/volume/mysql/data/mysql-bin.000026" -start-pos=259 -filter-tx=false -output="raw.sql"
```

输出：

```mysql
/* BEGIN -> Transaction BEGIN | binlog: mysql-bin.000026 | pos: (259, 335) | time: 2022-06-26 17:38:18 */
INSERT INTO `es_river`.`user`(`uuid`, `name`, `name_pinyin`, `email`, `avatar`, `phone`, `password`, `status`, `create_time`, `modify_time`) VALUES ('NsktovQv', '123', '123', 'qwe@qwe.com', 'qwe', '123456789', '', 1, 1656236297995157000, 1656236297995157000); /* ROW -> binlog: mysql-bin.000026 | pos: (259, 511) | time: 2022-06-26 17:38:18 */
/* COMMIT -> Transaction COMMIT | xid: 131 | binlog: mysql-bin.000026 | pos: (511, 542) | time: 2022-06-26 17:38:18 */

/* BEGIN -> Transaction BEGIN | binlog: mysql-bin.000026 | pos: (607, 683) | time: 2022-06-26 17:40:07 */
INSERT INTO `es_river`.`user`(`uuid`, `name`, `name_pinyin`, `email`, `avatar`, `phone`, `password`, `status`, `create_time`, `modify_time`) VALUES ('GRXVSPx5', 'es_river', '123', 'qwe@qwe.com', 'qwe', '123456789', '', 1, 1656236407174364000, 1656236407174364000); /* ROW -> binlog: mysql-bin.000026 | pos: (607, 864) | time: 2022-06-26 17:40:07 */
INSERT INTO `es_river`.`user`(`uuid`, `name`, `name_pinyin`, `email`, `avatar`, `phone`, `password`, `status`, `create_time`, `modify_time`) VALUES ('HBfZ7bFD', 'es_river', '123', 'qwe@qwe.com', 'qwe', '123456789', '', 1, 1656236407174364000, 1656236407174364000); /* ROW -> binlog: mysql-bin.000026 | pos: (607, 1045) | time: 2022-06-26 17:40:07 */
INSERT INTO `es_river`.`user`(`uuid`, `name`, `name_pinyin`, `email`, `avatar`, `phone`, `password`, `status`, `create_time`, `modify_time`) VALUES ('RKQ7xete', 'es_river', '123', 'qwe@qwe.com', 'qwe', '123456789', '', 1, 1656236407174364000, 1656236407174364000); /* ROW -> binlog: mysql-bin.000026 | pos: (607, 1226) | time: 2022-06-26 17:40:07 */
/* COMMIT -> Transaction COMMIT | xid: 148 | binlog: mysql-bin.000026 | pos: (1226, 1257) | time: 2022-06-26 17:40:07 */

/* BEGIN -> Transaction BEGIN | binlog: mysql-bin.000026 | pos: (1322, 1398) | time: 2022-06-26 17:44:42 */
INSERT INTO `es_river`.`user`(`uuid`, `name`, `name_pinyin`, `email`, `avatar`, `phone`, `password`, `status`, `create_time`, `modify_time`) VALUES ('D5t6Ekij', 'es_river2', '123', 'qwe@qwe.com', 'qwe', '123456789', '', 1, 1656236682906976000, 1656236682906976000); /* ROW -> binlog: mysql-bin.000026 | pos: (1322, 1580) | time: 2022-06-26 17:44:42 */
INSERT INTO `es_river`.`user`(`uuid`, `name`, `name_pinyin`, `email`, `avatar`, `phone`, `password`, `status`, `create_time`, `modify_time`) VALUES ('YRNWxCYS', 'es_river2', '123', 'qwe@qwe.com', 'qwe', '123456789', '', 1, 1656236682906976000, 1656236682906976000); /* ROW -> binlog: mysql-bin.000026 | pos: (1322, 1762) | time: 2022-06-26 17:44:42 */
INSERT INTO `es_river`.`user`(`uuid`, `name`, `name_pinyin`, `email`, `avatar`, `phone`, `password`, `status`, `create_time`, `modify_time`) VALUES ('T3mrAzoi', 'es_river2', '123', 'qwe@qwe.com', 'qwe', '123456789', '', 1, 1656236682906976000, 1656236682906976000); /* ROW -> binlog: mysql-bin.000026 | pos: (1322, 1944) | time: 2022-06-26 17:44:42 */
UPDATE `es_river`.`user` SET `uuid`='NsktovQv', `name`='123', `name_pinyin`='123', `email`='qwe@qwe.com', `avatar`='qwe', `phone`='123456789', `password`='', `status`=0, `create_time`=1656236297995157000, `modify_time`=1656236297995157000 WHERE `uuid`='NsktovQv' AND `name`='123' AND `name_pinyin`='123' AND `email`='qwe@qwe.com' AND `avatar`='qwe' AND `phone`='123456789' AND `password`='' AND `status`=1 AND `create_time`=1656236297995157000 AND `modify_time`=1656236297995157000 LIMIT 1; /* ROW -> binlog: mysql-bin.000026 | pos: (1322, 2187) | time: 2022-06-26 17:44:42 */
UPDATE `es_river`.`user` SET `uuid`='GRXVSPx5', `name`='es_riverXXXXXXXX', `name_pinyin`='123', `email`='qwe@qwe.com', `avatar`='qwe', `phone`='123456789', `password`='', `status`=1, `create_time`=1656236407174364000, `modify_time`=1656236682906976000 WHERE `uuid`='GRXVSPx5' AND `name`='es_river' AND `name_pinyin`='123' AND `email`='qwe@qwe.com' AND `avatar`='qwe' AND `phone`='123456789' AND `password`='' AND `status`=1 AND `create_time`=1656236407174364000 AND `modify_time`=1656236407174364000 LIMIT 1; /* ROW -> binlog: mysql-bin.000026 | pos: (1322, 2448) | time: 2022-06-26 17:44:42 */
/* COMMIT -> Transaction COMMIT | xid: 196 | binlog: mysql-bin.000026 | pos: (2448, 2479) | time: 2022-06-26 17:44:42 */

/* BEGIN -> Transaction BEGIN | binlog: mysql-bin.000026 | pos: (2544, 2620) | time: 2022-06-26 19:46:35 */
DELETE FROM `es_river`.`user` WHERE `uuid`='YRNWxCYS' AND `name`='es_river2' AND `name_pinyin`='123' AND `email`='qwe@qwe.com' AND `avatar`='qwe' AND `phone`='123456789' AND `password`='' AND `status`=1 AND `create_time`=1656236682906976000 AND `modify_time`=1656236682906976000 LIMIT 1; /* ROW -> binlog: mysql-bin.000026 | pos: (2544, 2802) | time: 2022-06-26 19:46:35 */
/* COMMIT -> Transaction COMMIT | xid: 574 | binlog: mysql-bin.000026 | pos: (2802, 2833) | time: 2022-06-26 19:46:35 */
```



### 输出回滚 SQL

```bash
./mysql-flashback -h=127.0.0.1 -P=3306 -u=root -p=root -d=es_river -start-file="/Users/XXX/volume/mysql/data/mysql-bin.000026" -start-pos=259 -rollback -output="rollback.sql"
```

```mysql
INSERT INTO `es_river`.`user`(`uuid`, `name`, `name_pinyin`, `email`, `avatar`, `phone`, `password`, `status`, `create_time`, `modify_time`) VALUES ('YRNWxCYS', 'es_river2', '123', 'qwe@qwe.com', 'qwe', '123456789', '', 1, 1656236682906976000, 1656236682906976000); /* ROW -> binlog: mysql-bin.000026 | pos: (2544, 2802) | time: 2022-06-26 19:46:35 */
UPDATE `es_river`.`user` SET `uuid`='GRXVSPx5', `name`='es_river', `name_pinyin`='123', `email`='qwe@qwe.com', `avatar`='qwe', `phone`='123456789', `password`='', `status`=1, `create_time`=1656236407174364000, `modify_time`=1656236407174364000 WHERE `uuid`='GRXVSPx5' AND `name`='es_riverXXXXXXXX' AND `name_pinyin`='123' AND `email`='qwe@qwe.com' AND `avatar`='qwe' AND `phone`='123456789' AND `password`='' AND `status`=1 AND `create_time`=1656236407174364000 AND `modify_time`=1656236682906976000 LIMIT 1; /* ROW -> binlog: mysql-bin.000026 | pos: (1322, 2448) | time: 2022-06-26 17:44:42 */
UPDATE `es_river`.`user` SET `uuid`='NsktovQv', `name`='123', `name_pinyin`='123', `email`='qwe@qwe.com', `avatar`='qwe', `phone`='123456789', `password`='', `status`=1, `create_time`=1656236297995157000, `modify_time`=1656236297995157000 WHERE `uuid`='NsktovQv' AND `name`='123' AND `name_pinyin`='123' AND `email`='qwe@qwe.com' AND `avatar`='qwe' AND `phone`='123456789' AND `password`='' AND `status`=0 AND `create_time`=1656236297995157000 AND `modify_time`=1656236297995157000 LIMIT 1; /* ROW -> binlog: mysql-bin.000026 | pos: (1322, 2187) | time: 2022-06-26 17:44:42 */
DELETE FROM `es_river`.`user` WHERE `uuid`='T3mrAzoi' AND `name`='es_river2' AND `name_pinyin`='123' AND `email`='qwe@qwe.com' AND `avatar`='qwe' AND `phone`='123456789' AND `password`='' AND `status`=1 AND `create_time`=1656236682906976000 AND `modify_time`=1656236682906976000 LIMIT 1; /* ROW -> binlog: mysql-bin.000026 | pos: (1322, 1944) | time: 2022-06-26 17:44:42 */
DELETE FROM `es_river`.`user` WHERE `uuid`='YRNWxCYS' AND `name`='es_river2' AND `name_pinyin`='123' AND `email`='qwe@qwe.com' AND `avatar`='qwe' AND `phone`='123456789' AND `password`='' AND `status`=1 AND `create_time`=1656236682906976000 AND `modify_time`=1656236682906976000 LIMIT 1; /* ROW -> binlog: mysql-bin.000026 | pos: (1322, 1762) | time: 2022-06-26 17:44:42 */
DELETE FROM `es_river`.`user` WHERE `uuid`='D5t6Ekij' AND `name`='es_river2' AND `name_pinyin`='123' AND `email`='qwe@qwe.com' AND `avatar`='qwe' AND `phone`='123456789' AND `password`='' AND `status`=1 AND `create_time`=1656236682906976000 AND `modify_time`=1656236682906976000 LIMIT 1; /* ROW -> binlog: mysql-bin.000026 | pos: (1322, 1580) | time: 2022-06-26 17:44:42 */
DELETE FROM `es_river`.`user` WHERE `uuid`='RKQ7xete' AND `name`='es_river' AND `name_pinyin`='123' AND `email`='qwe@qwe.com' AND `avatar`='qwe' AND `phone`='123456789' AND `password`='' AND `status`=1 AND `create_time`=1656236407174364000 AND `modify_time`=1656236407174364000 LIMIT 1; /* ROW -> binlog: mysql-bin.000026 | pos: (607, 1226) | time: 2022-06-26 17:40:07 */
DELETE FROM `es_river`.`user` WHERE `uuid`='HBfZ7bFD' AND `name`='es_river' AND `name_pinyin`='123' AND `email`='qwe@qwe.com' AND `avatar`='qwe' AND `phone`='123456789' AND `password`='' AND `status`=1 AND `create_time`=1656236407174364000 AND `modify_time`=1656236407174364000 LIMIT 1; /* ROW -> binlog: mysql-bin.000026 | pos: (607, 1045) | time: 2022-06-26 17:40:07 */
DELETE FROM `es_river`.`user` WHERE `uuid`='GRXVSPx5' AND `name`='es_river' AND `name_pinyin`='123' AND `email`='qwe@qwe.com' AND `avatar`='qwe' AND `phone`='123456789' AND `password`='' AND `status`=1 AND `create_time`=1656236407174364000 AND `modify_time`=1656236407174364000 LIMIT 1; /* ROW -> binlog: mysql-bin.000026 | pos: (607, 864) | time: 2022-06-26 17:40:07 */
```



## 参数

### Mysql 连接参数

- `h`：mysql host
- `P`：mysql port
- `u`：mysql user
- `p`：mysql password

### binlog 筛选参数

- `d`：只解析目标 db 的 sql，必填。
- `t`：只解析目标 table 的 sql，使用英文逗号隔开。为空则解析全部 table。
- `start-file`：起始解析文件。必填。
- `start-pos`：起始解析位置。为空则从 0 开始。
- `start-time`：起始解析时间，格式'%Y-%m-%d %H:%M:%S'。为空则不过滤。
- `stop-file`：终止解析文件。为空则解析到最新数据。
- `stop-pos`：终止解析时间，格式'%Y-%m-%d %H:%M:%S'。为空则不过滤。
- `stop-time`：中止始解析时间，格式'%Y-%m-%d %H:%M:%S'。为空则不过滤

### event 筛选参数

- `gtid-regexp`：若启用 GTID MODE，可用正则过滤，为空则不过滤。
- `only-sql-type`：解析指定类型，支持 INSERT, UPDATE, DELETE。使用英文逗号隔开。为空则不过滤。
- `only-DML`：只解析 dml，忽略 ddl。在 rollback 参数启用时，自动关闭。
- `filter-tx`：生成的标准 SQL 说明其所在的事务。在 rollback 参数启用时，自动关闭。默认为 true。

### 解析模式参数

- `rollback`：为 false 则输出标准 SQL，为 true 则生成 flashback 文件。默认为 false。

### 其他参数

- `output`：输出文件。默认为 stdout，即标准输出流。



## 回滚原理

首先解析 binlog，得到所有的 CUD event。根据这些 event 组装成标准 SQL。接着根据标准 SQL 生成回滚 SQL。

对于 delete 操作，生成的回滚语句是 insert。

```mysql
# raw
DELETE FROM `es_river`.`user` WHERE `uuid`='YRNWxCYS' AND `name`='es_river2' AND `name_pinyin`='123' AND `email`='qwe@qwe.com' AND `avatar`='qwe' AND `phone`='123456789' AND `password`='' AND `status`=1 AND `create_time`=1656236682906976000 AND `modify_time`=1656236682906976000 LIMIT 1;

# rollback
INSERT INTO `es_river`.`user`(`uuid`, `name`, `name_pinyin`, `email`, `avatar`, `phone`, `password`, `status`, `create_time`, `modify_time`) VALUES ('YRNWxCYS', 'es_river2', '123', 'qwe@qwe.com', 'qwe', '123456789', '', 1, 1656236682906976000, 1656236682906976000);
```

对于 insert 操作，生成的回滚语句是 delete。

```mysql
# raw
INSERT INTO `es_river`.`user`(`uuid`, `name`, `name_pinyin`, `email`, `avatar`, `phone`, `password`, `status`, `create_time`, `modify_time`) VALUES ('NsktovQv', '123', '123', 'qwe@qwe.com', 'qwe', '123456789', '', 1, 1656236297995157000, 1656236297995157000);

# rollback
DELETE FROM `es_river`.`user` WHERE `uuid`='GRXVSPx5' AND `name`='es_river' AND `name_pinyin`='123' AND `email`='qwe@qwe.com' AND `avatar`='qwe' AND `phone`='123456789' AND `password`='' AND `status`=1 AND `create_time`=1656236407174364000 AND `modify_time`=1656236407174364000 LIMIT 1;
```

对于 update 操作，生成的回滚语句交换 set 和 where 的值。

```mysql
# raw
UPDATE `es_river`. `user` SET `uuid`='GRXVSPx5', `name`='es_riverXXXXXXXX', `name_pinyin`='123', `email`='qwe@qwe.com', `avatar`='qwe', `phone`='123456789', `password`='', `status`=1, `create_time`=1656236407174364000, `modify_time`=1656236682906976000 WHERE `uuid`='GRXVSPx5' AND `name`='es_river' AND `name_pinyin`='123' AND `email`='qwe@qwe.com' AND `avatar`='qwe' AND `phone`='123456789' AND `password`='' AND `status`=1 AND `create_time`=1656236407174364000 AND `modify_time`=1656236407174364000 LIMIT 1;

# rollback
UPDATE `es_river`.`user` SET `uuid`='GRXVSPx5', `name`='es_river', `name_pinyin`='123', `email`='qwe@qwe.com', `avatar`='qwe', `phone`='123456789', `password`='', `status`=1, `create_time`=1656236407174364000, `modify_time`=1656236407174364000 WHERE `uuid`='GRXVSPx5' AND `name`='es_riverXXXXXXXX' AND `name_pinyin`='123' AND `email`='qwe@qwe.com' AND `avatar`='qwe' AND `phone`='123456789' AND `password`='' AND `status`=1 AND `create_time`=1656236407174364000 AND `modify_time`=1656236682906976000 LIMIT 1;
```

最后，只需要将所有的 SQL 语句，倒序输出即可。



## 其他

- 此工具基于 binlog，而 TABLE_MAP_EVENT 是没有存储 Table Field Name 的，且无法得知该 db 下的所有 Table。因此必须去数据库查。这就是需要连接数据库的原因。
- binlog 对于 ddl 的记录并不完全。对于 drop table，create index 之类的语句在 binlog 找不到完整的数据。如果你不小心删库了，那还是赶紧跑路吧。
- 因为生成的回滚 SQL 是根据标准 SQL 处理后的倒序输出，所以如果 output 参数使用 stdout，一样会生成一个中间文件，其格式为 fmt.Sprintf("rollback_%d.sql", time.Now().Unix())。
- 若执行 rollback SQL，一样也会生成 binlog event，所以理论上你可以使用 flashback 去 flashback 自己 :)

