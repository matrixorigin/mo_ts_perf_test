package common

import (
	"bufio"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/astaxie/beego/logs"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
)

var varRegExp = regexp.MustCompile(`%\(([a-zA-Z0-9_.\-]+)\)s`)

const (
	DefaultSection = "default"
	DepthValues    = 200
)

type MysqlConfig struct {
	MysqlAddr        string
	MysqlPort        int64
	MysqlUser        string
	MysqlPassword    string
	MysqlDatabase    string
	MysqlMaxOpenConn int64
	MysqlMaxIdleConn int64
}

type MysqlClient struct {
	db *sql.DB
}

// NewMysqlClient 创建MysqlClient
func NewMysqlClient(conf *MysqlConfig) (*MysqlClient, error) {
	db, err := OpenDatabase(conf)
	if err != nil {
		logs.Error("open database failed: err[%v]", err)
		return nil, driver.ErrBadConn
	}

	return &MysqlClient{
		db: db,
	}, nil
}

func OpenDatabase(conf *MysqlConfig) (*sql.DB, error) {
	db, err := initDatabase(conf, conf.MysqlDatabase)
	if err != nil {
		logs.Error("init database failed: err[%v]", err)
		return nil, driver.ErrBadConn
	}
	return db, nil
}

func initDatabase(conf *MysqlConfig, database string) (*sql.DB, error) {
	url := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?loc=UTC&parseTime=true", conf.MysqlUser, conf.MysqlPassword, conf.MysqlAddr, conf.MysqlPort, database)
	db, err := sql.Open("mysql", url)
	if err != nil {
		logs.Error("open database failed: err[%v]", err)
		return db, err
	}
	if err := db.Ping(); err != nil {
		logs.Error("ping database failed: err[%v]", err)
		return db, err
	}
	db.SetMaxOpenConns(int(conf.MysqlMaxOpenConn))
	db.SetMaxIdleConns(int(conf.MysqlMaxIdleConn))
	return db, nil
}

func (c *ConfigFile) GetString(section string, option string) (string, error) {

	value, err := c.GetRawString(section, option)
	if err != nil {
		return "", err
	}

	section = strings.ToLower(section)

	var i int

	for i = 0; i < DepthValues; i++ { // keep a sane depth

		vr := varRegExp.FindStringSubmatchIndex(value)
		if len(vr) == 0 {
			break
		}

		noption := value[vr[2]:vr[3]]
		noption = strings.ToLower(noption)

		// search variable in default section
		nvalue, _ := c.data[DefaultSection][noption]
		if _, ok := c.data[section][noption]; ok {
			nvalue = c.data[section][noption]
		}

		if nvalue == "" {
			return "", errors.New(fmt.Sprintf("Option not found: %s", noption))
		}

		// substitute by new value and take off leading '%(' and trailing ')s'
		value = value[0:vr[2]-2] + nvalue + value[vr[3]+2:]
	}

	if i == DepthValues {
		return "",
			errors.New(
				fmt.Sprintf(
					"Possible cycle while unfolding variables: max depth of %d reached",
					strconv.Itoa(DepthValues),
				),
			)
	}

	return value, nil
}

func (c *ConfigFile) GetRawString(section string, option string) (string, error) {

	section = strings.ToLower(section)
	option = strings.ToLower(option)

	if _, ok := c.data[section]; ok {

		if value, ok := c.data[section][option]; ok {
			return value, nil
		}

		return "", errors.New(fmt.Sprintf("Option not found: %s", option))
	}

	return "", errors.New(fmt.Sprintf("Section not found: %s", section))

}

func (c *ConfigFile) GetInt64(section string, option string) (int64, error) {

	sv, err := c.GetString(section, option)
	if err != nil {
		return 0, err
	}

	value, err := strconv.ParseInt(sv, 10, 64)
	if err != nil {
		return 0, err
	}

	return value, nil
}

func (c *ConfigFile) GetInt(section string, option string) (int, error) {

	sv, err := c.GetString(section, option)
	if err != nil {
		return 0, err
	}

	value, err := strconv.Atoi(sv)
	if err != nil {
		return 0, err
	}

	return value, nil
}

type ConfigFile struct {
	data map[string]map[string]string // Maps sections to options to values.
}

type SRConfig struct {
	Host                  string
	JdbcPort              string
	HttpPort              string
	User                  string
	Password              string
	Database              string
	Table                 string
	PointQueryTsCondition string
}

type DBConfig struct {
	Host                    string
	Port                    string
	User                    string
	Password                string
	TablePrefix             string
	PointQueryTsCondition   string
	InfluxdbTimeWindowStart string
	InfluxdbTimeWindowEnd   string
	LoadFilePath            string
}

func NewSRConfig() *SRConfig {
	return &SRConfig{}
}

func NewDBConfig() *DBConfig {
	return &DBConfig{}
}

func ReadSRFile(path string) (*SRConfig, error) {
	srConfig := NewSRConfig()
	var err error

	confFile, _ := ReadConfigFile(path)
	srConfig.Host, err = confFile.GetString("dbInfo", "host")
	if err != nil || len(srConfig.Host) <= 0 {
		fmt.Printf("load config [dbInfo:host] failed: host[%s], err[%v]\n", srConfig.Host, err)
		return srConfig, err
	}

	srConfig.JdbcPort, err = confFile.GetString("dbInfo", "jdbc_port")
	if err != nil || len(srConfig.JdbcPort) <= 0 {
		fmt.Printf("load config [dbInfo:jdbc_port] failed: jdbc_port[%s], err[%v]\n", srConfig.JdbcPort, err)
		return srConfig, err
	}

	srConfig.HttpPort, err = confFile.GetString("dbInfo", "http_port")
	if err != nil || len(srConfig.HttpPort) <= 0 {
		fmt.Printf("load config [dbInfo:http_port] failed: http_port[%s], err[%v]\n", srConfig.HttpPort, err)
		return srConfig, err
	}

	srConfig.User, err = confFile.GetString("dbInfo", "user")
	if err != nil || len(srConfig.User) <= 0 {
		fmt.Printf("load config [dbInfo:user] failed: user[%s], err[%v]\n", srConfig.User, err)
		return srConfig, err
	}

	srConfig.Password, err = confFile.GetString("dbInfo", "password")
	if err != nil || len(srConfig.Password) <= 0 {
		fmt.Printf("load config [dbInfo:password] failed: password[%s], err[%v]\n", srConfig.Password, err)
		return srConfig, err
	}

	srConfig.Database, err = confFile.GetString("dbInfo", "database")
	if err != nil || len(srConfig.Database) <= 0 {
		fmt.Printf("load config [dbInfo:database] failed: database[%s], err[%v]\n", srConfig.Database, err)
		return srConfig, err
	}

	srConfig.Table, err = confFile.GetString("dbInfo", "table")
	if err != nil || len(srConfig.Table) <= 0 {
		fmt.Printf("load config [dbInfo:table] failed: table[%s], err[%v]\n", srConfig.Table, err)
		return srConfig, err
	}

	srConfig.PointQueryTsCondition, err = confFile.GetString("dbInfo", "point_query_ts_condition")
	if err != nil || len(srConfig.PointQueryTsCondition) <= 0 {
		fmt.Printf("load config [dbInfo:point_query_ts_condition] failed: point_query_ts_condition[%s], err[%v]\n", srConfig.PointQueryTsCondition, err)
		return srConfig, err
	}
	return srConfig, nil
}

func ReadDBFile(path string, dbName string) (*DBConfig, error) {
	dbConfig := NewDBConfig()
	var err error
	confFile, err := ReadConfigFile(path)
	if err != nil {
		fmt.Printf("read config file fail, err:%v\n", err)
		return dbConfig, err
	}

	dbConfig.Host, err = confFile.GetString("dbInfo", "host")
	if err != nil || len(dbConfig.Host) <= 0 {
		fmt.Printf("load config [dbInfo:host] failed: host[%s], err[%v]\n", dbConfig.Host, err)
		return dbConfig, err
	}

	dbConfig.Port, err = confFile.GetString("dbInfo", "port")
	if err != nil || len(dbConfig.Port) <= 0 {
		fmt.Printf("load config [dbInfo:port] failed:port[%s], err[%v]\n", dbConfig.Port, err)
		return dbConfig, err
	}

	dbConfig.User, err = confFile.GetString("dbInfo", "user")
	if err != nil || len(dbConfig.User) <= 0 {
		fmt.Printf("load config [dbInfo:user] failed: user[%s], err[%v]\n", dbConfig.User, err)
		return dbConfig, err
	}

	dbConfig.Password, err = confFile.GetString("dbInfo", "password")
	if err != nil || len(dbConfig.Password) <= 0 {
		fmt.Printf("load config [dbInfo:password] failed: password[%s], err[%v]\n", dbConfig.Password, err)
		return dbConfig, err
	}

	//dbConfig.Database, err = confFile.GetString("dbInfo", "database")
	//if err != nil || len(dbConfig.Database) <= 0 {
	//	fmt.Printf("load config [dbInfo:database] failed: database[%s], err[%v]", dbConfig.Database, err)
	//	return dbConfig, err
	//}

	dbConfig.TablePrefix, err = confFile.GetString("dbInfo", "tablePrefix")
	if err != nil || len(dbConfig.TablePrefix) <= 0 {
		fmt.Printf("load config [dbInfo:tablePrefix] failed: tablePrefix[%s], err[%v]\n", dbConfig.TablePrefix, err)
		return dbConfig, err
	}

	//dbConfig.Table, err = confFile.GetString("dbInfo", "table")
	//if err != nil || len(dbConfig.Table) <= 0 {
	//	fmt.Printf("load config [dbInfo:table] failed: table[%s], err[%v]", dbConfig.Table, err)
	//	return dbConfig, err
	//}
	dbConfig.PointQueryTsCondition, err = confFile.GetString("dbInfo", "point_query_ts_condition")
	if err != nil || len(dbConfig.PointQueryTsCondition) <= 0 {
		fmt.Printf("load config [dbInfo:point_query_ts_condition] failed: point_query_ts_condition[%s], err[%v]\n", dbConfig.PointQueryTsCondition, err)
		return dbConfig, err
	}

	if dbName == InfluxDB {
		dbConfig.InfluxdbTimeWindowStart, err = confFile.GetString("dbInfo", "influxdb_timeWindow_start")
		if err != nil {
			fmt.Printf("load config [dbInfo:influxdb_timeWindow_start] failed: influxdb_timeWindow_start[%s], err[%v]\n", dbConfig.InfluxdbTimeWindowStart, err)
			return dbConfig, err
		}

		dbConfig.InfluxdbTimeWindowEnd, err = confFile.GetString("dbInfo", "influxdb_timeWindow_end")
		if err != nil {
			fmt.Printf("load config [dbInfo:influxdb_timeWindow_end] failed: influxdb_timeWindow_end[%s], err[%v]\n", dbConfig.InfluxdbTimeWindowEnd, err)
			return dbConfig, err
		}
	}

	if dbName == MO {
		dbConfig.LoadFilePath, err = confFile.GetString("dbInfo", "loadFilePath")
		if err != nil {
			fmt.Printf("load config [dbInfo:loadFilePath] failed: loadFilePath[%s], err[%v]\n", dbConfig.LoadFilePath, err)
			return dbConfig, err
		}
	}

	return dbConfig, nil
}

func ReadConfigFile(fname string) (*ConfigFile, error) {

	// 打开文件
	file, err := os.Open(fname)
	if err != nil {
		return nil, err
	}

	c := NewConfigFile()
	if err := c.read(bufio.NewReader(file)); err != nil {
		return nil, err
	}

	// 关闭文件实例
	if err := file.Close(); err != nil {
		return nil, err
	}

	return c, nil
}

func stripComments(l string) string {

	// comments are preceded by space or TAB
	for _, c := range []string{" ;", "\t;", " #", "\t#"} {
		if i := strings.Index(l, c); i != -1 {
			l = l[0:i]
		}
	}

	return l
}

// 读取文件数据
func (c *ConfigFile) read(buf *bufio.Reader) error {

	var section, option string

	for {
		l, err := buf.ReadString('\n') // parse line-by-line
		if err == io.EOF {
			if len(l) == 0 {
				break
			}
		} else if err != nil {
			return err
		}

		l = strings.TrimSpace(l)
		// switch written for readability (not performance)
		switch {
		case len(l) == 0: // empty line
			continue

		case l[0] == '#': // comment
			continue

		case l[0] == ';': // comment
			continue

		case len(l) >= 3 && strings.ToLower(l[0:3]) == "rem":
			// comment (for windows users)
			continue

		case l[0] == '[' && l[len(l)-1] == ']': // new section
			option = "" // reset multi-line value
			section = strings.TrimSpace(l[1 : len(l)-1])
			c.AddSection(section)

		case section == "": // not new section and no section defined so far
			return errors.New("Section not found: must start with section")

		default: // other alternatives
			i := firstIndex(l, []byte{'='})
			switch {
			case i > 0: // option and value
				i := firstIndex(l, []byte{'='})
				option = strings.TrimSpace(l[0:i])
				value := strings.TrimSpace(stripComments(l[i+1:]))
				c.AddOption(section, option, value)

			case section != "" && option != "":
				// continuation of multi-line value
				prev, _ := c.GetRawString(section, option)
				value := strings.TrimSpace(stripComments(l))
				c.AddOption(section, option, prev+"\n"+value)

			default:
				return errors.New(fmt.Sprintf("Could not parse line: %s", l))
			}
		}
	}

	return nil
}

func (c *ConfigFile) AddOption(section string, option string, value string) bool {

	c.AddSection(section) // make sure section exists

	// 把配置文件的选项和数据的key小写
	section = strings.ToLower(section)
	option = strings.ToLower(option)

	// 把小写后的选项和key作为map的key，然后存放数据
	_, ok := c.data[section][option]
	c.data[section][option] = value

	return !ok
}

func firstIndex(s string, delim []byte) int {

	for i := 0; i < len(s); i++ {
		for j := 0; j < len(delim); j++ {
			if s[i] == delim[j] {
				return i
			}
		}
	}

	return -1
}

func NewConfigFile() *ConfigFile {

	// 生成ConfigFile实例，并初始化c.data
	c := new(ConfigFile)
	c.data = make(map[string]map[string]string)

	c.AddSection(DefaultSection) // default section always exists

	return c
}

func (c *ConfigFile) AddSection(section string) bool {

	// 把配置文件的选项小写
	section = strings.ToLower(section)

	// 判断是否存在
	if _, ok := c.data[section]; ok {
		return false
	}

	// 数据项不存在初始化新的map
	c.data[section] = make(map[string]string)

	return true
}
