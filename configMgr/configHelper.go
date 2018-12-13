package configMgr

import (
	"github.com/spf13/viper"
	"strings"
	"sync"
)

var SelectedDb int

type EventConfig struct {
	*viper.Viper
	logLevel map[string]string
	sync.RWMutex
	Config *Config
}
type Config struct {
	Ip                   string
	Port                 string
	Username             string
	Passwd               string
	DataBaseName         string
	RegisterTableName    string
	EventmsgtableName    string
	EventResultTableName string
	EventPushedTableName string
	IdleConn             int
	MaxConn              int
	ClearDataInterval    int
	LibName              string
	Mport                string
	SelectedDb           int
	DbBase               string
}

const (
	mip           = "mysql.ip"
	mportt        = "mysql.port"
	musername     = "mysql.username"
	mpasswd       = "mysql.passwd"
	mdatabase     = "mysql.dataBaseName"
	mregisttabn   = "mysql.registerTableName"
	meventabn     = "mysql.eventmsgTableName"
	mresulttabn   = "mysql.eventResultTableName"
	mpushtabb     = "mysql.eventPushedTableName"
	mlibname      = "mysql.libName"
	mcleardataint = "mysql.clearDataInterval"
	midlcon       = "mysql.idleConn"
	mmaxcon       = "mysql.maxConn"
	selected      = "selectdb.selected"
	dbbase        = "selectdb.dbbase"
	monitport     = "monitor.mport"
)

/**
* @Title: NewEventConfig.go
* @Description: NewEventConfig  获取config工具
* @author ghc
* @date 9/25/18 13:42 PM
* @version V1.0
 */
func (evs *EventConfig) NewEventConfig() (econfig *EventConfig, err error) {
	var (
		config               *viper.Viper
		replacer             *strings.Replacer
		ec                   *EventConfig
		conf                 *Config
		ip                   string
		errRd                error
		port                 string
		username             string
		dataBaseName         string
		registerTableName    string
		passwd               string
		eventmsgtableName    string
		eventResulttableName string
		eventPushedTableName string
		libName              string
		mport                string
		dbbased              string
		clearDataInterval    int
		selectedb            int
		idleConn             int
		maxConn              int
	)
	config = viper.New()
	config.AddConfigPath("./configFile/")
	config.AddConfigPath("../configFile/")
	config.AddConfigPath("../../configFile/")
	config.AddConfigPath("./")
	config.SetConfigName("eventConfig") // name of config file (without extension)
	config.AutomaticEnv()
	replacer = strings.NewReplacer(".", "_")
	config.SetEnvKeyReplacer(replacer)
	errRd = config.ReadInConfig()
	if errRd != nil {
		return nil, errRd
	}
	ec = &EventConfig{}
	ec.Viper = config
	ec.logLevel = make(map[string]string)

	ip = ec.GetString(mip)
	port = ec.GetString(mportt)
	username = ec.GetString(musername)
	passwd = ec.GetString(mpasswd)
	dataBaseName = ec.GetString(mdatabase)
	registerTableName = ec.GetString(mregisttabn)
	eventmsgtableName = ec.GetString(meventabn)
	eventResulttableName = ec.GetString(mresulttabn)
	eventPushedTableName = ec.GetString(mpushtabb)
	libName = ec.GetString(mlibname)
	clearDataInterval = ec.GetInt(mcleardataint)
	idleConn = ec.GetInt(midlcon)
	maxConn = ec.GetInt(mmaxcon)

	selectedb = ec.GetInt(selected)
	dbbased = ec.GetString(dbbase)

	mport = ec.GetString(monitport)

	conf = &Config{
		Ip:                   ip,
		Port:                 port,
		Username:             username,
		Passwd:               passwd,
		DataBaseName:         dataBaseName,
		RegisterTableName:    registerTableName,
		EventmsgtableName:    eventmsgtableName,
		EventResultTableName: eventResulttableName,
		EventPushedTableName: eventPushedTableName,
		ClearDataInterval:    clearDataInterval,
		IdleConn:             idleConn,
		MaxConn:              maxConn,
		LibName:              libName,
		Mport:                mport,
		SelectedDb:           selectedb,
		DbBase:               dbbased,
	}
	SelectedDb = selectedb
	ec.Config = conf
	return ec, nil

}
