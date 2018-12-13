package db

import (
	"database/sql"
	"eventservice/configMgr"
	"fmt"
	_ "github.com/go-sql-driver/MySQL"
	"github.com/op/go-logging"
	leveldb "github.com/syndtr/goleveldb/leveldb"
	"io/ioutil"
	"runtime"
	"sync"
)

type DbHandler struct {
	Dbmysql *sql.DB
	Dblevel *leveldb.DB
	dbLock  sync.RWMutex
}

const (
	Mysqltyp   = 1
	Leveldbtyp = 2
	registDb   = "../leveldbRegist"
)

var Port string

var registerDbLog = logging.MustGetLogger("registerDb")

/**
* @Title: reistgetDbHandlererDb.go
* @Description: getDbHandler  获取db句柄
* @author ghc
* @date 9/25/18 13:42 PM
* @version V1.0
 */
func (dh *DbHandler) GetDbHandler(ec *configMgr.EventConfig) (db *DbHandler, err error) {
	var (
		dbhBase      *sql.DB
		dbh          *sql.DB
		ev           *configMgr.EventConfig
		result       sql.Result
		ip           string
		port         string
		userName     string
		passwd       string
		dataBaseName string
		mport        string
		sqlDataBase  string
		selectedb    int
	)
	ev = ec
	selectedb = ev.Config.SelectedDb
	mport = ev.Config.Mport

	Port = ":" + mport
	if selectedb != Mysqltyp && selectedb != Leveldbtyp {
		registerDbLog.Infof("config :selectedb not mysql and leveldb,will default set leveldb!!!,selectedb %d", selectedb)
		selectedb = Leveldbtyp
	}
	if selectedb == Mysqltyp {

		ip = ev.Config.Ip
		port = ev.Config.Port
		userName = ev.Config.Username
		passwd = ev.Config.Passwd
		dataBaseName = ev.Config.DataBaseName

		if dbh, err = sql.Open("mysql", userName+":"+passwd+"@tcp("+ip+":"+port+")/"+"?charset=utf8&multiStatements=true"); err != nil {
			fmt.Print("err", err)
			return nil, err
		}

		//建库
		// todo linux 下目录使用 docs/database/createDataBase.sql
		sys := string(runtime.GOOS) // 判断操作系统
		var sqlBytes []byte
		if sys == "windows" {
			if sqlBytes, err = ioutil.ReadFile("docs/database/createDataBase.sql"); err != nil {
				registerDbLog.Error("ioutil.ReadFile createDataBase.sql err", err)
				fmt.Print("ioutil.ReadFile createDataBase.sql fail")
				return
			}
		} else {
			if sqlBytes, err = ioutil.ReadFile("../docs/database/createDataBase.sql"); err != nil {
				registerDbLog.Error("ioutil.ReadFile createDataBase.sql err", err)
				fmt.Print("ioutil.ReadFile createDataBase.sql fail")
				return
			}
		}
		sqlDataBase = string(sqlBytes)
		fmt.Println("sqlTable", sqlDataBase)

		if result, err = dbh.Exec(sqlDataBase); err != nil {
			registerDbLog.Error("dbh.Exec sqlDataBase err", err, result)
			fmt.Print("dbh.Exec sqlDataBase fail")
			return
		}
		registerDbLog.Info("createDataBase success")
		fmt.Println("createDataBase success")
		//dataBaseName 要与sql语句名称一致
		if dbhBase, err = sql.Open("mysql", userName+":"+passwd+"@tcp("+ip+":"+port+")/"+dataBaseName+"?charset=utf8&multiStatements=true"); err != nil {
			registerDbLog.Error("sql.Open dbhBase err", err)
			fmt.Print("sql.Open dbhBase err fail")
			return
		}
		dbhBase.SetConnMaxLifetime(0)
		dbhBase.SetMaxOpenConns(50) // 最大连接数
		dbhBase.SetMaxIdleConns(50) // 空闲连接数
		// 使用mysql 也会使用leveldb作为缓存库；不使用mysql，则leveldb作为工作库
		if dh.Dblevel, err = dh.MakeLevelDbHandler(ev.Config.DbBase); err != nil {
			return nil, fmt.Errorf("dh.MakeLevelDbHandler err%s", err)
		}
		dh.Dbmysql = dbhBase
		return dh, nil
	} else {
		if dh.Dblevel, err = dh.MakeLevelDbHandler(ev.Config.DbBase); err != nil {
			return nil, fmt.Errorf("dh.MakeLevelDbHandler err%s", err)
		}
		return dh, nil
	}
	return nil, nil
}

//test
/*func main() {
	test := &DbHandler{}
	test.GetDbHandler()
}*/
// 新建levelDb仓库
func (dh *DbHandler) MakeLevelDbHandler(levelpath string) (*leveldb.DB, error) {
	var (
		db  *leveldb.DB
		err error
	)
	dh.dbLock.RLock()
	defer dh.dbLock.RUnlock()
	if db, err = leveldb.OpenFile(levelpath, nil); err != nil {
		registerDbLog.Error("leveldb.OpenFile err", err)
		return nil, err
	}
	return db, nil
}
