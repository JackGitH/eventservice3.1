package module

import (
	"fmt"
	"time"

	"io/ioutil"

	"eventservice/configMgr"
	"eventservice/db"

	"github.com/syndtr/goleveldb/leveldb/util"
	"runtime"
	"sync"
)

const (
	regeistdbsql_win = "docs/database/registerDb.sql"
	eventdbsql_win   = "docs/database/eventDb.sql"
	resultdbsql_win  = "docs/database/resultDb.sql"
	ispushedsql_win  = "docs/database/ispushedDb.sql"

	regeistdbsql_linux = "../docs/database/registerDb.sql"
	eventdbsql_linux   = "../docs/database/eventDb.sql"
	resultdbsql_linux  = "../docs/database/resultDb.sql"
	ispushedsql_linux  = "../docs/database/ispushedDb.sql"
)

/**
* @Title: service.go
* @Description:  server struct init
* @author ghc
* @date 9/25/18 16:05 PM
* @version V1.0
 */
func (s *Server) Init() {
	var (
		dhm  *db.DbHandler
		ecof *configMgr.EventConfig
		err  error
	)
	// loadconfig from yaml
	ecof = &configMgr.EventConfig{}
	if ecof, err = ecof.NewEventConfig(); err != nil {
		serviceLog.Error("newEventConfig fail", err)
	}
	s.ec = ecof
	serviceLog.Info("enter init function success")
	// getdb by config
	dhm = &db.DbHandler{}
	dbh, err := dhm.GetDbHandler(ecof)
	if err != nil {
		serviceLog.Error("getDbHandler fail", err)
		return
	}
	serviceLog.Infof("getDbHandler success")
	s.dh = dbh

	// init paramtype
	s.addressIdMap = make(map[string]string)
	TchannelMap = &sync.Map{}
	TxidsMap = &sync.Map{}
	TxidsSuccMap = &sync.Map{}
	StreamMap = &sync.Map{}                                                      //初始化缓存Ip地址map
	AddressMap = &sync.Map{}                                                     //缓存消息票数的队列
	ClientTransactionJavaReqChan = make(chan *ClientTransactionJavaReq, 1000000) //缓冲100万条数据
	GoChainRequestReqAscChan = make(chan *GoChainRequestReqAsc, 1000000)
	GoChainRequestCountAscChan = make(chan *GoChainRequestCountAsc, 1000000)
	s.updateIspushedChan = make(chan *UpdateIspushedsql, 1000000)
	go func() {
		serviceLog.Info("enter GoChainRequestEvent")
		fmt.Println("enter GoChainRequestEvent")
		s.GoChainRequestAscEvent()
	}()
	go func() {
		serviceLog.Info("enter GoChainRequestCountEvent")
		fmt.Println("enter GoChainRequestCountEvent")
		s.GoChainRequestCountAscEvent()
	}()
	// 消息中心，复杂分发消息
	go func() {
		s.DistinguishChan()
	}()
	// 定时清理缓存
	go func() {
		TaskClearMap()
	}()
	// 定时清理库
	go func() {
		TaskClearDataBase(s)
	}()
	//休眠十秒
	time.Sleep(10 * time.Second)
}

/**
* @Title: service.go
* @Description:  server struct createTable
* @author ghc
* @date 9/25/18 16:05 PM
* @version V1.0
 */
func (s *Server) CreateTable() {
	var (
		selectedb int
	)
	selectedb = configMgr.SelectedDb
	if selectedb == db.Mysqltyp {
		fmt.Println("init success")
		dh := s.dh
		//建表 events_client_address
		// todo linux 下目录使用 ../docs/database/registerDb.sql
		sys := string(runtime.GOOS) // 判断操作系统
		var sqlBytes []byte
		var sqlBytes2 []byte
		var sqlBytes3 []byte
		var sqlBytes4 []byte

		var err error
		if sys == "windows" {
			sqlBytes, err = ioutil.ReadFile(regeistdbsql_win)
			sqlBytes2, err = ioutil.ReadFile(eventdbsql_win)
			sqlBytes3, err = ioutil.ReadFile(resultdbsql_win)
			sqlBytes4, err = ioutil.ReadFile(ispushedsql_win)
		} else {
			sqlBytes, err = ioutil.ReadFile(regeistdbsql_linux)
			sqlBytes2, err = ioutil.ReadFile(eventdbsql_linux)
			sqlBytes3, err = ioutil.ReadFile(resultdbsql_linux)
			sqlBytes4, err = ioutil.ReadFile(ispushedsql_linux)
		}
		if err != nil {
			serviceLog.Error("ioutil.ReadFile sqlBytes err", err)
			return
		}
		sqlTable := string(sqlBytes)
		sqlTable2 := string(sqlBytes2)
		sqlTable3 := string(sqlBytes3)
		sqlTable4 := string(sqlBytes4)
		fmt.Println("sqlTable", sqlTable)
		fmt.Println("sqlTable2", sqlTable2)
		fmt.Println("sqlTable2", sqlTable3)
		fmt.Println("sqlTable2", sqlTable4)
		result, err := dh.Dbmysql.Exec(sqlTable)
		if err != nil {
			serviceLog.Error("createTable err", err, result)
			return
		}
		result2, err2 := dh.Dbmysql.Exec(sqlTable2)
		if err2 != nil {
			serviceLog.Error("createTable err", err2, result2)
			return
		}
		result3, err3 := dh.Dbmysql.Exec(sqlTable3)
		if err != nil {
			serviceLog.Error("createTable err", err3, result3)
			return
		}
		result4, err4 := dh.Dbmysql.Exec(sqlTable4)
		if err != nil {
			serviceLog.Error("createTable err", err4, result4)
			return
		}
		//建表 events_msg
		// todo linux 下目录使用 ../docs/database/eventDb.sql
		fmt.Println("createTable success")
		serviceLog.Info("createTable success")
	}

	// 初始化缓存ip channel
	s.LoadDbData()
	// 启动协成监听mysql push更新
	go func() {
		s.updateIspushed()
	}()
	time.Sleep(1 * time.Second)
}

// 预加载数据库address ip数据到缓存中。 防止断线重连找不到缓存数据
func (s *Server) LoadDbData() {
	var (
		selectedDb int
		tchan      chan *ClientTransactionJavaReq
	)
	selectedDb = configMgr.SelectedDb
	tchan = make(chan *ClientTransactionJavaReq, 1000000)
	if selectedDb == db.Mysqltyp {
		s.LoadDataByMysql(tchan)
	}
	if selectedDb == db.Leveldbtyp {
		s.LoadDataByleveldb(tchan)
	}

}

func (s *Server) updateIspushed() {
	for {
		select {
		case upush := <-s.updateIspushedChan:
			_, err := s.dh.Dbmysql.Exec(upush.sql) //更新状态
			if err != nil {
				serviceLog.Warning("upush.sql update fail", upush.sql)
				// 若失败了 则放回队列
				time.Sleep(1 * time.Second)
				s.updateIspushedChan <- upush
			} else {
				serviceLog.Info("upush success")
			}
		}
	}
}

// mysql
func (s *Server) LoadDataByMysql(tchan chan *ClientTransactionJavaReq) {
	var ip string
	sql := fmt.Sprintf("select ECLIENTIP from %s ", s.ec.Config.RegisterTableName)
	rows, err := s.dh.Dbmysql.Query(sql)
	if err != nil {
		serviceLog.Error("loadDbData err", err)
		return
	}
	if rows != nil {
		for rows.Next() {
			err = rows.Scan(&ip)
			if err != nil {
				serviceLog.Error("SendToJavaMsg findIp rows err", err)
				return
			} else {
				TchannelMap.Store(ip, tchan)
			}
		}
	}
}

// leveldb
func (s *Server) LoadDataByleveldb(tchan chan *ClientTransactionJavaReq) {
	var (
		keyb []byte
		key  string
		ip   string
	)
	// 模糊查询
	iter := s.dh.Dblevel.NewIterator(util.BytesPrefix([]byte(events_client_address_name)), nil)

	for iter.Next() {
		// Remember that the contents of the returned slice should not be modified, and
		// only valid until the next call to Next.
		keyb = iter.Key()
		key = string(keyb)
		ip = key[len(events_client_address_name):len(key)]
		serviceLog.Infof("LoadDataByleveldb [ip] %s", ip)
		TchannelMap.Store(ip, tchan)
	}
}
