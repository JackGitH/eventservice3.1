package main

// server.go

import (
	"crypto/md5"
	"fmt"
	"github.com/op/go-logging"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"net"

	"io/ioutil"

	"encoding/hex"
	"protocdemo/configMgr"
	"protocdemo/db"
	sv "protocdemo/example/serverproto"
	"time"
)

var serviceLog = logging.MustGetLogger("service")

const (
	port = ":8852"
)
const (
	code1000 = "1000"
	code1001 = "1001"
	code1002 = "1002"
	code1003 = "1003"
	msg1000  = "交易成功"
	msg1001  = "交易进行中"
	msg1002  = "交易失败"
	msg1003  = "交易不存在"

	msgRegist01 = "该ip-port已注册"
	msgRegist02 = "注册成功"
	msgRegist03 = "注册失败"
)

//server核心
type server struct {
	ec *configMgr.EventConfig
	dh *db.DbHandler
}

// 建表字段 ghc date 2018年9月25日10点41分
type ASSETFIELDNAME string

const (
	ID          ASSETFIELDNAME = "ID"
	IP          ASSETFIELDNAME = "IP"
	TXID        ASSETFIELDNAME = "TXID"
	ECLIENTPORT ASSETFIELDNAME = "ECLIENTPORT"
	ECLIENTIP   ASSETFIELDNAME = "ECLIENTIP"
	ETIME       ASSETFIELDNAME = "ETIME"
	REMARK      ASSETFIELDNAME = "REMARK"
	PORT        ASSETFIELDNAME = "PORT"
)

func (s *server) GoClientRegistEvent(ctx context.Context, request *sv.ClientRegisterAddressReq) (*sv.ClientRegisterAddressRes, error) {
	fmt.Println("request0", request)
	ip := request.AddRessIpReq
	fmt.Println("--------------------------------")
	fmt.Println("request1", request)
	fmt.Println("s", s)
	port := request.AddRessPortReq
	remarkReq := request.RemarkReq
	tm := time.Now().UnixNano()
	ipPort := ip + port
	sql := fmt.Sprintf("select count(*) as acount from %s where %s = '%s' and %s ='%s'",
		s.ec.Config.RegisterTableName, ECLIENTIP, ip, ECLIENTPORT, port)
	serviceLog.Info("findRepeat sql", sql)

	rows, err := s.dh.Db.Query(sql) //查询去重
	defer rows.Close()
	var acount int
	if rows != nil {
		for rows.Next() {
			err = rows.Scan(&acount)
		}
	}
	if err != nil {
		serviceLog.Error("findRepeat err", err)
	}
	fmt.Println("acount", acount)

	//去重

	if acount == 0 {
		//给注册信息分配hash id
		Md5Inst := md5.New()
		Md5Inst.Write([]byte(ipPort))
		id := Md5Inst.Sum([]byte(""))
		idStr := hex.EncodeToString(id)

		//拼接sql
		sqlValue := fmt.Sprintf("('%s','%s','%s','%d','%s')",
			idStr,
			ip,
			port,
			tm,
			remarkReq,
		)
		sqlSentence := fmt.Sprintf("insert into %s(%s,%s,%s,%s,%s) "+
			"values",
			s.ec.Config.RegisterTableName,
			ID,
			ECLIENTIP,
			ECLIENTPORT,
			ETIME,
			REMARK,
		)
		sqlFinal := sqlSentence + sqlValue

		//写库
		serviceLog.Info("sqlFinal is ", sqlFinal)
		_, err = s.dh.Db.Exec(sqlFinal)
		if err != nil {
			/*ph.DataCacheMap.Delete(sc.DataHash)*/
			serviceLog.Errorf("write db err:%s", err.Error())
		}
		return &sv.ClientRegisterAddressRes{MessageRes: msgRegist02, IsSuccess: true, MessageIDRes: idStr}, nil
	} else {
		return &sv.ClientRegisterAddressRes{MessageRes: msgRegist01, IsSuccess: false, MessageIDRes: ""}, nil
	}
	/*
		i := 0
		for rows.Next() {
			i++
		}
		if i >= 1 {
			return &sv.ClientRegisterAddressRes{MessageRes: msgRegist01, IsSuccess: false, MessageIDRes: ""}, nil
		}*/

}
func (s *server) GoClientRequestEvent(ctx context.Context, in *sv.ClientTransactionReq) (*sv.ClientTransactionRes, error) {
	return nil, nil
}
func (s *server) GoChainRequestEvent(sv.GoEventService_GoChainRequestEventServer) error {
	return nil
}

/**
* @Title: service.go
* @Description:  server struct init
* @author ghc
* @date 9/25/18 16:05 PM
* @version V1.0
 */
func (s *server) init() {
	serviceLog.Debug("enter init function success")
	dhm := &db.DbHandler{}
	dbh, err := dhm.GetDbHandler()
	if err != nil {
		serviceLog.Error("getDbHandler fail", err)
		return
	}
	s.dh = dbh
	ecof := &configMgr.EventConfig{}
	evcf, err := ecof.NewEventConfig()
	if err != nil {
		serviceLog.Error("newEventConfig fail", err)
	}
	s.ec = evcf
}

/**
* @Title: service.go
* @Description:  server struct createTable
* @author ghc
* @date 9/25/18 16:05 PM
* @version V1.0
 */
func (s *server) createTable() {
	//初始化server struct
	s.init()
	fmt.Println("init success")
	dh := s.dh
	//建表 events_client_address
	sqlBytes, err := ioutil.ReadFile("docs/database/registerDb.sql")
	if err != nil {
		serviceLog.Error("ioutil.ReadFile sqlBytes err", err)
		return
	}
	sqlTable := string(sqlBytes)
	fmt.Println("sqlTable", sqlTable)
	result, err := dh.Db.Exec(sqlTable)
	if err != nil {
		serviceLog.Error("createTable err", err, result)
		return
	}
	//建表 events_msg
	sqlBytes2, err := ioutil.ReadFile("docs/database/eventDb.sql")
	if err != nil {
		serviceLog.Error("ioutil.ReadFile sqlBytes2 err", err)
		return
	}
	sqlTable2 := string(sqlBytes2)
	fmt.Println("sqlTable2", sqlTable2)
	result2, err := dh.Db.Exec(sqlTable2)
	if err != nil {
		serviceLog.Error("createTable err", err, result2)
		return
	}
	fmt.Println("createTable success")
	serviceLog.Info("createTable success")
}

func main() {
	serv := &server{}
	serv.createTable()
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	sv.RegisterGoEventServiceServer(s, serv)
	s.Serve(lis)

	//test 使用
	/*se := &server{}
	se.createTable()*/
}
