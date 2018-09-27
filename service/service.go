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
	"io"
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
	code1004 = "1004"
	code1005 = "1005"
	msg1000  = "交易成功"
	msg1001  = "交易进行中"
	msg1002  = "交易失败"
	msg1003  = "交易不存在"
	msg1004  = "交易异常"
	msg1005  = "未注册，请求失败"

	msgRegist01 = "该ip-port已注册"
	msgRegist02 = "注册成功"
	msgRegist03 = "注册失败"
	msgRegist04 = "交易异常"
)

//server核心
type server struct {
	ec           *configMgr.EventConfig
	dh           *db.DbHandler
	addressIdMap map[string]string
}

var TxidsMap map[string]VoteAccount

type VoteAccount struct {
	txid       string
	totalVotes int
	votesMap   map[string]string
}

// 建表字段 ghc date 2018年9月25日10点41分
type ASSETFIELDNAME string

const (
	ID          ASSETFIELDNAME = "ID"
	IP          ASSETFIELDNAME = "IP"
	ECODE       ASSETFIELDNAME = "ECODE"
	EMESSAGE    ASSETFIELDNAME = "EMESSAGE"
	CHAINID     ASSETFIELDNAME = "CHAINID"
	TXID        ASSETFIELDNAME = "TXID"
	ECLIENTPORT ASSETFIELDNAME = "ECLIENTPORT"
	ECLIENTIP   ASSETFIELDNAME = "ECLIENTIP"
	ETIME       ASSETFIELDNAME = "ETIME"
	REMARK      ASSETFIELDNAME = "REMARK"
	PORT        ASSETFIELDNAME = "PORT"
	TXIP        ASSETFIELDNAME = "TXIP"
	TOTALNODES  ASSETFIELDNAME = "TOTALNODES"
)

/**
* @Title: service.go
* @Description: GoClientRegistEvent  注册
* @author ghc
* @date 9/25/18 16:50 PM
* @version V1.0
 */
func (s *server) GoClientRegistEvent(ctx context.Context, request *sv.ClientRegisterAddressReq) (*sv.ClientRegisterAddressRes, error) {
	ip := request.AddRessIpReq
	port := request.AddRessPortReq
	remarkReq := request.RemarkReq
	tm := time.Now().UnixNano()
	ipPort := ip + ":" + port
	sql := fmt.Sprintf("select count(*) as acount from %s where %s = '%s' and %s ='%s'",
		s.ec.Config.RegisterTableName, ECLIENTIP, ip, ECLIENTPORT, port)
	serviceLog.Info("findRepeat sql", sql)

	rows, err := s.dh.Db.Query(sql) //查询去重
	if err != nil {
		serviceLog.Error("findRepeat err", err)
		return &sv.ClientRegisterAddressRes{MessageRes: msgRegist04, IsSuccess: false, MessageIDRes: ""}, err
	}
	defer rows.Close()
	var acount int
	if rows != nil {
		for rows.Next() {
			err = rows.Scan(&acount)
			if err != nil {
				return &sv.ClientRegisterAddressRes{MessageRes: msgRegist04, IsSuccess: false, MessageIDRes: ""}, err
			}
		}
	} else {
		return &sv.ClientRegisterAddressRes{MessageRes: msgRegist03, IsSuccess: false, MessageIDRes: ""}, nil
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
		s.addressIdMap[idStr] = ipPort //放在缓存中
		return &sv.ClientRegisterAddressRes{MessageRes: msgRegist02, IsSuccess: true, MessageIDRes: idStr}, nil
	} else {
		return &sv.ClientRegisterAddressRes{MessageRes: msgRegist01, IsSuccess: false, MessageIDRes: ""}, nil
	}

}

/**
* @Title: service.go
* @Description: GoClientRequestEvent  处理客户端请求txid
* @author ghc
* @date 9/25/18 16:50 PM
* @version V1.0
 */
func (s *server) GoClientRequestEvent(ctx context.Context, request *sv.ClientTransactionReq) (*sv.ClientTransactionRes, error) {
	addressId := request.AddressIdReq
	chainId := request.ChainIdReq
	txid := request.TxIdReq

	cap, ok := s.addressIdMap[addressId] //先判断是否注册
	fmt.Println("s.addressIdMap：", s.addressIdMap)
	if !ok {
		fmt.Println("addressId Non-existent", cap)
		serviceLog.Info("addressId Non-existent:", addressId)
		return &sv.ClientTransactionRes{TxIdRes: txid, CodeRes: code1005, MessageRes: msg1005, TimeRes: "", ChainIdRes: ""}, nil
	}

	sql := fmt.Sprintf("select %s,%s,%s,%s,%s from %s where %s = '%s' and %s ='%s'",
		TXID, ECODE, EMESSAGE, ETIME, CHAINID, s.ec.Config.EventmsgtableName, TXID, txid, CHAINID, chainId)
	serviceLog.Info("RequestEvent sql", sql)

	rows, err := s.dh.Db.Query(sql) //查询去重
	if err != nil {
		serviceLog.Error("GoClientRequestEvent err", err)
		return &sv.ClientTransactionRes{TxIdRes: txid, CodeRes: code1004, MessageRes: msg1004, TimeRes: "", ChainIdRes: ""}, err
	}
	defer rows.Close()
	if rows != nil {
		for rows.Next() {

			var txidr string
			var ecoder string
			var emessager string
			var etimer string
			var chainIdr string

			fmt.Println("txidr:", txidr, "---ecoder:", ecoder, "---emessager:", emessager, "---etimer:", etimer, "---chainIdr:", chainIdr)
			err = rows.Scan(&txidr, &ecoder, &emessager, &etimer, &chainIdr)
			if err != nil {
				fmt.Println("GoClientRequestEvent err", err)
				serviceLog.Error("GoClientRequestEvent err", err)
				return &sv.ClientTransactionRes{TxIdRes: txid, CodeRes: code1004, MessageRes: msg1004, TimeRes: "", ChainIdRes: ""}, err
			}
			return &sv.ClientTransactionRes{TxIdRes: txidr, CodeRes: ecoder, MessageRes: emessager, TimeRes: etimer, ChainIdRes: chainIdr}, err
		}

	} else {
		return &sv.ClientTransactionRes{TxIdRes: txid, CodeRes: code1003, MessageRes: msg1003, TimeRes: "", ChainIdRes: ""}, err
	}

	return &sv.ClientTransactionRes{TxIdRes: txid, CodeRes: code1003, MessageRes: msg1003, TimeRes: "", ChainIdRes: ""}, err
}

/**
* @Title: service.go
* @Description: GoChainRequestEvent  uchains commitx阶段 收集txid
* @author ghc
* @date 9/27/18 15:31 PM
* @version V1.0
 */
func (s *server) GoChainRequestEvent(stream sv.GoEventService_GoChainRequestEventServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			fmt.Println("read done")
			return nil
		}
		if err != nil {
			fmt.Println("Server Stream ERR", err)
			serviceLog.Error("Server Stream recv err", err)
			stream.Send(&sv.ChainTranscationRes{TxIdRes: "", IsReceivedRes: false})
			return err
		}
		fmt.Println("req: ", req)
		reqTxId := req.TxIdReq
		reqTxIp := req.TxIpReq
		reqTotalNotes := req.TotalVotesReq
		reqChainId := req.ChainIdReq
		reqEcode := code1001
		reqMessage := msg1001
		etime := time.Now().UnixNano()

		_, ok := TxidsMap[reqTxId] //先缓存查询 若不存在，则取查询数据库
		if !ok {

			sql := fmt.Sprintf("select count(*) as acount from %s where %s = '%s'",
				s.ec.Config.EventmsgtableName, TXID, reqTxId)
			serviceLog.Info("findRepeat sql", sql)

			rows, err := s.dh.Db.Query(sql) //查询去重
			if err != nil {
				serviceLog.Error("findRepeat err", err)
				return err
			}
			defer rows.Close()
			var acount int
			if rows != nil {
				for rows.Next() {
					err = rows.Scan(&acount)
					if err != nil {
						return err
					}
				}
			} else {
				return nil
			}
			if acount == 0 {
				//拼接sql
				sqlValue := fmt.Sprintf("('%s','%s','%s','%d','%s','%s','%d')",
					reqTxId,
					reqEcode,
					reqMessage,
					etime,
					reqChainId,
					reqTxIp,
					reqTotalNotes,
				)
				sqlSentence := fmt.Sprintf("insert into %s(%s,%s,%s,%s,%s,%s,%s) "+
					"values",
					s.ec.Config.EventmsgtableName,
					TXID,
					ECODE,
					EMESSAGE,
					ETIME,
					CHAINID,
					TXIP,
					TOTALNODES,
				)
				sqlFinal := sqlSentence + sqlValue

				//写库
				serviceLog.Info("sqlFinal is ", sqlFinal)
				_, err = s.dh.Db.Exec(sqlFinal)
				if err != nil {
					/*ph.DataCacheMap.Delete(sc.DataHash)*/
					serviceLog.Errorf("write db err:%s", err.Error())
				}
			}
			voteMap := VoteAccount{}
			voteMap.txid = reqTxId
			voteMap.totalVotes = 0
			voteMap.votesMap = make(map[string]string)
			TxidsMap[reqTxId] = voteMap //缓存txid和票数
		}
		err = stream.Send(&sv.ChainTranscationRes{TxIdRes: req.TxIdReq, IsReceivedRes: true})
		if err != nil {
			serviceLog.Error(req.TxIdReq + "Server Stream send fail")
			return err
		}
	}

}
func (s *server) GoChainRequestCountEvent(sv.GoEventService_GoChainRequestCountEventServer) error {
	return nil
}

/*func (s *server) GoChainRequestCountEvent(ctx context.Context) (*sv.GoEventService_GoChainRequestEventClient, error) {
	return nil, nil
}*/

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
	s.addressIdMap = make(map[string]string) //初始化缓存map
	TxidsMap = make(map[string]VoteAccount)  //缓存消息票数的队列
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
