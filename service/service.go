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
	"sync"
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
	code1006 = "1006"
	code1007 = "1007"

	msg1000 = "交易成功"
	msg1001 = "交易进行中"
	msg1002 = "交易失败" //AppProcess Fail uchains返回码和这里对应
	msg1003 = "交易不存在"
	msg1004 = "交易异常"
	msg1005 = "未注册，请求失败"
	msg1006 = "共识前检查异常" //BeforeConsCheck Fail
	msg1007 = "共识后检查异常" //AfterConsCheckAndUpdateData Fail

	msgRegist01 = "该ip-port已注册"
	msgRegist02 = "注册成功"
	msgRegist03 = "注册失败"
	msgRegist04 = "交易异常"

	constAmount = 1 / 3 // 1/3容错
)

//server核心
type server struct {
	ec                 *configMgr.EventConfig
	dh                 *db.DbHandler
	addressIdMap       map[string]string
	updateIspushedChan chan *UpdateIspushedsql
	switchButton       bool
}
type UpdateIspushedsql struct {
	sql string
}

var TxidsMap sync.Map

var ClientTransactionJavaReqChan chan *ClientTransactionJavaReq

type ClientTransactionJavaReq struct {
	TxId     string
	Ecode    string
	Emessage string
	ChainId  string
}

type VoteAccount struct {
	txid            string
	totalNodes      int32
	votesSuccessMap map[string]string
	votesFailedMap  map[string]string
	txtask          *time.Timer
	chainId         string
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
	ISPUSHED    ASSETFIELDNAME = "ISPUSHED"
)

type MsgHandler interface {
	SendToJavaMsg(javaMsg *sv.ClientTransactionJavaReq) error
}

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
		isPushed := 0 //默认未推送
		etime := time.Now().UnixNano()

		_, ok := TxidsMap.Load(reqTxId) //先缓存查询 若不存在，则取查询数据库
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
				sqlValue := fmt.Sprintf("('%s','%s','%s','%d','%s','%s','%d','%d')",
					reqTxId,
					reqEcode,
					reqMessage,
					etime,
					reqChainId,
					reqTxIp,
					reqTotalNotes,
					isPushed,
				)
				sqlSentence := fmt.Sprintf("insert into %s(%s,%s,%s,%s,%s,%s,%s,%s) "+
					"values",
					s.ec.Config.EventmsgtableName,
					TXID,
					ECODE,
					EMESSAGE,
					ETIME,
					CHAINID,
					TXIP,
					TOTALNODES,
					ISPUSHED,
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
			voteMap.chainId = reqChainId
			voteMap.totalNodes = reqTotalNotes
			voteMap.votesSuccessMap = make(map[string]string)
			voteMap.votesFailedMap = make(map[string]string)
			voteMap.txtask = time.AfterFunc(120, func() {
				TaskEvent(reqTxId, s)
			})
			TxidsMap.Store(reqTxId, voteMap) //缓存txid和票数
		}
		err = stream.Send(&sv.ChainTranscationRes{TxIdRes: req.TxIdReq, IsReceivedRes: true})
		if err != nil {
			serviceLog.Error(req.TxIdReq + "Server Stream send fail")
			return err
		}
	}

}

/**
* @Title: service.go
* @Description: GoChainRequestCountEvent  uchains 交易统计阶段 收集votes
* @author ghc
* @date 9/28/18 17:59 PM
* @version V1.0
 */
func (s *server) GoChainRequestCountEvent(stream sv.GoEventService_GoChainRequestCountEventServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			fmt.Println("read done")
			return nil
		}
		if err != nil {
			fmt.Println("Server Stream ERR", err)
			serviceLog.Error("Server Stream recv err", err)
			stream.Send(&sv.ChainTranscationAccountRes{TxIdRes: "", IsReceivedRes: false})
			return err
		}
		txidreq := req.TxIdReq
		codereq := req.CodeReq
		messreq := req.MessageReq
		issuccreq := req.IsSuccessReq
		nodeidreq := req.NodeIdReq
		value, ok := TxidsMap.Load(txidreq) //map 中不存在，
		if !ok {

			serviceLog.Warning(txidreq, "hash handle over or txid not exit")

		} else {

			voteVal := value.(VoteAccount)
			totalNods := voteVal.totalNodes * constAmount
			var code string
			var msg string
			if issuccreq {
				voteVal.votesSuccessMap[txidreq] = nodeidreq
				TxidsMap.Store(txidreq, voteVal)
				voteAmount := int32(len(voteVal.votesSuccessMap))
				succ := voteAmount >= totalNods
				if succ {
					code = code1000
					msg = msg1000
					sqlFinal := fmt.Sprintf("update %s set %s = '%s' ,%s = '%s' where %s = '%s'",
						s.ec.Config.EventmsgtableName, ECODE, code, EMESSAGE, msg, TXID, txidreq)
					_, err := s.dh.Db.Exec(sqlFinal)
					if err != nil {
						serviceLog.Error("GoChainRequestCountEvent db set ecode fail txid", txidreq)
					} else {
						tarnsJavaReq := &ClientTransactionJavaReq{}
						tarnsJavaReq.TxId = txidreq
						tarnsJavaReq.ChainId = voteVal.chainId
						tarnsJavaReq.Ecode = code
						tarnsJavaReq.Emessage = msg
						ClientTransactionJavaReqChan <- tarnsJavaReq
						err = stream.Send(&sv.ChainTranscationAccountRes{txidreq, true})
						if err != nil {
							serviceLog.Error(txidreq + "Server Stream send fail")
							return err
						}
					}
				}
			} else {

				voteVal.votesFailedMap[txidreq] = nodeidreq
				TxidsMap.Store(txidreq, voteVal)
				voteAmount := int32(len(voteVal.votesFailedMap))
				fail := voteAmount >= totalNods
				if fail {
					code = codereq //todo 这里的失败原因使用的uchains返回的
					msg = messreq  //
					sqlFinal := fmt.Sprintf("update %s set %s = '%s' ,%s = '%s' where %s = '%s'",
						s.ec.Config.EventmsgtableName, ECODE, code, EMESSAGE, msg, TXID, txidreq)
					_, err := s.dh.Db.Exec(sqlFinal)
					if err != nil {
						serviceLog.Error("GoChainRequestCountEvent db set ecode fail txid", txidreq)
					} else {
						tarnsJavaReq := &ClientTransactionJavaReq{}
						tarnsJavaReq.TxId = txidreq
						tarnsJavaReq.ChainId = voteVal.chainId
						tarnsJavaReq.Ecode = code
						tarnsJavaReq.Emessage = msg
						ClientTransactionJavaReqChan <- tarnsJavaReq
						err = stream.Send(&sv.ChainTranscationAccountRes{txidreq, true})
						if err != nil {
							serviceLog.Error(txidreq + "Server Stream send fail")
							return err
						}
					}

				}
			}

		}

	}
}

/**
* @Title: service.go
* @Description: GoJavaRequestEvent  uchains 交易成功 推送消息到java服务器
* @author ghc
* @date 9/28/18 17:59 PM
* @version V1.0
 */
func (s *server) GoJavaRequestEvent(stream sv.GoEventService_GoJavaRequestEventServer) error {
	for {
		fmt.Println("before switchButton")
		if !s.switchButton {

			go func() {
				time.Sleep(20)
				s.SendToJavaMsg(stream)
			}()
		}

		//tx 成功或失败  推送消息
		req, err := stream.Recv()
		if err == io.EOF {
			fmt.Println("read done")
			return err
		}
		if err != nil {
			fmt.Println("Server Stream ERR", err)
			serviceLog.Error("Server Stream recv err", err)
			return err
		}
		if req != nil {
			serviceLog.Info("GoJavaRequestEvent send txid success:", req.TxIdRes)
			sqlFinal := fmt.Sprintf("update %s set %s = '%d'  where %s = '%s'",
				s.ec.Config.EventmsgtableName, ISPUSHED, 1, TXID, req.TxIdRes)
			serviceLog.Info("update ispushed sqlFinal", sqlFinal)
			usql := &UpdateIspushedsql{}
			usql.sql = sqlFinal
			s.updateIspushedChan <- usql //异步处理sqlupdate ispushed
		}

	}

}

/**
* @Title: service.go
* @Description: SendToJavaMsg  uchains 交易成功 推送消息到java服务器
* @author ghc
* @date 9/29/18 10:47 AM
* @version V1.0
 */
func (s *server) SendToJavaMsg(stream sv.GoEventService_GoJavaRequestEventServer) error {
	s.switchButton = true
		fmt.Println("SendToJavaMsg has send")
		for i := 0; i < 10; i++ {
			err := stream.Send(&sv.ClientTransactionJavaReq{"11356456", "1001", "发送成功", "coupon"})
			if err != nil {
				serviceLog.Error("11356456"+":Server Stream send fail erro", err)
				return err
			}
		}

		return nil
/*	for {
		select {
		//tx 成功或失败  推送消息
		case cj := <-ClientTransactionJavaReqChan:
			err := stream.Send(&sv.ClientTransactionJavaReq{cj.TxId, cj.Ecode, cj.Emessage, cj.ChainId})
			if err != nil {
				serviceLog.Error(cj.TxId+":Server Stream send fail erro", err)
				return err
			}
		}
	}*/

}

/**
* @Title: service.go
* @Description: TaskEvent   定时器处理阶段
* @author ghc
* @date 9/27/18 16:55 PM
* @version V1.0
 */
func TaskEvent(txid string, s *server) {
	value, _ := TxidsMap.Load(txid) //map 中不存在，
	voteVal := value.(VoteAccount)
	totalNods := voteVal.totalNodes * constAmount
	voteAmountSu := int32(len(voteVal.votesSuccessMap))
	voteAmountFal := int32(len(voteVal.votesFailedMap))
	succ := voteAmountSu >= totalNods
	fail := voteAmountFal >= totalNods
	var code string
	var msg string
	if succ || fail {
		if succ {
			code = code1000
			msg = msg1000
		}
		if fail {
			code = code1002
			msg = msg1002
		}
		sql := fmt.Sprintf("update %s set %s = '%s' ,%s = '%s' where %s = '%s'",
			s.ec.Config.EventmsgtableName, ECODE, code, EMESSAGE, msg, TXID, txid)
		serviceLog.Info("update sql", sql)

		_, err := s.dh.Db.Exec(sql) //更新状态
		if err == nil {
			serviceLog.Info("txid write db success", txid)
			tarnsJavaReq := &ClientTransactionJavaReq{}
			tarnsJavaReq.TxId = txid
			tarnsJavaReq.ChainId = voteVal.chainId
			tarnsJavaReq.Ecode = code
			tarnsJavaReq.Emessage = msg
			ClientTransactionJavaReqChan <- tarnsJavaReq //传进通道 调用 response服务端方法
			TxidsMap.Delete(txid)                        //成功 删除缓存
		} else {
			serviceLog.Error("txid write db failed", txid)
		}
	}

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
	s.addressIdMap = make(map[string]string) //初始化缓存Ip地址map
	TxidsMap = sync.Map{}                    //缓存消息票数的队列
	ClientTransactionJavaReqChan = make(chan *ClientTransactionJavaReq, 20)
	s.updateIspushedChan = make(chan *UpdateIspushedsql, 20)
}

func (s *server) updateIspushed() {
	for {
		select {
		case upush := <-s.updateIspushedChan:
			_, err := s.dh.Db.Exec(upush.sql) //更新状态
			if err != nil {
				serviceLog.Info("upush.sql update fail", upush.sql)
			}
		}
	}
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
	// 启动协成监听mysql push更新
	go func() {
		s.updateIspushed()
	}()
}

//todo  抽象出公共部分  以后完善
/*func (s *server) commonCountEventHandle(succc bool, fail bool, txidreq string, voteVal VoteAccount, nodeidreq string, totalNods int32, stream sv.GoEventService_GoChainRequestCountEventServer) {
	voteVal.votesSuccessMap[txidreq] = nodeidreq
	TxidsMap.Store(txidreq, voteVal)
	voteAmount := int32(len(voteVal.votesSuccessMap))
	succ := voteAmount >= totalNods
	if succ {
		code = code1000
		msg = msg1000
		sqlFinal := fmt.Sprintf("update %s set %s = '%s' ,%s = '%s' where %s = '%s'",
			s.ec.Config.EventmsgtableName, ECODE, code, EMESSAGE, msg, TXID, txidreq)
		_, err := s.dh.Db.Exec(sqlFinal)
		if err != nil {
			serviceLog.Error("GoChainRequestCountEvent db set ecode fail txid", txidreq)
		} else {
			tarnsJavaReq := &ClientTransactionJavaReq{}
			tarnsJavaReq.TxId = txidreq
			tarnsJavaReq.ChainId = voteVal.chainId
			tarnsJavaReq.Ecode = code
			tarnsJavaReq.Emessage = msg
			ClientTransactionJavaReqChan <- tarnsJavaReq
			err = stream.Send(&sv.ChainTranscationAccountRes{txidreq, true})
			if err != nil {
				serviceLog.Error(txidreq + "Server Stream send fail")
				return err
			}
		}
	}
}*/

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
