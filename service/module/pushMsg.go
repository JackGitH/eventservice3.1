package module

import (
	"fmt"

	sv "eventservice/example/serverproto"
	"io"

	"encoding/json"
	"eventservice/configMgr"
	"eventservice/db"
	"github.com/syndtr/goleveldb/leveldb"
	"time"
)

type events_push struct {
	Txid     string `json:"txid"`
	Ispushed int    `json:"ispushed"`
	Etime    int64  `json:"etime"`
}

const (
	ispushed  = 1
	notpushed = 0
)

/**
* @Title: service.go
* @Description: GoJavaRequestEvent  uchains 交易成功 推送消息到java服务器
* @author ghc
* @date 9/28/18 17:59 PM
* @version V1.0
 */
func (s *Server) GoJavaRequestEvent(stream sv.GoEventService_GoJavaRequestEventServer) error {
	/*serviceLog = logging.MustGetLogger("service")*/
	serviceLog.Info("Server GoJavaRequestEvent enter")
	var address string
	req, err := stream.Recv()
	if err == io.EOF {
		fmt.Println("read done")
		return nil
	}
	if err != nil {
		fmt.Println("Server GoJavaRequestEvent start receive Stream ERR", err)
		serviceLog.Error("Server Stream recv start receive err", err)
		return err
	}
	if req != nil {
		fmt.Println("GoJavaRequestEvent req", req.TxIdRes)
		address = req.TxIdRes
	}
	address = req.TxIdRes

	value, ok := StreamMap.Load(address)
	timein := time.Now().UnixNano()
	if ok {
		serviceLog.Info("shut down channel  address", value)
		quichchan := value.(chan *ClientQuickReq)
		cq := &ClientQuickReq{}
		cq.QuickSwitch = true
		cq.Address = address
		cq.AddressMark = timein
		quichchan <- cq
	} else {
		quichchan := make(chan *ClientQuickReq, 10)
		StreamMap.Store(address, quichchan)
	}
	time.Sleep(1 * time.Second)
	go func() {
		//time.Sleep(20)
		s.SendToJavaMsg(stream, address, AddressCount, timein)

	}()

	for {

		req, err = stream.Recv()
		if err == io.EOF {
			fmt.Println("read done")
			return nil
		}
		if err != nil {
			fmt.Println("Server GoJavaRequestEvent arround receive  Stream ERR", err)
			serviceLog.Error("Server Stream recv arround receive err", err)
			return err
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
func (s *Server) SendToJavaMsg(stream sv.GoEventService_GoJavaRequestEventServer, address string, addressCount int, tiin int64) {
	var (
		selectedb int
		ipbyte    []byte
		err       error
		txid      string
		ep        *events_push
	)
	selectedb = configMgr.SelectedDb
	// 注册时的IP 地址 对应返回的address
	ip, ok := AddressMap.Load(address)
	var ipstr string
	if ok {
		if ip != nil {
			ipstr = ip.(string)
		}
	} else {
		//fmt.Println("SendToJavaMsg ip", ip)
		//缓存不存在 去数据库中查
		if selectedb == db.Leveldbtyp {
			if ipbyte, err = s.dh.Dblevel.Get([]byte(address), nil); err != nil {
				if err != leveldb.ErrNotFound {
					serviceLog.Warningf("s.dh.Dblevel.Get [key]:%s,[err],%s", address, err)
				}
			}
			ipstr = string(ipbyte)
		}
		if selectedb == db.Mysqltyp {
			sql := fmt.Sprintf("select %s  from %s where %s = '%s'",
				ECLIENTIP, s.ec.Config.RegisterTableName, ID, address)
			serviceLog.Info("findRepeat sql", sql)

			rows, err := s.dh.Dbmysql.Query(sql) //查询去重
			if err != nil {
				serviceLog.Error("SendToJavaMsg findIp err", err)
			}
			defer rows.Close()
			if rows != nil {
				for rows.Next() {
					err = rows.Scan(&ipstr)
					if err != nil {
						serviceLog.Error("SendToJavaMsg findIp rows err", err)
					}
				}
			}
		}
	}
	serviceLog.Info("ipstr ip", ipstr, ip)
	tchan, ok := TchannelMap.Load(ipstr)

	value, oks := StreamMap.Load(address)
	if oks {
		serviceLog.Info("enter oks address", address)
		quichchan := value.(chan *ClientQuickReq)
		if ok {
			tcha := tchan.(chan *ClientTransactionJavaReq)
			for {
				select {
				//tx 成功或失败  推送消息
				case cj := <-tcha:
					err := stream.Send(&sv.ClientTransactionJavaReq{cj.TxId, cj.Ecode, cj.Emessage, cj.ChainId})
					if err != nil {
						serviceLog.Error(cj.TxId+":Server Stream send fail erro", err)
						// 出错代表没发送成功 重试次数10次 继续塞入管道
						/*if cj.SendAmount <= constRetryAmount {
							time.Sleep(1 * time.Second)
							cj.SendAmount++
							tcha <- cj
						} else {*/
						TxidsMap.Delete(cj.TxId) // 30次后再从缓存中删除 需要主动去查询了
						return
						//return err
					} else {
						TxidsMap.Delete(cj.TxId) // 发送成功再从缓存中删除
						serviceLog.Info("GoJavaRequestEvent send txid success:", cj.TxId)
						etime := time.Now().UnixNano()
						txid = cj.TxId
						ep = &events_push{
							txid,
							ispushed,
							etime,
						}
						if selectedb == db.Mysqltyp {
							s.handlePushMsgByMysql(ep)
						}
						if selectedb == db.Leveldbtyp {
							s.handlePushMsgleveldb(ep)
						}
					}
				case qk := <-quichchan:
					serviceLog.Info("Address will stop", qk.Address, "--", qk.AddressMark)
					if qk.AddressMark > tiin {
						serviceLog.Info("Address hash stop", qk.Address, "--", qk.AddressMark)
						// 退出
						return
					} else {
						if qk.AddressCount < constRetryAmount {
							qk.AddressCount++
							time.Sleep(1 * time.Second)
							quichchan <- qk
						} else {
							serviceLog.Info("quick hash been release ", qk.Address, "--", qk.AddressMark)
						}

					}

				}
			}

		} else {
			serviceLog.Info("ok is nil", address)
		}
	} else {
		serviceLog.Warning("oks is nil", address)
	}
}

// mysql
func (s *Server) handlePushMsgByMysql(push *events_push) {

	//拼接sql
	sqlValue := fmt.Sprintf("('%s','%d','%d')",
		push.Txid,
		push.Ispushed,
		push.Etime,
	)
	sqlSentence := fmt.Sprintf("insert into %s(%s,%s,%s) "+
		"values",
		s.ec.Config.EventPushedTableName,
		TXID,
		ISPUSHED,
		ETIME,
	)
	sqlFinal := sqlSentence + sqlValue
	serviceLog.Info("update ispushed sqlFinal", sqlFinal)
	usql := &UpdateIspushedsql{}
	usql.sql = sqlFinal
	//异步处理sqlupdate ispushed
	s.updateIspushedChan <- usql
}

//leveldb
func (s *Server) handlePushMsgleveldb(push *events_push) {
	var (
		spbyte []byte
		err    error
		txid   string
	)
	txid = push.Txid
	if spbyte, err = json.Marshal(push); err != nil {
		serviceLog.Errorf("json.Marshal(ep) err", err)
	}
	if err = s.dh.Dblevel.Put([]byte(events_push_name+txid), spbyte, nil); err != nil {
		serviceLog.Errorf("[txid] %s hash pushed faild", txid)
	}
	serviceLog.Infof("[txid] %s hash pushed success", txid)
}
