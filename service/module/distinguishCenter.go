package module

import (
	"encoding/json"
	"eventservice/db"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"time"
	"eventservice/configMgr"
)

/**
* @Title: service.go
* @Description: DistinguishChan  处理通知慢的问题
* @author ghc
* @date 10/21/18 21:46 PM
* @version V1.0
 */
func (s *Server) DistinguishChan() {
	serviceLog.Info("enter DistinguishChan")
	var (
		selectedb int
		txidd     string
		ipr       string
		val       interface{}
		ok        bool
	)
	selectedb = configMgr.SelectedDb
	for {
		select {
		//tx 成功或失败  推送消息
		case cj := <-ClientTransactionJavaReqChan:
			//fmt.Println("GoJavaRequestEvent hash receive txid:", cj.TxId, cj.Emessage) //todo 如果不注释掉 发送不成功的会一直刷日志
			serviceLog.Info("DistinguishChan hash receive txid:", cj.TxId, cj.Emessage)
			txidd = cj.TxId
			// 从交易缓存中获取txid 对应的 IP

			val, ok = TxidsMap.Load(txidd)

			if !ok {
				if selectedb == db.Leveldbtyp {
					ipr = s.findIpByLeveldb(txidd)
				} else if selectedb == db.Mysqltyp {
					ipr = s.findIpByMysql(txidd)
				}

			} else {
				voteVal := val.(*VoteAccount)
				ipr = voteVal.address
			}
			tchan, ok := TchannelMap.Load(ipr)
			if ok {
				serviceLog.Info("tcha hash send txid", txidd, "ipr", ipr)
				tcha := tchan.(chan *ClientTransactionJavaReq)
				tcha <- cj
			} else {
				if cj.DistinguishAmount <= constSendAmount {
					cj.DistinguishAmount++
					time.Sleep(1 * time.Second)
					ClientTransactionJavaReqChan <- cj
				}
			}
		}
	}
}

// leveldb
func (s *Server) findIpByLeveldb(txid string) string {
	var (
		key  []byte
		val  []byte
		err  error
		emsg *events_msg
		ip   string
	)
	key = []byte(events_msg_name + txid)
	val, err = s.dh.Dblevel.Get(key, nil)
	if err != nil {
		if err != leveldb.ErrNotFound {
			serviceLog.Warningf("s.dh.Dblevel.Get [key]:%s,[err],%s", txid, err)
			return ip
		}
		return ip
	}
	if err = json.Unmarshal(val, emsg); err != nil {
		serviceLog.Errorf("json.Unmarshal(val,emsg) [err] %s,[txid] %s", err, txid)
		return ip
	}
	ip = emsg.Txip
	serviceLog.Infof("findIpByLeveldb [ip] %s success", ip)
	return emsg.Txip
}

// mysql
func (s *Server) findIpByMysql(txid string) string {
	var (
		ipr string
		sql string
	)
	sql = fmt.Sprintf("select %s  from %s where %s = '%s'",
		TXIP, s.ec.Config.EventmsgtableName, TXID, txid)
	serviceLog.Info("find s.ec.Config.EventmsgtableName findIp sql", sql)

	rows, err := s.dh.Dbmysql.Query(sql) //查询去重
	if err != nil {
		serviceLog.Error("find s.ec.Config.EventmsgtableName findIp err", err)
	}
	defer rows.Close()
	if rows != nil {
		for rows.Next() {
			err = rows.Scan(&ipr)
			if err != nil {
				serviceLog.Error("DistinguishChan findIp rows err", err)
			} // 没必要查出来继续放进缓存中 因为接下来处理完会马上删掉
		}
	}
	return ipr
}
