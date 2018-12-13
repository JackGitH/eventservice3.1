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

type events_msg struct {
	Txid       string `json:"txid"`
	Txip       string `json:"txip"`
	TotalNodes int32  `json:"totalnodes"`
	ChainId    string `json:"chainid"`
	Etime      int64  `json:"etime"`
}

/**
* @Title: service.go
* @Description: GoChainRequestEvent  uchains commitx阶段 收集txid
* @author ghc
* @date 9/27/18 15:31 PM
* @version V1.0
 */
func (s *Server) GoChainRequestEvent(stream sv.GoEventService_GoChainRequestEventServer) error {
	for {
		var (
			req *sv.ChainTranscationReq
			err error
		)
		req, err = stream.Recv()
		if err == io.EOF {
			fmt.Println("read done")
			return nil
		}
		if err != nil {
			fmt.Println("Server  GoChainRequestEvent Stream ERR", err)
			serviceLog.Error("Server GoChainRequestEvent Stream recv err", err)
			stream.Send(&sv.ChainTranscationRes{TxIdRes: "", IsReceivedRes: false})
			return err
		}
		if req != nil {
			_, ok := TxidsMap.Load(req.TxIdReq)                                        //先缓存查询 若不存在，则取查询数据库
			serviceLog.Info("--------A-----------------------", s.totalEventTxid)      //todo 后面去掉这个日志  todo 以后去掉 测试tps观察使用
			serviceLog.Info("--------B-----------------------", s.totalEventCountTxid) //todo 后面去掉这个日志  todo 以后去掉 测试tps观察使用
			if !ok {
				gasc := &GoChainRequestReqAsc{}
				gasc.req = req
				GoChainRequestReqAscChan <- gasc
				s.totalEventTxid++
			}
			if err = stream.Send(&sv.ChainTranscationRes{TxIdRes: req.TxIdReq, IsReceivedRes: true}); err != nil {
				serviceLog.Error(req.TxIdReq + "Server Stream send fail")
				return err
			}
		} else {
			serviceLog.Warning("GoChainRequestEvent stream.Recv() req is nil")
		}
	}

}

/**
* @Title: service.go
* @Description: GoChainRequestAscEvent  异步处理 收集txid
* @author ghc
* @date 9/27/18 15:31 PM
* @version V1.0
 */
func (s *Server) GoChainRequestAscEvent() error {
	var (
		req           *sv.ChainTranscationReq
		reqTxId       string
		reqTxIp       string
		reqTotalNotes int32
		reqChainId    string
		etime         int64
		selectedb     int
	)
	selectedb = configMgr.SelectedDb
	for {
		select {
		case asc := <-GoChainRequestReqAscChan:
			serviceLog.Info("[len GoChainRequestReqAscChan]", len(GoChainRequestReqAscChan))
			serviceLog.Info("--------AAA-----------------------", s.totalEventTxid) // todo 以后去掉 测试tps观察使用
			req = asc.req                                                           //req 上一步已经做了nil判断
			reqTxId = req.TxIdReq
			reqTxIp = req.TxIpReq
			reqTotalNotes = req.TotalVotesReq
			reqChainId = req.ChainIdReq
			etime = time.Now().UnixNano()
			collectedTxidMap := &events_msg{
				reqTxId,
				reqTxIp,
				reqTotalNotes,
				reqChainId,
				etime,
			}

			// 使用leveldb || mysql
			if selectedb == db.Leveldbtyp {
				s.handle_eventsmsg_Byleveldb(collectedTxidMap)
			} else if selectedb == db.Mysqltyp {
				s.handle_eventsmsg_ByMysql(collectedTxidMap)
			}
			voteMap := &VoteAccount{}
			voteMap.txid = reqTxId
			voteMap.chainId = reqChainId
			voteMap.address = reqTxIp
			//voteMap.address = "10.10.70.146" //todo 暂时改掉ip  ****
			voteMap.totalNodes = reqTotalNotes
			voteMap.votesSuccessMap = make(map[string]string)
			voteMap.votesFailedMap = make(map[string]string)
			voteMap.txtask = time.AfterFunc(60*time.Second, func() { //todo  定时取消有问题，需要再研究
				TaskEvent(reqTxId, s)
			})
			TxidsMap.Store(reqTxId, voteMap) //缓存txid和票数
		}
	}

}

// leveldb
func (s *Server) handle_eventsmsg_Byleveldb(coll *events_msg) {
	var (
		err           error
		dblevel       *leveldb.DB
		key           []byte
		indextabArray []byte
		txid          string
	)
	txid = coll.Txid
	key = []byte(events_msg_name + coll.Txid)
	dblevel = s.dh.Dblevel
	if _, err = dblevel.Get(key, nil); err != nil {
		if err != leveldb.ErrNotFound {
			serviceLog.Warningf("s.dh.Dblevel.Get [key]:%s,[err],%s", txid, err)
		}
		// if key not exit,will return "key not found" err
		if indextabArray, err = json.Marshal(coll); err != nil {
			serviceLog.Errorf("json.Marshal(coll) [key] %s,[err] %s", key, err)
		}
		if err = dblevel.Put(key, indextabArray, nil); err != nil {
			serviceLog.Errorf("dblevel.Put [key] %s,[err] %s", key, err)
		} else {
			serviceLog.Infof("dblevel hash received [txid] %s and save events_msg success ", txid)
		}

	}
}

// mysql
func (s *Server) handle_eventsmsg_ByMysql(coll *events_msg) {
	var (
		sql string
	)
	sql = fmt.Sprintf("select count(*) as acount from %s where %s = '%s'",
		s.ec.Config.EventmsgtableName, TXID, coll.Txid)
	serviceLog.Info("findRepeat sql", sql)

	rows, err := s.dh.Dbmysql.Query(sql) //查询去重
	if err != nil {
		serviceLog.Error("findRepeat err", err)
	}
	var acount int
	if rows != nil {
		for rows.Next() {
			err = rows.Scan(&acount)
			if err != nil {
				serviceLog.Error("rows.Scan err", err)
			}
		}
		rows.Close()
	} else {
		serviceLog.Warning("row is nil")
	}
	if acount == 0 {
		//拼接sql
		sqlValue := fmt.Sprintf("('%s','%d','%s','%s','%d')",
			coll.Txid,
			coll.Etime,
			coll.ChainId,
			coll.Txip,
			coll.TotalNodes,
		)
		sqlSentence := fmt.Sprintf("insert into %s(%s,%s,%s,%s,%s) "+
			"values",
			s.ec.Config.EventmsgtableName,
			TXID,
			ETIME,
			CHAINID,
			TXIP,
			TOTALNODES,
		)
		sqlFinal := sqlSentence + sqlValue

		//写库
		serviceLog.Info("sqlFinal is ", sqlFinal)
		_, err = s.dh.Dbmysql.Exec(sqlFinal)
		if err != nil {
			serviceLog.Errorf("write db err:%s", err.Error())
		}
	}
}
