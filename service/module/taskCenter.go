package module

import (
	"encoding/json"
	"eventservice/configMgr"
	"eventservice/db"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"strconv"
	"sync"
	"time"
)

/**
* @Title: service.go
* @Description: TaskEvent   定时器处理阶段
* @author ghc
* @date 9/27/18 16:55 PM
* @version V1.0
 */
func TaskEvent(txid string, s *Server) {
	var (
		selectedb int
		er        *events_result
		code      string
		msg       string
		err       error
	)
	selectedb = configMgr.SelectedDb

	value, ok := TxidsMap.Load(txid) //map 中不存在，
	if ok {
		etime := time.Now().UnixNano()
		serviceLog.Info("enter TaskEvent txid", txid)
		voteVal := value.(*VoteAccount)
		totalNods := voteVal.totalNodes*1/3 + 1
		voteAmountSu := int32(len(voteVal.votesSuccessMap))
		voteAmountFal := int32(len(voteVal.votesFailedMap))
		succ := voteAmountSu >= totalNods
		fail := voteAmountFal >= totalNods
		serviceLog.Info("txid", txid, "totalNods:", totalNods, "voteAmountSu:", voteAmountSu)
		if succ || fail {
			if succ {
				code = code1000
				msg = msg1000
			}
			if fail {
				code = code1002
				msg = msg1002
			}
			er = &events_result{
				txid,
				code,
				msg,
				etime,
			}
			if selectedb == db.Leveldbtyp {
				err = handleTaskByleveldb(er, s)
			}
			if selectedb == db.Mysqltyp {
				err = handleTaskByMysql(er, s)
			}
			if err == nil {
				serviceLog.Info("txid write db success", txid)
				tarnsJavaReq := &ClientTransactionJavaReq{}
				tarnsJavaReq.TxId = txid
				tarnsJavaReq.ChainId = voteVal.chainId
				tarnsJavaReq.Ecode = code
				tarnsJavaReq.Emessage = msg
				ClientTransactionJavaReqChan <- tarnsJavaReq //传进通道 调用 response服务端方法
				//TxidsMap.Delete(txid)
				// 成功 删除缓存 放在了发送给java成功再删除
			}

		}
	} else {
		//每个节点投票只有一次机会 若缓存中不存在，则代表记录票数丢失，只能提示警告
		serviceLog.Warning("TaskEvent txid TxidsMap.Load(txid) fail", txid)
	}

}

// 定时清理map
func TaskClearMap() {
	for {
		ticker := time.NewTicker(time.Second * 60)

		for t := range ticker.C {
			TxidsSuccMap = &sync.Map{}
			t.Second()
			//serviceLog.Info("clear TxidsSuccMap", t.String())
		}
	}
}

// 定时清理数据库
func TaskClearDataBase(s *Server) {
	var (
		selectedb         int
		key_events_msg    []byte
		key_events_result []byte
		key_events_push   []byte
		txid              string
	)
	selectedb = s.ec.Config.SelectedDb
	tm := time.Now().UnixNano()
	tmstring := strconv.FormatInt(tm, 10)
	for {
		ticker := time.NewTicker(time.Hour * time.Duration(s.ec.Config.ClearDataInterval))
		for t := range ticker.C {
			if selectedb == db.Leveldbtyp {
				// 模糊查询（范围查询）
				iter := s.dh.Dblevel.NewIterator(&util.Range{Start: []byte(""), Limit: []byte(tmstring)}, nil)
				serviceLog.Warning("clear leveldb old data", time.Now().Second())
				// todo leveldb暂未实现清理逻辑
				for iter.Next() {
					txid = string(iter.Value())
					serviceLog.Infof("txid:", txid)
					key_events_msg = []byte(events_msg_name + txid)
					key_events_result = []byte(events_result_name + txid)
					key_events_push = []byte(events_push_name + txid)
					s.dh.Dblevel.Delete(key_events_msg, nil)
					s.dh.Dblevel.Delete(key_events_result, nil)
					s.dh.Dblevel.Delete(key_events_push, nil)
					// 最后删自己
					s.dh.Dblevel.Delete(iter.Key(), nil)
				}
			}
			if selectedb == db.Mysqltyp {
				sqldelEm := fmt.Sprintf("delete from %s where %s < '%d'", s.ec.Config.EventmsgtableName, ETIME, tm)
				serviceLog.Infof("delete table %s, sql is %s t: %s", s.ec.Config.EventmsgtableName, sqldelEm, t)
				_, err := s.dh.Dbmysql.Exec(sqldelEm)
				if err != nil {
					serviceLog.Errorf("delete db err:%s", err.Error())
				}
				sqldelRs := fmt.Sprintf("delete from %s where %s < '%d'", s.ec.Config.EventResultTableName, ETIME, tm)
				serviceLog.Infof("delete table %s, sql is %s", s.ec.Config.EventResultTableName, sqldelRs)
				_, err = s.dh.Dbmysql.Exec(sqldelRs)
				if err != nil {
					serviceLog.Errorf("delete db err:%s", err.Error())
				}
				sqldelPu := fmt.Sprintf("delete from %s where %s < '%d'", s.ec.Config.EventPushedTableName, ETIME, tm)
				serviceLog.Infof("delete table %s, sql is %s", s.ec.Config.EventPushedTableName, sqldelPu)
				_, err = s.dh.Dbmysql.Exec(sqldelPu)
				if err != nil {
					serviceLog.Errorf("delete db err:%s", err.Error())
				}
			}
			tm = time.Now().UnixNano()

		}
	}
}

func handleTaskByleveldb(er *events_result, s *Server) error {
	var (
		key                 []byte
		events_result_array []byte
		txid                string
		eve                 *events_result
		err                 error
	)
	txid = er.Txid
	key = []byte(events_result_name + txid)
	if events_result_array, err = json.Marshal(eve); err != nil {
		serviceLog.Errorf("json.Marshal(coll) [key] %s,[err] %s", key, err)
		return err
	}
	if err = s.dh.Dblevel.Put(key, events_result_array, nil); err != nil {
		serviceLog.Errorf("dblevel hash received [txid] %s but save events_result faild", txid)
		return err
	} else {
		serviceLog.Infof("dblevel hash received [txid] %s and save events_result success ", txid)
		return err
	}
	return err
}
func handleTaskByMysql(er *events_result, s *Server) error {
	var (
		txid  string
		code  string
		msg   string
		etime int64
	)
	txid = er.Txid
	code = er.Code
	msg = er.Message
	etime = er.Etime
	sqlSentence := fmt.Sprintf("insert into %s(%s,%s,%s,%s) "+
		"values",
		s.ec.Config.EventResultTableName,
		TXID,
		ECODE,
		EMESSAGE,
		ETIME,
	)
	//拼接sql
	sqlValue := fmt.Sprintf("('%s','%s','%s','%d')",
		txid,
		code,
		msg,
		etime,
	)
	sqlFinal := sqlSentence + sqlValue
	//写库
	serviceLog.Info("sqlFinal is ", sqlFinal)
	_, err := s.dh.Dbmysql.Exec(sqlFinal)
	if err != nil {
		/*ph.DataCacheMap.Delete(sc.DataHash)*/
		serviceLog.Error("txid write db failed", txid)
		return fmt.Errorf("txid write db failed", txid)
	}
	return err
}
