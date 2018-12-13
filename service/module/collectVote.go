package module

import (
	"fmt"

	sv "eventservice/example/serverproto"

	"io"

	"encoding/json"
	"eventservice/configMgr"
	"eventservice/db"
	"github.com/syndtr/goleveldb/leveldb"
	"strconv"
	"time"
)

type collectVote struct {
	txid    string
	code    string
	message string
	issucc  bool
	nodeid  string
	etime   int64
}
type events_result struct {
	Txid    string `json:"txid"`
	Code    string `json:"code"`
	Message string `json:"message"`
	Etime   int64  `json:"etime"`
}

const (
	getmsg  = 1
	votemsg = 2
)

/**
* @Title: service.go
* @Description: GoChainRequestCountEvent  uchains 交易统计阶段 收集votes
* @author ghc
* @date 9/28/18 17:59 PM
* @version V1.0
 */
func (s *Server) GoChainRequestCountEvent(stream sv.GoEventService_GoChainRequestCountEventServer) error {
	var (
		err error
		req *sv.ChainTranscationAccountReq
	)
	for {
		req, err = stream.Recv()
		if err == io.EOF {
			fmt.Println("read done")
			return nil
		}
		if err != nil {
			serviceLog.Error("Server GoChainRequestCountEvent Stream recv err", err)
			stream.Send(&sv.ChainTranscationAccountRes{TxIdRes: "", IsReceivedRes: false})
			return err
		}
		asc := &GoChainRequestCountAsc{}
		if req != nil {
			asc.req = req
			GoChainRequestCountAscChan <- asc
			if err = stream.Send(&sv.ChainTranscationAccountRes{req.TxIdReq, true}); err != nil {
				serviceLog.Error("GoChainRequestCountEvent stream.Send [err]", err)
				return err
			}
		}
	}
}

/**
* @Title: service.go
* @Description: GoChainRequestCountAscEvent  uchains 交易统计阶段 收集votes 单独处理
* @author ghc
* @date 9/28/18 17:59 PM
* @version V1.0
 */
func (s *Server) GoChainRequestCountAscEvent() error {
	var (
		asc       *GoChainRequestCountAsc
		selectedb int
		collvote  *collectVote
		err       error
		txidreq   string
		codereq   string
		messreq   string
		issuccreq bool
		nodeidreq string
		code      string
		msg       string
		succ      bool
	)
	selectedb = configMgr.SelectedDb
	for {
		select {
		case asc = <-GoChainRequestCountAscChan:
			serviceLog.Info("[len GoChainRequestCountAscChan]:", len(GoChainRequestCountAscChan))
			req := asc.req // req is nil 上一步已经判断
			txidreq = req.TxIdReq
			codereq = req.CodeReq
			messreq = req.MessageReq
			issuccreq = req.IsSuccessReq
			nodeidreq = req.NodeIdReq
			collvote = &collectVote{
				txidreq,
				codereq,
				messreq,
				issuccreq,
				nodeidreq,
				0,
			}
			t1 := time.Now()
			_, okk := TxidsSuccMap.Load(txidreq)
			if okk {
				serviceLog.Info("[txid hash handle over]:", txidreq)
				continue
			}
			value, ok := TxidsMap.Load(txidreq) //map 中不存在

			if !ok {
				if selectedb == db.Leveldbtyp {
					s.handle_eventsresult_ByLeveldb(asc, nil, getmsg)
				} else if selectedb == db.Mysqltyp {
					s.handle_eventsresult_ByMysql(asc, nil, getmsg)
				}
			} else {
				t3 := time.Now()
				etime := time.Now().UnixNano()
				voteVal := value.(*VoteAccount)

				totalNods := int32(voteVal.totalNodes)*1/3 + 1
				//todo 统计票数日志 适时删除
				if issuccreq {

					voteVal.votesSuccessMap[nodeidreq] = txidreq //nodeId 作为key 避免票数重复

					TxidsMap.Store(txidreq, voteVal)
					serviceLog.Info("[len(voteVal.votesSuccessMap)]:", int32(len(voteVal.votesSuccessMap)))
					serviceLog.Info("[voteVal.totalNodes*1/3]:", totalNods)
					serviceLog.Info("[voteVal.totalNodes]:", voteVal.totalNodes)

					voteAmount := int32(len(voteVal.votesSuccessMap))
					succ = voteAmount >= totalNods

					if succ {
						// 只要满足记账要求就发送
						if !voteVal.isUpdate {
							// 避免两票/三票重复发
							voteVal.isUpdate = true
							TxidsMap.Store(txidreq, voteVal)
							code = code1000
							msg = msg1000
							collvote = &collectVote{
								txidreq,
								code,
								msg,
								issuccreq,
								nodeidreq,
								etime,
							}
							if selectedb == db.Leveldbtyp {
								err = s.handle_eventsresult_ByLeveldb(nil, collvote, votemsg)
							}
							if selectedb == db.Mysqltyp {
								err = s.handle_eventsresult_ByMysql(nil, collvote, votemsg)
							}
							if err != nil {
								/*ph.DataCacheMap.Delete(sc.DataHash)*/
								serviceLog.Errorf("write db err:%s", err.Error())
							} else {
								TxidsSuccMap.Store(txidreq, txidreq)
								ok = voteVal.txtask.Stop() // 主动停掉定时任务
								serviceLog.Info("stop task ok:", ok)
								s.totalEventCountTxid++
								//TxidsMap.Delete(txidreq)
								tarnsJavaReq := &ClientTransactionJavaReq{}
								tarnsJavaReq.TxId = txidreq
								tarnsJavaReq.ChainId = voteVal.chainId
								tarnsJavaReq.Ecode = code
								tarnsJavaReq.Emessage = msg
								tarnsJavaReq.SendAmount = 0
								ClientTransactionJavaReqChan <- tarnsJavaReq
								serviceLog.Info("[GoJavaRequestCountEvent hash send txid]:", txidreq, msg)
								if err != nil {
									serviceLog.Error(txidreq + "Server Stream send fail")
									return err
								}
								t5 := time.Now()
								serviceLog.Info("[t5-t3]", t5.Sub(t3).String())
								serviceLog.Info("[t5-t1]", t5.Sub(t1).String())
							}
						} else {
							t4 := time.Now()
							serviceLog.Info("[t4-t3]", t4.Sub(t3).String())
						}

					}
				} else {
					voteVal.votesFailedMap[nodeidreq] = txidreq
					TxidsMap.Store(txidreq, voteVal)
					voteAmount := int32(len(voteVal.votesFailedMap))
					serviceLog.Info("[len(voteVal.votesFailedMap)]:", int32(len(voteVal.votesFailedMap)))
					serviceLog.Info("[voteVal.totalNodes*1/3]:", totalNods)
					serviceLog.Info("[voteVal.totalNodes]:", voteVal.totalNodes)
					fail := voteAmount >= totalNods
					if fail {
						// 只要满足记账要求就发送
						if !voteVal.isUpdate {
							// 避免两票/三票重复发
							voteVal.isUpdate = true
							code = codereq                //todo 这里的失败原因使用的uchains返回的
							msg = msg1002 + ":" + messreq //
							collvote = &collectVote{
								txidreq,
								code,
								msg,
								issuccreq,
								nodeidreq,
								etime,
							}
							if selectedb == db.Leveldbtyp {
								err = s.handle_eventsresult_ByLeveldb(nil, collvote, votemsg)
							}
							if selectedb == db.Mysqltyp {
								err = s.handle_eventsresult_ByMysql(nil, collvote, votemsg)
							}
							if err != nil {
								/*ph.DataCacheMap.Delete(sc.DataHash)*/
								serviceLog.Errorf("write db err:%s", err.Error())
							} else {
								voteVal.txtask.Stop() // 主动停掉定时任务
								s.totalEventCountTxid++
								tarnsJavaReq := &ClientTransactionJavaReq{}
								tarnsJavaReq.TxId = txidreq
								tarnsJavaReq.ChainId = voteVal.chainId
								tarnsJavaReq.Ecode = code
								tarnsJavaReq.Emessage = msg
								ClientTransactionJavaReqChan <- tarnsJavaReq //todo
							}
						}

					}
				}

			}
			t2 := time.Now()
			serviceLog.Info("t2-t1", t2.Sub(t1).String())
		}
	}

}

// leveldb handleVoteByLeveldb
func (s *Server) handle_eventsresult_ByLeveldb(asc *GoChainRequestCountAsc, coll *collectVote, msgtype int) error {
	var (
		key                 []byte
		events_result_array []byte
		err                 error
		txid                string
		eve                 *events_result
	)

	if msgtype == getmsg {
		txid = asc.req.TxIdReq
		key = []byte(events_msg_name + txid)
		if _, err = s.dh.Dblevel.Get(key, nil); err != nil {
			if err != leveldb.ErrNotFound {
				serviceLog.Warningf("s.dh.Dblevel.Get [key]:%s,[err],%s", txid, err)
				time.Sleep(1 * time.Second)
				if asc.count <= constSendAmount {
					asc.count++
					GoChainRequestCountAscChan <- asc
				}
				return err
			}
			time.Sleep(1 * time.Second)
			if asc.count <= constSendAmount {
				asc.count++
				GoChainRequestCountAscChan <- asc
			}
			return nil
		} else {
			serviceLog.Warningf("dblevel hash handle over or txid not exit [txid] %s", txid)
			return nil
		}
	}
	if msgtype == votemsg {
		txid = coll.txid
		key = []byte(events_result_name + txid)
		eve = &events_result{
			txid,
			coll.code,
			coll.message,
			coll.etime,
		}
		if events_result_array, err = json.Marshal(eve); err != nil {
			serviceLog.Errorf("json.Marshal(coll) [key] %s,[err] %s", key, err)
		}
		// 存储 events_result
		if err = s.dh.Dblevel.Put(key, events_result_array, nil); err != nil {
			serviceLog.Errorf("dblevel hash received [txid] %s but save events_result faild", txid)
		} else {
			serviceLog.Infof("dblevel hash received [txid] %s and save events_result success ", txid)
		}
		// 存储 key（时间）-value（txid）方便定时清除管理
		tmstring := strconv.FormatInt(coll.etime, 10)
		if err = s.dh.Dblevel.Put([]byte(tmstring), []byte(txid), nil); err != nil {
			serviceLog.Errorf("dblevel hash storage key[time]-value[txid] %s but save  faild", txid)
		}
	}
	return err
}

// mysql
func (s *Server) handle_eventsresult_ByMysql(asc *GoChainRequestCountAsc, coll *collectVote, msgtype int) error {
	var (
		coun int
		txid string
		err  error
	)

	if msgtype == getmsg {
		txid = asc.req.TxIdReq
		sqlFindRepeat := fmt.Sprintf("select  count(1) as coun from %s  where %s = '%s'",
			s.ec.Config.EventmsgtableName, TXID, txid)
		rows, err := s.dh.Dbmysql.Query(sqlFindRepeat) //查询去重
		serviceLog.Infof("s.dh.Db.Query(sqlFindRepeat) sql %s", sqlFindRepeat)
		if err != nil {
			serviceLog.Errorf("find s.ec.Config.EventmsgtableName findtxid err %s,txid %s", err, txid)
		}
		defer rows.Close()
		if rows != nil {
			for rows.Next() {
				err = rows.Scan(&coun)
				if err != nil {
					serviceLog.Errorf("rows.Scan(&coun) err %s txid %s ", err, txid)
				} // 没必要查出来继续放进缓存中 因为接下来处理完会马上删掉
			}
		}
		if coun != 0 {
			serviceLog.Warning(txid, "hash handle over or txid not exit")
		} else {
			time.Sleep(1 * time.Second)
			if asc.count <= constSendAmount {
				asc.count++
				GoChainRequestCountAscChan <- asc
			}
		}
	}
	if msgtype == votemsg {
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
			coll.txid,
			coll.code,
			coll.message,
			coll.etime,
		)
		sqlFinal := sqlSentence + sqlValue
		//写库
		serviceLog.Info("sqlFinal is ", sqlFinal)
		_, err = s.dh.Dbmysql.Exec(sqlFinal)
	}
	return err
}
