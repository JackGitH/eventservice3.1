package module

import (
	"fmt"

	"golang.org/x/net/context"

	"database/sql"
	"encoding/json"
	"eventservice/configMgr"
	"eventservice/db"
	sv "eventservice/example/serverproto"
	"github.com/syndtr/goleveldb/leveldb"
	"strconv"
)

/**
* @Title: service.go
* @Description: GoClientRequestEvent  处理客户端请求txid 该方法调用次数少 无须分离逻辑
* @author ghc
* @date 9/25/18 16:50 PM
* @version V1.0
 */
func (s *Server) GoClientRequestEvent(ctx context.Context, request *sv.ClientTransactionReq) (*sv.ClientTransactionRes, error) {
	var (
		selectedDb int
	)
	selectedDb = configMgr.SelectedDb

	addressId := request.AddressIdReq

	cap, ok := s.addressIdMap[addressId] //先判断是否注册
	serviceLog.Info("GoClientRequestEvent s.addressIdMap", s.addressIdMap)
	if selectedDb == db.Mysqltyp {
		return s.handleRequestByMysql(cap, ok, request)
	}
	if selectedDb == db.Leveldbtyp {
		return s.handleRequestByleveldb(cap, ok, request)
	}
	return handleReturnMsg(request.TxIdReq, code1003, "", "", nil)
}

func handleReturnMsg(txid string, code string, timeres string, chainid string, err error) (*sv.ClientTransactionRes, error) {
	if code == code1004 {
		return &sv.ClientTransactionRes{TxIdRes: txid, CodeRes: code1004, MessageRes: msg1004, TimeRes: "", ChainIdRes: ""}, err
	} else if code == code1001 {
		return &sv.ClientTransactionRes{TxIdRes: txid, CodeRes: code1001, MessageRes: msg1001, TimeRes: timeres, ChainIdRes: chainid}, err
	} else if code == code1005 {
		return &sv.ClientTransactionRes{TxIdRes: txid, CodeRes: code1005, MessageRes: msg1005, TimeRes: "", ChainIdRes: ""}, nil
	} else if code == code1003 {
		return &sv.ClientTransactionRes{TxIdRes: txid, CodeRes: code1003, MessageRes: msg1003, TimeRes: "", ChainIdRes: ""}, err
	} else {
		return nil, err
	}
}

func (s *Server) handleRequestByMysql(cap string, ok bool, request *sv.ClientTransactionReq) (*sv.ClientTransactionRes, error) {
	var (
		sql1      string
		sql2      string
		err       error
		err1      error
		rows      *sql.Rows
		err2      error
		rows2     *sql.Rows
		rows1     *sql.Rows
		ipstr     string
		txid      string
		txidr     string
		ecode     string
		emessage  string
		etimer    string
		chainIdr  string
		addressId string
		chainId   string
	)
	addressId = request.AddressIdReq
	chainId = request.ChainIdReq
	txid = request.TxIdReq
	if !ok {
		sql1 = fmt.Sprintf("select %s from %s where %s = '%s' ",
			ECLIENTIP, s.ec.Config.RegisterTableName, ID, addressId)
		rows, err = s.dh.Dbmysql.Query(sql1) //查询去重
		if err != nil {
			serviceLog.Error("GoClientRequestEvent err", err)
			return handleReturnMsg(txid, code1004, "", "", err)
		}
		defer rows.Close()
		if rows != nil {
			for rows.Next() {
				err = rows.Scan(&ipstr)
				if err != nil {
					fmt.Println("GoClientRequestEvent err", err)
					serviceLog.Error("GoClientRequestEvent err", err)
					return handleReturnMsg(txid, code1004, "", "", err)
				}
			}
		}
		if ipstr == "" {
			serviceLog.Info("addressId Non-existent cap:", cap, "addressId Non-existent", addressId)
			return handleReturnMsg(txid, code1005, "", "", nil)
		}
	}

	sql1 = fmt.Sprintf("SELECT m.txid,  m.ETIME, m.CHAINID FROM events_msg m  WHERE m.txid = '%s' and m.CHAINID= '%s'",
		txid, chainId)

	sql2 = fmt.Sprintf("SELECT  r.ECODE, r.EMESSAGE  FROM  events_result r WHERE r.txid = '%s' ",
		txid)
	serviceLog.Info("RequestEvent sql", sql1)
	serviceLog.Info("RequestEvent sql", sql2)

	rows1, err1 = s.dh.Dbmysql.Query(sql1) //查询去重
	if err1 != nil {
		serviceLog.Error("GoClientRequestEvent err", err1)
		return handleReturnMsg(txid, code1004, "", "", err1)
	}
	rows2, err2 = s.dh.Dbmysql.Query(sql2) //查询去重
	if err2 != nil {
		serviceLog.Error("GoClientRequestEvent err", err2)
		return handleReturnMsg(txid, code1004, "", "", err2)
	}

	defer rows1.Close()
	defer rows2.Close()
	if rows1 != nil {
		for rows1.Next() {
			err := rows1.Scan(&txidr, &etimer, &chainIdr)
			if err != nil {
				fmt.Println("GoClientRequestEvent err", err)
				serviceLog.Error("GoClientRequestEvent err", err)
				return handleReturnMsg(txid, code1004, "", "", err)
			}
		}
		if rows2 != nil {
			for rows2.Next() {
				err := rows2.Scan(&ecode, &emessage)
				if err != nil {
					fmt.Println("GoClientRequestEvent err", err)
					serviceLog.Error("GoClientRequestEvent err", err)
					return handleReturnMsg(txid, code1004, "", "", err)
				}
			}
		}
		serviceLog.Info("txidr:", txidr, "---ecoder:", ecode, "---emessager:", emessage, "---etimer:", etimer, "---chainIdr:", chainIdr)
		if ecode == "" {
			return handleReturnMsg(txidr, code1001, etimer, chainIdr, nil)
		} else {
			return &sv.ClientTransactionRes{TxIdRes: txidr, CodeRes: ecode, MessageRes: emessage, TimeRes: etimer, ChainIdRes: chainIdr}, nil
		}
	} else {
		return handleReturnMsg(txid, code1003, "", "", nil)
	}
	return handleReturnMsg(txid, code1003, "", "", nil)
}

func (s *Server) handleRequestByleveldb(cap string, ok bool, request *sv.ClientTransactionReq) (*sv.ClientTransactionRes, error) {
	var (
		key        []byte
		val        []byte
		ipstr      string
		addressId  string
		chainId    string
		txid       string
		err        error
		txidresult *events_msg
		eve        *events_result
		etime      int64
		etimestr   string
		ecode      string
		emessage   string
	)
	addressId = request.AddressIdReq
	chainId = request.ChainIdReq
	txid = request.TxIdReq
	addressId = request.AddressIdReq
	txidresult = &events_msg{}
	eve = &events_result{}
	if !ok {
		if val, err = s.dh.Dblevel.Get([]byte(addressId), nil); err != nil {
			if err != leveldb.ErrNotFound {
				serviceLog.Warningf("s.dh.Dblevel.Get [key]:%s,[err],%s", addressId, err)
			}
		}
		ipstr = string(val)
		if ipstr == "" {
			serviceLog.Info("addressId Non-existent cap:", cap, "addressId Non-existent", addressId)
			return handleReturnMsg(txid, code1005, "", "", nil)
		}
	}
	// select events_msg
	key = []byte(events_msg_name + txid)
	if val, err = s.dh.Dblevel.Get(key, nil); err != nil {
		serviceLog.Warningf("s.dh.Dblevel.Get [key]:%s,[err],%s", events_msg_name+txid, err)
		serviceLog.Error("GoClientRequestEvent err", err)
		return handleReturnMsg(txid, code1004, "", "", err)
	}
	if err = json.Unmarshal(val, txidresult); err != nil {
		serviceLog.Error("json.Unmarshal(val,txidresult)[txid]%s err %s", txid, err)
		return handleReturnMsg(txid, code1004, "", "", err)
	}
	etime = txidresult.Etime
	chainId = txidresult.ChainId
	//select events_result
	key = []byte(events_result_name + txid)
	if val, err = s.dh.Dblevel.Get(key, nil); err != nil {
		serviceLog.Warningf("s.dh.Dblevel.Get [key]:%s,[err],%s", events_result_name+txid, err)
		serviceLog.Error("GoClientRequestEvent err", err)
		return handleReturnMsg(txid, code1004, "", "", err)
	}
	if err = json.Unmarshal(val, eve); err != nil {
		serviceLog.Error("json.Unmarshal(val,txidresult)[txid]%s err %s", txid, err)
		return handleReturnMsg(txid, code1004, "", "", err)
	}
	ecode = eve.Code
	emessage = eve.Message
	etimestr = strconv.FormatInt(etime, 10)
	serviceLog.Info("txidr:", txid, "---ecoder:", ecode, "---emessager:", emessage, "---etimer:", etime, "---chainIdr:", chainId)
	// return
	if ecode == "" {
		return handleReturnMsg(txid, code1001, etimestr, chainId, nil)
	} else {
		return &sv.ClientTransactionRes{TxIdRes: txid, CodeRes: ecode, MessageRes: emessage, TimeRes: etimestr, ChainIdRes: chainId}, nil
	}

}
