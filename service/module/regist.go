package module

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"eventservice/configMgr"
	"eventservice/db"
	sv "eventservice/example/serverproto"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"golang.org/x/net/context"
	"time"
)

type events_client_address struct {
	id        string
	eclientip string
	etime     int64
	remark    string
}

/**
* @Title: service.go
* @Description: GoClientRegistEvent  注册 该方法调用次数少 无须分离逻辑
* @author ghc
* @date 9/25/18 16:50 PM
* @version V1.0
 */
func (s *Server) GoClientRegistEvent(ctx context.Context, request *sv.ClientRegisterAddressReq) (*sv.ClientRegisterAddressRes, error) {
	var (
		ip        string
		eca       *events_client_address
		idStr     string
		id        []byte
		tm        int64
		remarkReq string
		ipPort    string
		selectedb int
	)
	selectedb = configMgr.SelectedDb
	ip = request.AddRessIpReq
	remarkReq = request.RemarkReq
	tm = time.Now().UnixNano()
	ipPort = ip
	//去重
	//给注册信息分配hash id
	Md5Inst := md5.New()
	Md5Inst.Write([]byte(ipPort))
	id = Md5Inst.Sum([]byte(""))
	idStr = hex.EncodeToString(id)

	eca = &events_client_address{
		idStr,
		ip,
		tm,
		remarkReq,
	}

	if selectedb == db.Mysqltyp {
		return s.handle_events_client_address_ByMysql(eca)
	}
	if selectedb == db.Leveldbtyp {
		return s.handle_events_client_address_Byleveldb(eca)
	}
	return nil, nil
}

// handle mysql
func (s *Server) handle_events_client_address_ByMysql(eca *events_client_address) (*sv.ClientRegisterAddressRes, error) {
	var (
		ip        string
		err       error
		sql       string
		idStr     string
		acount    int
		id        []byte
		tm        int64
		remarkReq string
		ipPort    string
	)
	ipPort = ip
	ip = eca.eclientip
	tm = eca.etime
	idStr = eca.id
	remarkReq = eca.remark

	sql = fmt.Sprintf("select count(*) as acount from %s where %s ='%s'",
		s.ec.Config.RegisterTableName, ECLIENTIP, ip)
	serviceLog.Info("findRepeat sql", sql)

	rows, err := s.dh.Dbmysql.Query(sql) //查询去重
	if err != nil {
		serviceLog.Error("findRepeat err", err)
		return s.handleRegeistErrMsg(msgRegist04, false, ""), err
	}
	defer rows.Close()
	if rows != nil {
		for rows.Next() {
			err = rows.Scan(&acount)
			if err != nil {
				return s.handleRegeistErrMsg(msgRegist04, false, ""), err
			}
		}
	} else {
		return s.handleRegeistErrMsg(msgRegist03, false, ""), nil
	}

	//去重
	//给注册信息分配hash id
	Md5Inst := md5.New()
	Md5Inst.Write([]byte(ipPort))
	id = Md5Inst.Sum([]byte(""))
	idStr = hex.EncodeToString(id)

	if acount == 0 {

		//拼接sql
		sqlValue := fmt.Sprintf("('%s','%s','%d','%s')",
			idStr,
			ip,
			tm,
			remarkReq,
		)
		sqlSentence := fmt.Sprintf("insert into %s(%s,%s,%s,%s) "+
			"values",
			s.ec.Config.RegisterTableName,
			ID,
			ECLIENTIP,
			ETIME,
			REMARK,
		)
		sqlFinal := sqlSentence + sqlValue

		//写库
		serviceLog.Info("sqlFinal is ", sqlFinal)
		_, err = s.dh.Dbmysql.Exec(sqlFinal)
		if err != nil {
			/*ph.DataCacheMap.Delete(sc.DataHash)*/
			serviceLog.Errorf("write db err:%s", err.Error())
		}
		// 缓存ip 对应地址 推送消息时使用
		AddressMap.Store(idStr, ip)
		s.addressIdMap[idStr] = ipPort //放在缓存中
		tchan := make(chan *ClientTransactionJavaReq, 1000000)
		_, ok := TchannelMap.Load(ip)
		if !ok {
			TchannelMap.Store(ip, tchan)
		}
		return s.handleRegeistErrMsg(msgRegist02, true, idStr), nil
	} else {
		tchan := make(chan *ClientTransactionJavaReq, 1000000)
		_, ok := TchannelMap.Load(ip)
		if !ok {
			TchannelMap.Store(ip, tchan)
		}
		return s.handleRegeistErrMsg(msgRegist01, false, idStr), nil
	}
}

// handle leveldb
func (s *Server) handle_events_client_address_Byleveldb(eca *events_client_address) (*sv.ClientRegisterAddressRes, error) {
	var (
		key           []byte
		idStr         string
		indextabArray []byte
		ip            string
		err           error
	)
	idStr = eca.id
	ip = eca.eclientip
	key = []byte(events_client_address_name + ip)
	if _, err = s.dh.Dblevel.Get(key, nil); err != nil {
		if err != leveldb.ErrNotFound {
			serviceLog.Warningf("s.dh.Dblevel.Get [key]:%s,[err],%s", ip, err)
			return nil, err
		}
		if indextabArray, err = json.Marshal(eca); err != nil {
			serviceLog.Errorf("json.Marshal(eca) [key] %s,[err] %s", ip, err)
		}
		if err = s.dh.Dblevel.Put([]byte(idStr), []byte(ip), nil); err != nil {
			serviceLog.Errorf("dblevel.Put [key] %s,[err] %s", idStr, err)
		} else {
			serviceLog.Infof("dblevel.Put []byte(idStr), []byte(ip) [ip] %s success", ip)
		}
		if err = s.dh.Dblevel.Put(key, indextabArray, nil); err != nil {
			serviceLog.Errorf("dblevel.Put [key] %s,[err] %s", key, err)
		} else {
			serviceLog.Infof("注册成功 [ip] %s success", ip)
		}
		AddressMap.Store(idStr, ip)
		s.addressIdMap[idStr] = ip //放在缓存中
		tchan := make(chan *ClientTransactionJavaReq, 1000000)
		_, ok := TchannelMap.Load(ip)
		if !ok {
			TchannelMap.Store(ip, tchan)
		}
		return s.handleRegeistErrMsg(msgRegist02, true, idStr), nil
	} else {
		tchan := make(chan *ClientTransactionJavaReq, 1000000)
		_, ok := TchannelMap.Load(ip)
		if !ok {
			TchannelMap.Store(ip, tchan)
		}
		return s.handleRegeistErrMsg(msgRegist01, false, idStr), nil
	}

}

// handle err msg
func (s *Server) handleRegeistErrMsg(code string, success bool, msg string) *sv.ClientRegisterAddressRes {
	scc := &sv.ClientRegisterAddressRes{
		code,
		success,
		msg,
	}
	return scc
}
