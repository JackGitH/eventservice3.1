package module

import (
	"eventservice/configMgr"
	"eventservice/db"
	sv "eventservice/example/serverproto"
	"github.com/op/go-logging"
	"sync"
	"time"
)

const (
	events_msg_name            string = "events_msg"
	events_result_name         string = "events_result"
	events_client_address_name string = "events_client_address"
	events_push_name           string = "events_push_name"
)

var serviceLog = logging.MustGetLogger("serviceLog")

// 计数收集投票
var GoChainRequestCountAscChan chan *GoChainRequestCountAsc

// 异步处理投票
type GoChainRequestCountAsc struct {
	req   *sv.ChainTranscationAccountReq
	count int
}

// 更新推送状态
type UpdateIspushedsql struct {
	sql string
}

// 处理txid
var GoChainRequestReqAscChan chan *GoChainRequestReqAsc

// 异步处理txid
type GoChainRequestReqAsc struct {
	req *sv.ChainTranscationReq
}

// 缓存交易情况的map
var TxidsMap *sync.Map

// 缓存交易成功的map
var TxidsSuccMap *sync.Map

// 缓存注册 channel的map

var TchannelMap *sync.Map

// 缓存send java消息的stream 判断是不是重启java客户端 造成stream重复
var StreamMap *sync.Map

// 缓存交易情况的map 中的value
type VoteAccount struct {
	txid       string
	totalNodes int32
	//srsu            sync.RWMutex
	//srfa            sync.RWMutex
	votesSuccessMap map[string]string
	votesFailedMap  map[string]string
	txtask          *time.Timer
	chainId         string
	address         string
	isUpdate        bool
}

// 缓存ip地址对应的
var AddressMap *sync.Map

var AddressCount int

//
type ClientTransactionJavaReq struct {
	TxId              string
	Ecode             string
	Emessage          string
	ChainId           string
	Address           string
	SendAmount        int32
	DistinguishAmount int32
}

type ClientQuickReq struct {
	QuickSwitch  bool
	Address      string
	AddressCount int
	AddressMark  int64
}
type Password string
type MsgHandler interface {
	SendToJavaMsg(javaMsg *sv.ClientTransactionJavaReq) error
}

func (p Password) Redacted() interface{} {
	return logging.Redact(string(p))
}

//sdk请求txid 获得交易结果
var ClientTransactionJavaReqChan chan *ClientTransactionJavaReq

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
const (
	code1000 = "0000"
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
	msg1004 = "查询异常"
	msg1005 = "未注册，请求失败"
	msg1006 = "共识前检查异常" //BeforeConsCheck Fail
	msg1007 = "共识后检查异常" //AfterConsCheckAndUpdateData Fail

	msgRegist01 = "该ip-port已注册"
	msgRegist02 = "注册成功"
	msgRegist03 = "注册失败"
	msgRegist04 = "交易异常"

	constAmount      = 1 / 3 // 1/3容错
	constSendAmount  = 100   // 发送30次失败则不再发送
	constRetryAmount = 10
)

//server核心
type Server struct {
	ec                  *configMgr.EventConfig
	dh                  *db.DbHandler
	addressIdMap        map[string]string
	updateIspushedChan  chan *UpdateIspushedsql
	totalEventTxid      int32
	totalEventCountTxid int32
}
