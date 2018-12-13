CREATE TABLE IF NOT EXISTS `events_result` (
  `ID` int NOT NULL AUTO_INCREMENT,
  `TXID` char(64) NOT NULL,
  `ECODE` char(64) NOT NULL,
  `EMESSAGE` VARCHAR(512) NOT NULL,
  `ETIME` bigint(20) NOT NULL,
  PRIMARY KEY (`ID`),
  KEY `TXID` (`TXID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;