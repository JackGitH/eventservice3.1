package eventstruct

import (
	"database/sql"
	"github.com/spf13/viper"
)

type EventStruct struct {
	*viper.Viper
	db *sql.DB
}
