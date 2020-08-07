package synchronizer

import (
	"fmt"
	"gravity-synchronizer/internal/projection"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/go-sql-driver/mysql"
	_ "github.com/godror/godror"
	_ "github.com/lib/pq"

	"github.com/jmoiron/sqlx"
)

const (
	DatabaseTypePostgres = iota
	DatabaseTypeMySQL
	DatabaseTypeMSSQL
	DatabaseTypeOracle
)

type RecordDef struct {
	HasPrimary    bool
	PrimaryColumn string
	Values        map[string]interface{}
	ColumnDefs    []*ColumnDef
}

type ColumnDef struct {
	ColumnName  string
	BindingName string
	Value       interface{}
}

type DBCommand struct {
	QueryStr string
	Args     map[string]interface{}
}

type Database struct {
	name     string
	dbType   int
	db       *sqlx.DB
	commands chan *DBCommand
}

func OpenDatabase(dbname string, info *DatabaseInfo) (*Database, error) {

	dbType := DatabaseTypePostgres

	var connStr string
	if info.Type == "mysql" {
		dbType = DatabaseTypeMySQL
		var params map[string]string

		if info.Secure {
			params["tls"] = "true"
		}

		config := mysql.Config{
			User:                 info.Username,
			Passwd:               info.Password,
			Addr:                 fmt.Sprintf("%s:%d", info.Host, info.Port),
			Net:                  "tcp",
			DBName:               info.DbName,
			AllowNativePasswords: true,
			Params:               params,
		}
		connStr = config.FormatDSN()
	} else {
		sslmode := "disable"
		if info.Secure {
			sslmode = "enable"
		}

		connStr = fmt.Sprintf(
			"%s://%s:%s@%s:%d/%s?sslmode=%s",
			info.Type,
			info.Username,
			info.Password,
			info.Host,
			info.Port,
			info.DbName,
			sslmode,
		)
	}

	log.WithFields(log.Fields{
		"uri": connStr,
	}).Info("Connecting to database...")

	// Open database
	db, err := sqlx.Open(info.Type, connStr)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	database := &Database{
		name:     dbname,
		dbType:   dbType,
		db:       db,
		commands: make(chan *DBCommand, 2048),
	}

	go database.run()

	return database, nil
}

func (database *Database) run() {
	for {
		select {
		case cmd := <-database.commands:
			database.db.NamedExec(cmd.QueryStr, cmd.Args)
		}
	}
}

func (database *Database) ProcessData(table string, sequence uint64, pj *projection.Projection) error {

	if pj.Method == "delete" {
		return database.DeleteRecord(table, sequence, pj)
	}

	return database.UpdateRecord(table, sequence, pj)
}

func (database *Database) GetDefinition(pj *projection.Projection) *RecordDef {

	recordDef := &RecordDef{
		HasPrimary: false,
		Values:     make(map[string]interface{}),
		ColumnDefs: make([]*ColumnDef, 0, len(pj.Fields)),
	}

	// Scanning fields
	for n, field := range pj.Fields {

		// Primary key
		if field.Primary == true {
			recordDef.Values["primary_val"] = field.Value
			recordDef.HasPrimary = true
			recordDef.PrimaryColumn = field.Name
			continue
		}

		// Generate binding name
		bindingName := fmt.Sprintf("val_%s", strconv.Itoa(n))
		recordDef.Values[bindingName] = field.Value

		// Store definition
		recordDef.ColumnDefs = append(recordDef.ColumnDefs, &ColumnDef{
			ColumnName:  field.Name,
			Value:       field.Name,
			BindingName: bindingName,
		})
	}

	return recordDef
}

func (database *Database) UpdateRecord(table string, sequence uint64, pj *projection.Projection) error {

	recordDef := database.GetDefinition(pj)

	// Ignore if no primary key
	if recordDef.HasPrimary == false {
		return nil
	}

	// TODO: performance issue because do twice for each record
	err := database.insert(table, recordDef)
	if err != nil {
		return err
	}
	_, err = database.update(table, recordDef)
	if err != nil {
		return err
	}

	return nil
}

func (database *Database) update(table string, recordDef *RecordDef) (bool, error) {

	// Preparing SQL string
	var updates []string
	var template string
	if database.dbType == DatabaseTypeMySQL {
		template = "UPDATE `%s` SET %s WHERE `%s` = :primary_val"
		for _, def := range recordDef.ColumnDefs {
			updates = append(updates, "`"+def.ColumnName+"` = :"+def.BindingName)
		}
	} else {
		template = `UPDATE "%s" SET %s WHERE "%s" = :primary_val`
		for _, def := range recordDef.ColumnDefs {
			updates = append(updates, `"`+def.ColumnName+`" = :`+def.BindingName)
		}
	}

	updateStr := strings.Join(updates, ",")
	sqlStr := fmt.Sprintf(template, table, updateStr, recordDef.PrimaryColumn)

	database.commands <- &DBCommand{
		QueryStr: sqlStr,
		Args:     recordDef.Values,
	}

	return false, nil
}

func (database *Database) insert(table string, recordDef *RecordDef) error {

	// Insert a new record
	colNames := []string{
		recordDef.PrimaryColumn,
	}
	valNames := []string{
		":primary_val",
	}

	// Preparing columns and bindings
	if database.dbType == DatabaseTypeMySQL {
		for _, def := range recordDef.ColumnDefs {
			colNames = append(colNames, "`"+def.ColumnName+"`")
			valNames = append(valNames, `:`+def.BindingName)
		}
	} else {
		for _, def := range recordDef.ColumnDefs {
			colNames = append(colNames, `"`+def.ColumnName+`"`)
			valNames = append(valNames, `:`+def.BindingName)
		}
	}

	var template string
	if database.dbType == DatabaseTypeMySQL {
		template = "INSERT INTO `%s` (%s) VALUES (%s)"
	} else {
		template = `INSERT INTO "%s" (%s) VALUES (%s)`
	}

	// Preparing SQL string to insert
	colsStr := strings.Join(colNames, ",")
	valsStr := strings.Join(valNames, ",")
	insertStr := fmt.Sprintf(template, table, colsStr, valsStr)

	//	database.db.NamedExec(insertStr, recordDef.Values)
	database.commands <- &DBCommand{
		QueryStr: insertStr,
		Args:     recordDef.Values,
	}

	return nil
}

func (database *Database) DeleteRecord(table string, sequence uint64, pj *projection.Projection) error {

	var template string
	if database.dbType == DatabaseTypeMySQL {
		template = "DELETE FROM `%s` WHERE `%s` = :primary_val"
	} else {
		template = `DELETE FROM "%s" WHERE "%s" = :primary_val`
	}

	for _, field := range pj.Fields {

		// Primary key
		if field.Primary == true {

			sqlStr := fmt.Sprintf(template, table, field.Name)
			database.commands <- &DBCommand{
				QueryStr: sqlStr,
				Args: map[string]interface{}{
					"primary_val": field.Value,
				},
			}

			break
		}
	}

	return nil
}

func (database *Database) Import(table string, data map[string]interface{}) error {

	colNames := make([]string, 0, len(data))
	valNames := make([]string, 0, len(data))
	// Preparing columns and bindings
	if database.dbType == DatabaseTypeMySQL {
		for colName, _ := range data {
			colNames = append(colNames, "`"+colName+"`")
			valNames = append(valNames, `:`+colName)
		}
	} else {
		for colName, _ := range data {
			colNames = append(colNames, `"`+colName+`"`)
			valNames = append(valNames, `:`+colName)
		}
	}

	colsStr := strings.Join(colNames, ",")
	valsStr := strings.Join(valNames, ",")

	var template string
	if database.dbType == DatabaseTypeMySQL {
		template = "INSERT INTO `%s` (%s) VALUES (%s)"
	} else {
		template = `INSERT INTO "%s" (%s) VALUES (%s)`
	}

	insertStr := fmt.Sprintf(template, table, colsStr, valsStr)
	_, err := database.db.NamedExec(insertStr, data)
	if err != nil {
		return err
	}

	return nil
}

func (database *Database) Truncate(table string) error {

	var template string
	if database.dbType == DatabaseTypeMySQL {
		template = "TRUNCATE TABLE `%s`"
	} else {
		template = `TRUNCATE TABLE "%s"`
	}

	sqlStr := fmt.Sprintf(template, table)
	_, err := database.db.Exec(sqlStr)
	if err != nil {
		return err
	}

	return nil
}
