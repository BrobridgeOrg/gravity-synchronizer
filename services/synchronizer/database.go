package synchronizer

import (
	"fmt"
	"gravity-synchronizer/internal/projection"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/godror/godror"
	_ "github.com/lib/pq"

	"github.com/jmoiron/sqlx"
)

type ColumnDef struct {
	ColumnName  string
	BindingName string
	Value       interface{}
}

type Database struct {
	name string
	db   *sqlx.DB
}

func OpenDatabase(dbname string, info *DatabaseInfo) (*Database, error) {

	sslmode := "disable"
	if info.Secure {
		sslmode = "enable"
	}

	var connStr string
	if info.Type == "mysql" {
		connStr = fmt.Sprintf(
			"%s://%s:%s@(%s:%d)/%s?sslmode=%s",
			info.Type,
			info.Username,
			info.Password,
			info.Host,
			info.Port,
			info.DbName,
			sslmode,
		)
	} else {
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

	return &Database{
		name: dbname,
		db:   db,
	}, nil
}

func (database *Database) ProcessData(table string, sequence uint64, pj *projection.Projection) error {

	if pj.Method == "delete" {
		return database.DeleteRecord(table, sequence, pj)
	}

	return database.UpdateRecord(table, sequence, pj)
}

func (database *Database) UpdateRecord(table string, sequence uint64, pj *projection.Projection) error {

	// Preparing SQL string
	var columnDefs []*ColumnDef
	var primaryColumn string
	hasPrimary := false
	values := make(map[string]interface{})
	for n, field := range pj.Fields {

		// Primary key
		if field.Primary == true {

			values["primary_val"] = field.Value

			hasPrimary = true
			primaryColumn = field.Name

			continue
		}

		idxStr := strconv.Itoa(n)

		valName := "val_" + idxStr
		values[valName] = field.Value

		columnDefs = append(columnDefs, &ColumnDef{
			ColumnName:  field.Name,
			Value:       field.Name,
			BindingName: "val_" + idxStr,
		})
	}

	// Ignore if no primary key
	if hasPrimary == false {
		return nil
	}

	// Preparing SQL string
	var updates []string
	for _, def := range columnDefs {
		updates = append(updates, `"`+def.ColumnName+`" = :`+def.BindingName)
	}

	updateStr := strings.Join(updates, ",")
	sqlStr := fmt.Sprintf(`UPDATE "%s" SET %s WHERE "%s" = :primary_val`, table, updateStr, primaryColumn)

	// Trying to update database
	result, err := database.db.NamedExec(sqlStr, values)
	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rows > 0 {
		return nil
	}

	// Insert a new record
	colNames := []string{
		primaryColumn,
	}
	valNames := []string{
		":primary_val",
	}

	for _, def := range columnDefs {
		colNames = append(colNames, `"`+def.ColumnName+`"`)
		valNames = append(valNames, `:`+def.BindingName)
	}

	colsStr := strings.Join(colNames, ",")
	valsStr := strings.Join(valNames, ",")

	insertStr := fmt.Sprintf(`INSERT INTO "%s" (%s) VALUES (%s)`, table, colsStr, valsStr)
	_, err = database.db.NamedExec(insertStr, values)
	if err != nil {
		return err
	}

	return nil
}

func (database *Database) Import(table string, data map[string]interface{}) error {

	colNames := make([]string, 0)
	valNames := make([]string, 0)
	for colName, _ := range data {
		colNames = append(colNames, `"`+colName+`"`)
		valNames = append(valNames, `:`+colName)
	}

	colsStr := strings.Join(colNames, ",")
	valsStr := strings.Join(valNames, ",")

	insertStr := fmt.Sprintf(`INSERT INTO "%s" (%s) VALUES (%s)`, table, colsStr, valsStr)
	_, err := database.db.NamedExec(insertStr, data)
	if err != nil {
		return err
	}

	return nil
}

func (database *Database) DeleteRecord(table string, sequence uint64, pj *projection.Projection) error {

	for _, field := range pj.Fields {

		// Primary key
		if field.Primary == true {

			sqlStr := fmt.Sprintf(`DELETE FROM "%s" WHERE "%s" = $1`, table, field.Name)
			_, err := database.db.Exec(sqlStr, field.Value)
			if err != nil {
				return err
			}

			break
		}
	}

	return nil
}

func (database *Database) Truncate(table string) error {

	sqlStr := fmt.Sprintf(`TRUNCATE TABLE "%s"`, table)
	_, err := database.db.Exec(sqlStr)
	if err != nil {
		return err
	}

	return nil
}
