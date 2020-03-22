package synchronizer

import (
	"fmt"
	"gravity-synchronizer/internal/projection"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"

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

	connStr := fmt.Sprintf(
		"%s://%s:%s@%s:%d/%s?sslmode=%s",
		info.Type,
		info.Username,
		info.Password,
		info.Host,
		info.Port,
		info.DbName,
		sslmode,
	)

	log.Info("Connect to database: " + connStr)

	// Open database
	db, err := sqlx.Open("postgres", connStr)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	return &Database{
		name: dbname,
		db:   db,
	}, nil
}

func (database *Database) ProcessData(sequence uint64, pj *projection.Projection) error {

	if pj.Method == "delete" {
		return database.DeleteRecord(sequence, pj)
	}

	return database.UpdateRecord(sequence, pj)
}

func (database *Database) UpdateRecord(sequence uint64, pj *projection.Projection) error {

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
	sqlStr := fmt.Sprintf(`UPDATE "%s" SET %s WHERE "%s" = :primary_val`, pj.Table, updateStr, primaryColumn)

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

	insertStr := fmt.Sprintf(`INSERT INTO "%s" (%s) VALUES (%s)`, pj.Table, colsStr, valsStr)
	_, err = database.db.NamedExec(insertStr, values)
	if err != nil {
		return err
	}

	return nil
}

func (database *Database) DeleteRecord(sequence uint64, pj *projection.Projection) error {

	for _, field := range pj.Fields {

		// Primary key
		if field.Primary == true {

			sqlStr := fmt.Sprintf(`DELETE FROM "%s" WHERE "%s" = $1`, pj.Table, field.Name)
			_, err := database.db.Exec(sqlStr, field.Value)
			if err != nil {
				return err
			}

			break
		}
	}

	return nil
}
