package xugusql_driver

import (
        "database/sql"
        "fmt"
        "regexp"
        "strings"


	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

var regFullDataType = regexp.MustCompile(`\D*(\d+)\D?`)

var typeAliasMap = map[string][]string{
	"char": {"varchar"},
	"string":  {"varchar"},
	"bool":    {"boolen"},
	"tinyint": {"tinyint"},
	"uint": {"bigint"},
}

type Migrator struct {
	migrator.Migrator
	Dialector
}

/*func (m Migrator) FullDataTypeOf(field *schema.Field)  (expr clause.Expr) {
	expr.SQL = m.Migrator.DataTypeOf(field)

        if field.NotNull {
                expr.SQL += " NOT NULL"
        }

        if field.Unique {
                expr.SQL += " UNIQUE"
        }
        if field.HasDefaultValue && (field.DefaultValueInterface != nil || field.DefaultValue != "") {
		if field.DataType == "bool"{
			switch field.DefaultValue{
			case "0":
				expr.SQL += " DEFAULT False" 
			case "1":
				expr.SQL += " DEFAULT True"
			}
		} else if field.DefaultValue != "(-)" {
                        expr.SQL += " DEFAULT " + field.DefaultValue
                }
        }

	value1, _ := field.TagSettings["COMMENT"];
	
	if value, ok := field.TagSettings["COMMENT"]; ok {
		expr.SQL += " COMMENT " + m.Dialector.Explain("?", value)
	}
	return expr
}
*/




func (m Migrator) CurrentDatabase() (name string) {
	m.DB.Raw("select current_database").Row().Scan(&name)
        return
}


func (m Migrator) AlterColumnComment(value interface{}, field string) error {
        return m.RunWithValue(value, func(stmt *gorm.Statement) error {
                if field := stmt.Schema.LookUpField(field); field != nil {

			value, _ := field.TagSettings["COMMENT"];
			COMMENT_SQL := fmt.Sprintf("COMMENT ON COLUMN ?.? IS '%s'",value)
                        return m.DB.Exec(
                                COMMENT_SQL,
                                m.CurrentTable(stmt), clause.Column{Name: field.DBName}, 
                        ).Error

                }
                return fmt.Errorf("failed to look up field with name: %s", field)
        })
	
}


// AlterColumn alter value's `field` column' type based on schema definition
func (m Migrator) AlterColumn(value interface{}, field string) error {
        return m.RunWithValue(value, func(stmt *gorm.Statement) error {
                if field := stmt.Schema.LookUpField(field); field != nil {
                        fileType := m.FullDataTypeOf(field)
                        return m.DB.Exec(
                                "ALTER TABLE ? ALTER COLUMN ?  ?",
                                m.CurrentTable(stmt), clause.Column{Name: field.DBName}, fileType,
                        ).Error

                }
                return fmt.Errorf("failed to look up field with name: %s", field)
        })
}




func (m Migrator) RenameIndex(value interface{}, oldName, newName string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
                return m.DB.Exec(
                        "ALTER INDEX ?.? RENAME TO ?", // wat
                        clause.Table{Name: stmt.Table}, clause.Column{Name: oldName}, clause.Column{Name: newName},
                ).Error
        })

}



func (m Migrator) ColumnTypes(value interface{}) ([]gorm.ColumnType, error) {
        columnTypes := make([]gorm.ColumnType, 0)
        err := m.RunWithValue(value, func(stmt *gorm.Statement) error {
                var (
                        currentDatabase, table = m.CurrentSchema(stmt, stmt.Table)
                        columnTypeSQL          = "select a.col_name,a.def_val,b.cons_type,a.is_serial,a.comments from all_columns a left join all_constraints b on a.db_id = b.db_id and a.table_id = b.table_id  and b.define like '%'|a.col_name|'%'"
                        rows, err              = m.DB.Session(&gorm.Session{}).Table(table).Limit(1).Rows()
                )

                if err != nil {
                        return err
                }

                rawColumnTypes, err := rows.ColumnTypes()

                if err != nil {
                        return err
                }

                if err := rows.Close(); err != nil {
                        return err
                }

                columnTypeSQL += " where db_id = (select db_id from all_databases where db_name = ?)   and table_id =  (SELECT TABLE_ID  from all_tables where table_name = ?)"
		

                columns, rowErr := m.DB.Table(table).Raw(columnTypeSQL, currentDatabase, table).Rows()
                if rowErr != nil {
                        return rowErr
                }

                defer columns.Close()

                for columns.Next() {
                        var (
                                column            migrator.ColumnType
                                datetimePrecision sql.NullInt64
                                extraValue        sql.NullString
                                columnKey         sql.NullString
                                values            = []interface{}{
                                        &column.NameValue, &column.DefaultValueValue,  &columnKey, &extraValue, &column.CommentValue,
                                }
                        )


                        if scanErr := columns.Scan(values...); scanErr != nil {
                                return scanErr
                        }

                        column.PrimaryKeyValue = sql.NullBool{Bool: false, Valid: true}
                        column.UniqueValue = sql.NullBool{Bool: false, Valid: true}
                        switch columnKey.String {
                        case "P":
                                column.PrimaryKeyValue = sql.NullBool{Bool: true, Valid: true}
                        case "U":
                                column.UniqueValue = sql.NullBool{Bool: true, Valid: true}
                        }


			switch column.DefaultValueValue.String{
			case "CAST(FALSE AS BOOLEAN)":
				column.DefaultValueValue.String = "0"
			case "CAST(TRUE AS BOOLEAN)":
				column.DefaultValueValue.String = "1"
			}
                       
            
                        

                        column.DefaultValueValue.String = strings.Trim(column.DefaultValueValue.String, "'")

                        if datetimePrecision.Valid {
                                column.DecimalSizeValue = datetimePrecision
                        }

                        for _, c := range rawColumnTypes {
                                if c.Name() == column.NameValue.String {
                                        column.SQLColumnType = c
                                        break
                                }
                        }

                        columnTypes = append(columnTypes, column)
                }

                return nil
        })

        return columnTypes, err
}







// MigrateColumn migrate column
func (m Migrator) MigrateColumn(value interface{}, field *schema.Field, columnType gorm.ColumnType) error {
        // found, smart migrate
        fullDataType := strings.TrimSpace(strings.ToLower(m.FullDataTypeOf(field).SQL))
        realDataType := strings.ToLower(columnType.DatabaseTypeName())


        var (
                alterColumn bool
                isSameType  = fullDataType == realDataType
        )


        if !field.PrimaryKey {
                // check type
                if !strings.HasPrefix(fullDataType, realDataType) {
                        // check type aliases
                        aliases := m.GetTypeAliases(realDataType)
                        for _, alias := range aliases {
                                if strings.HasPrefix(fullDataType, alias) {
                                        isSameType = true
                                        break
                                }
                        }

                        if !isSameType {
                                alterColumn = true
                        }
                }
        }

        if !isSameType {
                // check size
                if length, ok := columnType.Length(); length != int64(field.Size) {
                        if length > 0 && field.Size > 0 {
                                alterColumn = true
                        } else {
                                // has size in data type and not equal
                                // Since the following code is frequently called in the for loop, reg optimization is needed here
                                matches2 := regFullDataType.FindAllStringSubmatch(fullDataType, -1)
                                if !field.PrimaryKey &&
                                        (len(matches2) == 1 && matches2[0][1] != fmt.Sprint(length) && ok) {
                                        alterColumn = true
                                }
                        }
                }

                // check precision
                if precision, _, ok := columnType.DecimalSize(); ok && int64(field.Precision) != precision {
                        if regexp.MustCompile(fmt.Sprintf("[^0-9]%d[^0-9]", field.Precision)).MatchString(m.Migrator.DataTypeOf(field)) {
                                alterColumn = true
                        }
                }
        }

        // check nullable
        if nullable, ok := columnType.Nullable(); ok && nullable == field.NotNull {
                // not primary key & database is nullable
                if !field.PrimaryKey && nullable {
                        alterColumn = true
                }
        }
        // check unique

        if unique, ok := columnType.Unique(); ok && unique != field.Unique {
                // not primary key
                if !field.PrimaryKey {
                        alterColumn = true
                }
        }
        // check default value
        if !field.PrimaryKey {
                currentDefaultNotNull := field.HasDefaultValue && (field.DefaultValueInterface != nil || !strings.EqualFold(field.DefaultValue, "NULL"))
                dv, dvNotNull := columnType.DefaultValue()
                if dvNotNull && !currentDefaultNotNull {
                        // default value -> null
                        alterColumn = true
                } else if !dvNotNull && currentDefaultNotNull {
                        // null -> default value
                        alterColumn = true
                } else if (field.GORMDataType != schema.Time && dv != field.DefaultValue) ||
                        (field.GORMDataType == schema.Time && !strings.EqualFold(strings.TrimSuffix(dv, "()"), strings.TrimSuffix(field.DefaultValue, "()"))) {
                        // default value not equal
                        // not both null
                        if currentDefaultNotNull || dvNotNull {
                                alterColumn = true
                        }
                }
        }

	if alterColumn && !field.IgnoreMigration {

                return m.AlterColumn(value, field.DBName)
        }


        // check comment
        if comment, ok := columnType.Comment(); ok && comment != field.Comment {
                // not primary key
                if !field.PrimaryKey {
                        alterColumn = true
                }
        }
        if alterColumn && !field.IgnoreMigration {

                return m.AlterColumnComment(value, field.DBName)
        }

        return nil
}





func (m Migrator) DropTable(values ...interface{}) error {
	values = m.ReorderModels(values, false)
	return m.DB.Connection(func(tx *gorm.DB) error {
		for i := len(values) - 1; i >= 0; i-- {
			if err := m.RunWithValue(values[i], func(stmt *gorm.Statement) error {
				return tx.Exec("DROP TABLE IF EXISTS ? CASCADE", clause.Table{Name: stmt.Table}).Error
			}); err != nil {
				return err
			}
		}
		return nil
	})
}

func (m Migrator) DropConstraint(value interface{}, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		constraint, chk, table := m.GuessConstraintAndTable(stmt, name)
		if chk != nil {
			return m.DB.Exec("ALTER TABLE ? DROP CONSTRAINT ?", clause.Table{Name: stmt.Table}, clause.Column{Name: chk.Name}).Error
		}
		if constraint != nil {
			name = constraint.Name
		}

		return m.DB.Exec(
			"ALTER TABLE ? DROP FOREIGN KEY ?", clause.Table{Name: table}, clause.Column{Name: name},
		).Error
	})
}



func (m Migrator) GetTables() (tableList []string, err error) {
	err = m.DB.Raw("select table_name from all_tables where db_id = (select db_id from all_databases where db_name = ? );", m.CurrentDatabase()).
		Scan(&tableList).Error
	return
}


func (m Migrator) HasTable(value interface{}) bool {
        var count int64

        m.RunWithValue(value, func(stmt *gorm.Statement) error {
                currentDatabase := m.DB.Migrator().CurrentDatabase()
                return m.DB.Raw(
			"SELECT count(*) FROM all_tables WHERE db_id  = (select db_id from all_databases where db_name = ?) AND table_name = ?", 
			currentDatabase, stmt.Table).Row().Scan(&count)
        })

        return count > 0
}




func (m Migrator) HasColumn(value interface{}, field string) bool {
        var count int64
        m.RunWithValue(value, func(stmt *gorm.Statement) error {
                currentDatabase := m.DB.Migrator().CurrentDatabase()
                name := field
                if field := stmt.Schema.LookUpField(field); field != nil {
                        name = field.DBName
                }

                return m.DB.Raw(
                        "select count(*) from all_columns where  db_id = (select db_id from all_databases where db_name = ?) and table_id = (select table_id from all_tables where db_id = (select db_id from all_databases where db_name =? ) and table_name = ?) and col_name = ?",
                        currentDatabase,currentDatabase, stmt.Table, name,
                ).Row().Scan(&count)
        })

        return count > 0
}


func (m Migrator) HasConstraint(value interface{}, name string) bool {
        var count int64
        m.RunWithValue(value, func(stmt *gorm.Statement) error {
                currentDatabase := m.DB.Migrator().CurrentDatabase()
                constraint, chk, table := m.GuessConstraintAndTable(stmt, name)
                if constraint != nil {
                        name = constraint.Name
                } else if chk != nil {
                        name = chk.Name
                }

                return m.DB.Raw(
                        "select count(*) from all_constraints where  db_id = (select db_id from all_databases where db_name = ?) and table_id = (select table_id from all_tables where db_id = (select db_id from all_databases where db_name =? ) and table_name = ?) and cons_name = ?",
			currentDatabase, table, name,
                ).Row().Scan(&count)
        })

        return count > 0
}



/*
func (m Migrator) GetIndexes(value interface{}) ([]gorm.Index, error) {
	indexes := make([]gorm.Index, 0)
	err := m.RunWithValue(value, func(stmt *gorm.Statement) error {

		result := make([]*Index, 0)
		schema, table := m.CurrentSchema(stmt, stmt.Table)
		scanErr := m.DB.Table(table).Raw(indexSql, schema, table).Scan(&result).Error
		if scanErr != nil {
			return scanErr
		}
		indexMap, indexNames := groupByIndexName(result)

		for _, name := range indexNames {
			idx := indexMap[name]
			if len(idx) == 0 {
				continue
			}
			tempIdx := &migrator.Index{
				TableName: idx[0].TableName,
				NameValue: idx[0].IndexName,
				PrimaryKeyValue: sql.NullBool{
					Bool:  idx[0].IndexName == "PRIMARY",
					Valid: true,
				},
				UniqueValue: sql.NullBool{
					Bool:  idx[0].NonUnique == 0,
					Valid: true,
				},
			}
			for _, x := range idx {
				tempIdx.ColumnList = append(tempIdx.ColumnList, x.ColumnName)
			}
			indexes = append(indexes, tempIdx)
		}
		return nil
	})
	return indexes, err
}

*/
// Index table index info
type Index struct {
	TableName  string `gorm:"column:TABLE_NAME"`
	ColumnName string `gorm:"column:COLUMN_NAME"`
	IndexName  string `gorm:"column:INDEX_NAME"`
	NonUnique  int32  `gorm:"column:NON_UNIQUE"`
}

func groupByIndexName(indexList []*Index) (map[string][]*Index, []string) {
	columnIndexMap := make(map[string][]*Index, len(indexList))
	indexNames := make([]string, 0, len(indexList))
	for _, idx := range indexList {
		if _, ok := columnIndexMap[idx.IndexName]; !ok {
			indexNames = append(indexNames, idx.IndexName)
		}
		columnIndexMap[idx.IndexName] = append(columnIndexMap[idx.IndexName], idx)
	}
	return columnIndexMap, indexNames
}

func (m Migrator) CurrentSchema(stmt *gorm.Statement, table string) (string, string) {
	if tables := strings.Split(table, `.`); len(tables) == 2 {
		return tables[0], tables[1]
	}
	m.DB = m.DB.Table(table)
	return m.CurrentDatabase(), table
}

func (m Migrator) GetTypeAliases(databaseTypeName string) []string {
	return typeAliasMap[databaseTypeName]
}


func (m Migrator) HasIndex(value interface{}, name string) bool {
        var count int64
        m.RunWithValue(value, func(stmt *gorm.Statement) error {
                currentDatabase := m.DB.Migrator().CurrentDatabase()
                if idx := stmt.Schema.LookIndex(name); idx != nil {
                        name = idx.Name
                }

                return m.DB.Raw(
                        "SELECT count(*) FROM all_indexes WHERE db_id = (select db_id from all_databases where db_name = ?) AND table_id = (select table_id from all_tables where table_name = ?) AND index_name = ?",
                        currentDatabase, stmt.Table, name,
                ).Row().Scan(&count)
        })

        return count > 0
}


