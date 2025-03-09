package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

// Config holds the database connection parameters
type Config struct {
	SourceDBHost     string
	SourceDBUser     string
	SourceDBName     string
	SourceDBPassword string
	TargetDBHost     string
	TargetDBUser     string
	TargetDBName     string
	TargetDBPassword string
	DBVendor         string
	UpdatedAtDate    string
	ExcludeColumns   string
}

func main() {
	// Parse command line arguments
	config := parseFlags()

	// Validate inputs
	if err := validateConfig(config); err != nil {
		log.Fatalf("Configuration error: %v", err)
	}

	// Process excluded columns
	excludedColumns := parseExcludedColumns(config.ExcludeColumns)

	// Connect to source and target databases
	sourceDB, err := connectToDatabase(config, true)
	if err != nil {
		log.Fatalf("Failed to connect to source database: %v", err)
	}

	targetDB, err := connectToDatabase(config, false)
	if err != nil {
		log.Fatalf("Failed to connect to target database: %v", err)
	}

	// Get all tables from source database
	tables, err := getTables(sourceDB, config.DBVendor)
	if err != nil {
		log.Fatalf("Failed to get tables: %v", err)
	}

	if len(tables) == 0 {
		log.Println("No tables found in the source database.")
		return
	}

	// Parse the updated_at cutoff date
	cutoffDate, err := time.Parse("2006-01-02", config.UpdatedAtDate)
	if err != nil {
		log.Fatalf("Invalid date format for updated_at_date. Please use YYYY-MM-DD format: %v", err)
	}

	// Process each table
	for _, tableName := range tables {
		log.Printf("Processing table: %s", tableName)
		err := syncTable(sourceDB, targetDB, tableName, cutoffDate, excludedColumns)
		if err != nil {
			log.Printf("Error syncing table %s: %v", tableName, err)
			continue
		}
	}

	log.Println("Sync completed successfully!")
}

func parseFlags() Config {
	config := Config{}

	flag.StringVar(&config.SourceDBHost, "source-db-host", "", "Source database host")
	flag.StringVar(&config.SourceDBUser, "source-db-user", "", "Source database user")
	flag.StringVar(&config.SourceDBName, "source-db-name", "", "Source database name")
	flag.StringVar(&config.SourceDBPassword, "source-db-password", "", "Source database password")
	flag.StringVar(&config.TargetDBHost, "target-db-host", "", "Target database host")
	flag.StringVar(&config.TargetDBUser, "target-db-user", "", "Target database user")
	flag.StringVar(&config.TargetDBName, "target-db-name", "", "Target database name")
	flag.StringVar(&config.TargetDBPassword, "target-db-password", "", "Target database password")
	flag.StringVar(&config.DBVendor, "db-vendor", "", "Database vendor (mysql or postgresql)")
	flag.StringVar(&config.UpdatedAtDate, "updated-at-date", "", "Date after which records should be synced (YYYY-MM-DD)")
	flag.StringVar(&config.ExcludeColumns, "exclude-columns", "", "Comma-separated list of columns to exclude from sync (e.g., 'created_at,deleted_at')")

	flag.Parse()

	return config
}

func validateConfig(config Config) error {
	if config.SourceDBHost == "" || config.SourceDBUser == "" || config.SourceDBName == "" {
		return fmt.Errorf("source database connection parameters are required")
	}
	if config.TargetDBHost == "" || config.TargetDBUser == "" || config.TargetDBName == "" {
		return fmt.Errorf("target database connection parameters are required")
	}
	if config.DBVendor != "mysql" && config.DBVendor != "postgresql" {
		return fmt.Errorf("db-vendor must be either 'mysql' or 'postgresql'")
	}
	if config.UpdatedAtDate == "" {
		return fmt.Errorf("updated-at-date is required in format YYYY-MM-DD")
	}
	return nil
}

func parseExcludedColumns(excludeColumnsStr string) map[string]bool {
	excludedColumns := make(map[string]bool)
	
	if excludeColumnsStr != "" {
		columns := strings.Split(excludeColumnsStr, ",")
		for _, col := range columns {
			trimmedCol := strings.TrimSpace(col)
			if trimmedCol != "" {
				excludedColumns[trimmedCol] = true
			}
		}
	}
	
	if len(excludedColumns) > 0 {
		log.Printf("Excluding columns from sync: %s", excludeColumnsStr)
	}
	
	return excludedColumns
}

func connectToDatabase(config Config, isSource bool) (*gorm.DB, error) {
	var dsn string
	var dialector gorm.Dialector

	host := config.TargetDBHost
	user := config.TargetDBUser
	dbName := config.TargetDBName
	password := config.TargetDBPassword

	if isSource {
		host = config.SourceDBHost
		user = config.SourceDBUser
		dbName = config.SourceDBName
		password = config.SourceDBPassword
	}

	switch config.DBVendor {
	case "mysql":
		dsn = fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=True&loc=Local", 
			user, password, host, dbName)
		dialector = mysql.Open(dsn)
	case "postgresql":
		dsn = fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable", 
			host, user, password, dbName)
		dialector = postgres.Open(dsn)
	default:
		return nil, fmt.Errorf("unsupported database vendor: %s", config.DBVendor)
	}

	dbType := "source"
	if !isSource {
		dbType = "target"
	}

	db, err := gorm.Open(dialector, &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true,
		},
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s database: %v", dbType, err)
	}

	log.Printf("Connected to %s database successfully", dbType)
	return db, nil
}

func getTables(db *gorm.DB, dbVendor string) ([]string, error) {
	var tables []string
	var query string

	if dbVendor == "mysql" {
		query = "SHOW TABLES"
	} else if dbVendor == "postgresql" {
		query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
	}

	rows, err := db.Raw(query).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		tables = append(tables, tableName)
	}

	return tables, nil
}

func syncTable(sourceDB, targetDB *gorm.DB, tableName string, cutoffDate time.Time, excludedColumns map[string]bool) error {
	// Check if table exists in target database
	hasTable := targetDB.Migrator().HasTable(tableName)
	if !hasTable {
		return fmt.Errorf("target table %s does not exist", tableName)
	}

	// Check if updated_at column exists in this table
	hasUpdatedAt := sourceDB.Migrator().HasColumn(tableName, "updated_at")
	if !hasUpdatedAt {
		log.Printf("Table %s does not have an updated_at column, skipping", tableName)
		return nil
	}

	// Get primary key columns
	primaryKeys, err := getPrimaryKeys(sourceDB, tableName)
	if err != nil {
		return fmt.Errorf("failed to get primary keys: %v", err)
	}

	if len(primaryKeys) == 0 {
		return fmt.Errorf("table %s has no primary key, cannot sync", tableName)
	}

	// Get all columns for the table
	columns, err := getTableColumns(sourceDB, tableName)
	if err != nil {
		return fmt.Errorf("failed to get table columns: %v", err)
	}

	// Convert cutoff date to string in database format
	cutoffDateStr := cutoffDate.Format("2006-01-02")

	// Get updated records from source
	var records []map[string]interface{}
	result := sourceDB.Table(tableName).
		Where("updated_at >= ?", cutoffDateStr).
		Find(&records)

	if result.Error != nil {
		return fmt.Errorf("failed to query source table: %v", result.Error)
	}

	// Process records in batches
	batchSize := 100
	totalRecords := len(records)
	log.Printf("Found %d records to sync in table %s", totalRecords, tableName)

	for i := 0; i < totalRecords; i += batchSize {
		end := i + batchSize
		if end > totalRecords {
			end = totalRecords
		}

		batch := records[i:end]
		err := syncBatch(targetDB, tableName, batch, primaryKeys, columns, excludedColumns)
		if err != nil {
			return fmt.Errorf("batch sync failed: %v", err)
		}

		log.Printf("Synced %d/%d records in table %s", end, totalRecords, tableName)
	}

	return nil
}

func getPrimaryKeys(db *gorm.DB, tableName string) ([]string, error) {
	var primaryKeys []string
	
	// Execute the appropriate SQL to get primary keys based on the database type
	var rows *sql.Rows
	var err error
	
	if db.Dialector.Name() == "mysql" {
		rows, err = db.Raw(`
			SELECT k.COLUMN_NAME
			FROM information_schema.table_constraints t
			JOIN information_schema.key_column_usage k
			USING(constraint_name,table_schema,table_name)
			WHERE t.constraint_type='PRIMARY KEY'
			AND t.table_name=?`, tableName).Rows()
	} else {
		// PostgreSQL
		rows, err = db.Raw(`
			SELECT a.attname
			FROM   pg_index i
			JOIN   pg_attribute a ON a.attrelid = i.indrelid
			AND a.attnum = ANY(i.indkey)
			WHERE  i.indrelid = ?::regclass
			AND    i.indisprimary`, tableName).Rows()
	}
		
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, err
		}
		primaryKeys = append(primaryKeys, columnName)
	}

	return primaryKeys, nil
}

func getTableColumns(db *gorm.DB, tableName string) ([]string, error) {
	var columns []string
	
	// Get database type from the GORM connection
	dbType := db.Dialector.Name()
	
	var rows *sql.Rows
	var err error
	
	if dbType == "mysql" {
		rows, err = db.Raw("SHOW COLUMNS FROM " + tableName).Rows()
	} else {
		// PostgreSQL
		rows, err = db.Raw(`
			SELECT column_name 
			FROM information_schema.columns 
			WHERE table_name = ?`, tableName).Rows()
	}
	
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var columnName string
		
		if dbType == "mysql" {
			var dummy1, dummy2, dummy3, dummy4, dummy5 interface{}
			if err := rows.Scan(&columnName, &dummy1, &dummy2, &dummy3, &dummy4, &dummy5); err != nil {
				return nil, err
			}
		} else {
			// PostgreSQL
			if err := rows.Scan(&columnName); err != nil {
				return nil, err
			}
		}
		
		columns = append(columns, columnName)
	}

	return columns, nil
}

func syncBatch(db *gorm.DB, tableName string, records []map[string]interface{}, primaryKeys, columns []string, excludedColumns map[string]bool) error {
	// Start a transaction
	tx := db.Begin()
	if tx.Error != nil {
		return tx.Error
	}

	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()

	for _, record := range records {
		// Build where clause for finding existing record by primary keys
		query := db.Table(tableName)
		for _, pk := range primaryKeys {
			if pkValue, exists := record[pk]; exists {
				query = query.Where(pk+" = ?", pkValue)
			}
		}

		// Check if record exists
		var exists bool
		var count int64
		if err := query.Count(&count).Error; err != nil {
			tx.Rollback()
			return err
		}
		exists = count > 0

		// Remove any keys that aren't in the table schema or are in excluded columns
		for key := range record {
			// Skip if column is excluded
			if excludedColumns[key] {
				delete(record, key)
				continue
			}
			
			// Skip if column doesn't exist in target
			found := false
			for _, col := range columns {
				if key == col {
					found = true
					break
				}
			}
			if !found {
				delete(record, key)
			}
		}

		// Insert or update record
		if exists {
			if err := db.Table(tableName).Where(buildPrimaryKeyCondition(primaryKeys, record)).Updates(record).Error; err != nil {
				tx.Rollback()
				return err
			}
		} else {
			if err := db.Table(tableName).Create(record).Error; err != nil {
				tx.Rollback()
				return err
			}
		}
	}

	return tx.Commit().Error
}

func buildPrimaryKeyCondition(primaryKeys []string, record map[string]interface{}) string {
	condition := ""
	for i, pk := range primaryKeys {
		if i > 0 {
			condition += " AND "
		}
		condition += pk + " = " + fmt.Sprintf("%v", record[pk])
	}
	return condition
}