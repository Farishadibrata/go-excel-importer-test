package main

import (
	"context"
	"fmt"
	"time"

	pgx "github.com/jackc/pgx/v5"
	_ "github.com/lib/pq"
	"github.com/xuri/excelize/v2"
)

// CREATE TABLE public.vehicleinfo (
// 	active varchar NULL,
// 	vehiclelicensenumber varchar(255) NULL,
// 	"name" varchar(255) NULL,
// 	licensetype varchar(255) NULL,
// 	expirationdate date NULL,
// 	permitlicensenumber varchar(255) NULL,
// 	dmvlicenseplatenumber varchar(255) NULL,
// 	vehiclevinnumber varchar(255) NULL,
// 	wheelchairaccessible varchar(255) NULL,
// 	certificationdate date NULL,
// 	hackupdate date NULL,
// 	vehicleyear varchar(255) NULL,
// 	basenumber varchar(255) NULL,
// 	basename varchar(255) NULL,
// 	basetype varchar(255) NULL,
// 	veh varchar(255) NULL,
// 	basetelephonenumber varchar(255) NULL,
// 	website varchar(255) NULL,
// 	baseaddress text NULL,
// 	reason text NULL,
// 	orderdate date NULL,
// 	lastdateupdated date NULL,
// 	id int4 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE) NOT NULL
// );

func getExcelFile(fileName string) *excelize.File {
	xlFile, xlErr := excelize.OpenFile(fileName)
	if xlErr != nil {
		panic(xlErr)
	}
	return xlFile
}

func RemoveIndex(s [][]any, index int) [][]any {
	return append(s[:index], s[index+1:]...)
}

func RemoveIndexA(s []any, index int) []any {
	return append(s[:index], s[index+1:]...)
}

func InsertPGSQL(ctx context.Context, rows [][]any) error {
	now := time.Now()

	db, err := pgx.Connect(ctx, "postgres://postgres:pgsqlpass@localhost:5432/pgtest?sslmode=disable")
	if err != nil {
		print("FAILED CONNECT GPSQL")
		return err
	}
	rows = RemoveIndex(rows, 0)
	tracker := 0
	num, errInsert := db.CopyFrom(ctx, pgx.Identifier{"vehicleinfo"}, []string{"active", "vehiclelicensenumber", "name", "licensetype", "expirationdate", "permitlicensenumber", "dmvlicenseplatenumber", "vehiclevinnumber", "wheelchairaccessible", "certificationdate", "hackupdate", "vehicleyear", "basenumber", "basename", "basetype", "veh", "basetelephonenumber", "website", "baseaddress", "reason", "orderdate", "lastdateupdated"}, pgx.CopyFromSlice(len(rows), func(i int) ([]interface{}, error) {
		data := rows[i]

		if data[9] == "" {
			data[9] = "2022-01-20"
		}
		if data[10] == "" {
			data[10] = "2022-01-20"
		}
		if data[20] == "" {
			data[20] = "2022-01-20"
		}
		if data[21] == "" {
			data[21] = "2022-01-20"
		}
		// nobody care about time. just put as single value
		data = RemoveIndexA(data, 22)
		return data, nil
	}))
	if errInsert != nil {
		println("ERROR PGSQL. ROW : ", tracker)
		println(string(errInsert.Error()))
		return err
	}
	defer func() {
		elapsed := time.Since(now)
		// Convert duration to seconds
		seconds := elapsed.Seconds()
		fmt.Printf("PGSQL Insert: %.2f seconds\n Over :%d Rows", seconds, num)
	}()

	return nil
}

func parseCellValue(value string) interface{} {
	if value == "" {
		return ""
	}

	if date, err := time.Parse("01/02/2006", value); err == nil {
		return date.Format("2006-01-02")
	}
	return value
}

func readExcel(xlFile *excelize.File, worksheet string) [][]any {
	now := time.Now()

	rows, err := xlFile.GetRows(worksheet)
	if err != nil {
		panic(err)
	}

	var result [][]any
	for _, row := range rows {
		var convertedRow []any
		for _, cell := range row {
			value := parseCellValue(cell)
			convertedRow = append(convertedRow, value)
		}
		result = append(result, convertedRow)
	}

	defer func() {
		elapsed := time.Since(now)
		// Convert duration to seconds
		seconds := elapsed.Seconds()
		fmt.Printf("Reading XLSX: %.2f seconds\n", seconds)
	}()
	return result
}

func main() {
	source := getExcelFile("source.xlsx")
	ctx := context.Background()
	worksheets := source.GetSheetList()
	rows := readExcel(source, worksheets[0])
	InsertPGSQL(ctx, rows)
}
