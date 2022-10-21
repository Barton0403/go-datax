package main

import (
	"bytes"
	"context"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/viper"
	"io/ioutil"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"text/template"
)

type table struct {
	Pk      string
	Columns []string
}

type incrementTable struct {
	UpdateDateField string
	Where           string
}

func getTables() map[string]table {
	db, err := sql.Open("mysql", sourceDb.getDataSourceName())
	if err != nil {
		panic(err)
	}
	defer db.Close()

	db.SetConnMaxLifetime(0)
	db.SetMaxIdleConns(3)
	db.SetMaxOpenConns(3)

	tables := map[string]table{}

	rows, e := db.QueryContext(context.Background(), fmt.Sprintf(`
select a.TABLE_NAME,a.COLUMN_NAME
from information_schema.STATISTICS a 
left join information_schema.COLUMNS b on b.COLUMN_NAME=a.COLUMN_NAME and b.TABLE_NAME=a.TABLE_NAME
where a.TABLE_SCHEMA='%s' and a.INDEX_NAME='PRIMARY' and b.COLUMN_TYPE like '%%int%%' and a.TABLE_NAME not in ("%s")
`, sourceDb.Database, strings.Join(ignoreTables, "\",\"")))
	if e != nil {
		panic(e)
	}
	for rows.Next() {
		var tableName string
		var columnName string
		if e = rows.Scan(&tableName, &columnName); e != nil {
			panic(e)
		}

		t := table{Pk: columnName}

		if increment {
		}

		tables[tableName] = t
	}

	rows, e = db.QueryContext(context.Background(), fmt.Sprintf(`
select a.TABLE_NAME,a.COLUMN_NAME
from information_schema.COLUMNS a
left join information_schema.TABLES b on b.TABLE_SCHEMA=a.TABLE_SCHEMA and a.TABLE_NAME=b.TABLE_NAME
where a.TABLE_SCHEMA='%s' and b.TABLE_TYPE='BASE TABLE' and a.TABLE_NAME not in ("%s")
`, sourceDb.Database, strings.Join(ignoreTables, "\",\"")))
	if e != nil {
		panic(e)
	}
	for rows.Next() {
		var tableName string
		var columnName string
		if e = rows.Scan(&tableName, &columnName); e != nil {
			panic(e)
		}

		t := tables[tableName]
		t.Columns = append(t.Columns, columnName)
		tables[tableName] = t
	}

	return tables
}

type Job struct {
	Pk            string
	TableName     string
	ColumnNames   []string
	TargetDb      Db
	TargetJdbcUrl string
	SourceDb      Db
	SourceJdbcUrl string
	Where         string
}

type Db struct {
	Username string
	Password string
	Database string
	Host     string
	Port     string
}

// root:root@tcp(127.0.0.1:3307)/dbname
func (d *Db) getDataSourceName() string {
	return d.Username + ":" + d.Password + "@tcp(" + d.Host + ":" + d.Port + ")/" + d.Database
}

// jdbc:mysql://127.0.0.1:3307/dbname?useSSL=false&useUnicode=true&characterEncoding=utf8
func (d *Db) getJdbcUrl() string {
	return "jdbc:mysql://" + d.Host + ":" + d.Port + "/" + d.Database + "?useSSL=false&useUnicode=true&characterEncoding=utf8"
}

func generateJob() {
	var content []byte
	if increment {
		content, _ = os.ReadFile("./job_schema_increment.template")
	} else {
		content, _ = os.ReadFile("./job_schema.template")
	}

	tmpl, e := template.New("job").Funcs(template.FuncMap{"join": strings.Join}).Parse(string(content))
	if e != nil {
		panic(e)
	}

	tables := getTables()

	//loop:
	for name, table := range tables {
		job := Job{
			Pk:            table.Pk,
			TableName:     name,
			ColumnNames:   table.Columns,
			SourceDb:      sourceDb,
			SourceJdbcUrl: sourceDb.getJdbcUrl(),
			TargetDb:      targetDb,
			TargetJdbcUrl: targetDb.getJdbcUrl(),
		}
		var filename string
		if increment {
			if incrementTables[name].Where == "" {
				filename = "./" + outdir + "/job_" + name + ".json"
			} else {
				// 增量同步
				job.Where = incrementTables[name].Where
				filename = "./" + outdir + "/job_increment_" + name + ".json"
			}
		} else {
			filename = "./" + outdir + "/job_" + name + ".json"
		}

		_, e = os.Stat(filename)
		if e == nil || !os.IsNotExist(e) {
			fmt.Println(filename + " exist")
			continue
		}

		file, e := os.Create(filename)
		if e != nil {
			panic(e)
		}
		e = tmpl.Execute(file, job)
		if e != nil {
			panic(e)
		}
		file.Close()
		fmt.Println(filename + " generate")
	}
}

var (
	help bool

	generateCmd *flag.FlagSet
	outdir      string
	increment   bool
	startDate   string
	endDate     string

	sourceDb        Db
	targetDb        Db
	ignoreTables    []string
	incrementTables map[string]incrementTable

	runCmd *flag.FlagSet
	jobId  int
	mode   string

	defaultJvm    string
	engineCommand string
)

type JavaCommandMap struct {
	Jvm    string
	Params string
	Mode   string
	JobId  int
	Job    string
}

func buildJavaArgs(jobFilename string) []string {
	jvm := defaultJvm

	start := 0
	if len(jobFilename) > 20 {
		start = len(jobFilename) - 20
	}
	t := strings.Replace(jobFilename[start:], "/", "_", -1)
	t = strings.Replace(t, ".", "_", -1)
	jobParams := fmt.Sprintf("-Dlog.file.name=%s", t)

	commandMap := JavaCommandMap{
		Jvm:    jvm,
		Job:    jobFilename,
		Params: jobParams,
		JobId:  jobId,
		Mode:   mode,
	}
	tmpl, _ := template.New("command").Parse(engineCommand)
	buf := new(bytes.Buffer)
	tmpl.Execute(buf, commandMap)

	return strings.Split(buf.String(), " ")
}

func GetAllFile(pathname string) (s []string, err error) {
	rd, err := ioutil.ReadDir(pathname)
	if err != nil {
		fmt.Println("read dir fail:", err)
		return s, err
	}

	for _, fi := range rd {
		if !fi.IsDir() {
			fullName := pathname + "/" + fi.Name()
			s = append(s, fullName)
		}
	}
	return s, nil
}

func init() {
	flag.BoolVar(&help, "help", false, "this help")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `go-datax is a data sync tool
Usage: go-datax <command> [arguments]
The commands are:
  run           run job file
  generate      generate job file
`)
	}
	generateCmd = flag.NewFlagSet("generate", flag.ExitOnError)
	generateCmd.StringVar(&outdir, "jobdir", "job", "Job file dir")
	generateCmd.BoolVar(&increment, "increment", false, "increment job")
	generateCmd.StringVar(&startDate, "startdate", "", "increment start date")
	generateCmd.StringVar(&endDate, "enddate", "", "increment end date")
	generateCmd.BoolVar(&help, "help", false, "this help")
	generateCmd.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: go-datax generate [arguments]
The arguments are:
`)
		generateCmd.PrintDefaults()
	}
	runCmd = flag.NewFlagSet("run", flag.ExitOnError)
	runCmd.IntVar(&jobId, "jobid", -1, "Set job unique id when running by Distribute/Local Mode.")
	runCmd.StringVar(&mode, "mode", "standalone", "Set job runtime mode such as: standalone, local, distribute. Default mode is standalone.")
	runCmd.StringVar(&outdir, "jobdir", "job", "Job file dir when run all")
	runCmd.BoolVar(&help, "help", false, "this help")
	runCmd.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: go-datax run [arguments]
The arguments are:
`)
		runCmd.PrintDefaults()
	}
}

func main() {
	pwd, _ := os.Getwd()
	dataxHome := pwd + "/datax"
	var classPath string
	if runtime.GOOS == "windows" {
		//python codecs.register(lambda name: name == 'cp65001' and codecs.lookup('utf-8') or None)
		classPath = fmt.Sprintf("%s/lib/*", dataxHome)
	} else {
		classPath = fmt.Sprintf("%s/lib/*:.", dataxHome)
	}
	logbackFile := fmt.Sprintf("%s/conf/logback.xml", dataxHome)
	defaultJvm = fmt.Sprintf("-Xms1g -Xmx1g -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=%s/log", dataxHome)
	defaultPropertyConf := fmt.Sprintf("-Dfile.encoding=UTF-8 -Dlogback.statusListenerClass=ch.qos.logback.core.status.NopStatusListener -Djava.security.egd=file:///dev/urandom -Ddatax.home=%s -Dlogback.configurationFile=%s",
		dataxHome, logbackFile)
	engineCommand = fmt.Sprintf("-server {{.Jvm}} %s -classpath %s {{.Params}} com.alibaba.datax.core.Engine -mode {{.Mode}} -jobid {{.JobId}} -job {{.Job}}",
		defaultPropertyConf, classPath)

	if len(os.Args) < 2 {
		fmt.Println("expected 'generate' or 'run' subcommands")
		return
	}

	switch os.Args[1] {
	case "generate":
		generateCmd.Parse(os.Args[2:])
		if help {
			generateCmd.Usage()
			return
		}

		viper.SetConfigName("app")
		viper.SetConfigType("yaml")
		viper.AddConfigPath("./config")
		if err := viper.ReadInConfig(); err != nil {
			panic(err)
		}
		sub := viper.Sub("source_db")
		sub.Unmarshal(&sourceDb)
		sub = viper.Sub("target_db")
		sub.Unmarshal(&targetDb)

		viper.SetConfigName("ignore")
		if err := viper.ReadInConfig(); err != nil {
			panic(err)
		}
		ignoreTables = viper.GetStringSlice("ignore_tables")

		if increment {
			viper.SetConfigName("increment")
			if err := viper.ReadInConfig(); err != nil {
				panic(err)
			}
			sub = viper.Sub("increment_tables")
			sub.Unmarshal(&incrementTables)
		}

		generateJob()
	case "run":
		runCmd.Parse(os.Args[2:])
		if help {
			runCmd.Usage()
			return
		}

		if len(runCmd.Args()) < 1 {
			fmt.Println("no job file")
			return
		}

		if runCmd.Arg(0) == "all" {
			files, _ := GetAllFile("./" + outdir)
			for _, file := range files {
				args := buildJavaArgs(file)
				c := exec.Command("java", args...)
				c.Stdout = os.Stdout
				c.Stderr = os.Stderr
				e := c.Run()
				if e != nil {
					panic(e)
				}
			}
		} else {
			args := buildJavaArgs(runCmd.Arg(0))
			c := exec.Command("java", args...)
			c.Stdout = os.Stdout
			c.Stderr = os.Stderr
			e := c.Run()
			if e != nil {
				panic(e)
			}
		}
	default:
		flag.Parse()
		if help {
			flag.Usage()
			return
		}

		fmt.Println("expected 'generate' or 'run' subcommands")
		os.Exit(1)
	}
}
