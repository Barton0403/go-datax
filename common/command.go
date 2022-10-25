package common

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"strings"
	"text/template"
)

var (
	defaultJvm    string
	engineCommand string
)

func init() {
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
}

type JavaCommandMap struct {
	Jvm    string
	Params string
	Mode   string
	JobId  int
	Job    string
}

func BuildJavaArgs(jobFilename string, jobId int, mode string) []string {
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
