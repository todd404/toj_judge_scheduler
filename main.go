package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Server struct {
	Adress string `json:"adress"`
	Port   int    `json:"port"`
	MaxJob int    `json:"max_job"`
}

type ServerConfig struct {
	BackendServer struct {
		Adress string `json:"adress"`
		Port   int    `json:"port"`
	} `json:"backend_server"`
	Servers []Server `json:"servers"`
}

type Job struct {
	Uuid string
	Body []byte
}

var config ServerConfig
var judgeQueue = make(chan Job, 100)
var jobPool = sync.Pool{
	New: func() interface{} {
		return new(Job)
	},
}
var serverReserveMap = make(map[Server]int)
var uuidRealUuidMap sync.Map
var realUuidUuidMap sync.Map
var serverMap sync.Map

var serverReserveMapMutex sync.RWMutex

func main() {
	var err error
	config, err = loadConfig()
	if err != nil {
		log.Fatalf(fmt.Sprintf("Read config.json fail: %v", err))
		os.Exit(1)
	}

	initMap(config)
	go judgeWorker()
	go monitorServerReserve()

	http.HandleFunc("/judge", handleJudgeRequest)
	http.HandleFunc("/state", handleStateRequest)
	http.HandleFunc("/api/set-result", handleSetResult)

	http.ListenAndServe(":7000", nil)
}

func initMap(config ServerConfig) {
	servers := config.Servers
	for _, s := range servers {
		serverReserveMap[s] = s.MaxJob
	}
}

func loadConfig() (server_config ServerConfig, err error) {
	_data, _err := os.ReadFile("config.json")
	if _err != nil {
		return ServerConfig{}, _err
	}

	var config ServerConfig
	_err = json.Unmarshal(_data, &config)
	if _err != nil {
		return ServerConfig{}, _err
	}

	return config, nil
}

func handleJudgeRequest(w http.ResponseWriter, r *http.Request) {
	job := jobPool.Get().(*Job)
	defer jobPool.Put(job)
	job.Uuid = uuid.New().String()

	uuidRealUuidMap.Store(job.Uuid, "queuing")

	body, err := io.ReadAll(r.Body)

	if err != nil {
		log.Printf("Read request body fail: %v", err)
		http.Error(w, "Judge Scheduler Crash", 500)
		return
	}
	job.Body = body

	//开一个协程异步在judgeQueue channel里放job
	//防止阻塞而导致uuid无法返回
	go func() {
		judgeQueue <- *job
	}()

	var res struct {
		Uuid string `json:"uuid"`
	}

	res.Uuid = job.Uuid

	w.Header().Set("content-type", "application/json")
	json.NewEncoder(w).Encode(res)
}

func handleStateRequest(w http.ResponseWriter, r *http.Request) {
	var res struct {
		State   string `json:"state"`
		Message string `json:"message"`
	}

	query := r.URL.Query()
	uuid := query.Get("uuid")

	realUuid, ok := uuidRealUuidMap.Load(uuid)

	w.Header().Set("Content-Type", "application/json")
	if uuid == "" || !ok {
		res.State = "error uuid"
		res.Message = "uuid错误..."
		json.NewEncoder(w).Encode(res)
		return
	}

	if realUuid == "queuing" {
		res.State = "queuing"
		res.Message = "排队中..."
		json.NewEncoder(w).Encode(res)
		return
	}

	if realUuid == "error" || realUuid == "" {
		res.State = "error"
		res.Message = "判题服务器错误."
		json.NewEncoder(w).Encode(res)
		return
	}

	value, _ := serverMap.Load(uuid)
	server := value.(Server)

	resp, err := http.Get(fmt.Sprintf("http://%s:%d/state?uuid=%s", server.Adress, server.Port, realUuid))
	if err != nil {
		log.Printf("Http get: http://%s:%d/state?uuid=%s fail", server.Adress, server.Port, realUuid)
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Printf("Http get: http://%s:%d/state?uuid=%s fail", server.Adress, server.Port, realUuid)
	}

	w.Header().Set("Content-Type", "application/json")
	io.Copy(w, resp.Body)
}

func judgeWorker() {
	for job := range judgeQueue {
		go func(job Job) {
			var idleServer Server = Server{}

			serverReserveMapMutex.Lock()
			for server, reserve := range serverReserveMap {
				if reserve > 0 {
					idleServer = server
					serverReserveMap[server]--
					break
				}
			}
			serverReserveMapMutex.Unlock()

			if idleServer == (Server{}) {
				//time.Sleep(5 * time.Millisecond)
				judgeQueue <- job
				return
			}

			resp, err := http.Post(
				fmt.Sprintf("http://%s:%d/judge", idleServer.Adress, idleServer.Port),
				"application/json",
				bytes.NewBuffer(job.Body),
			)
			if err != nil {
				uuidRealUuidMap.Store(job.Uuid, "error")
				serverReserveMapMutex.Lock()
				serverReserveMap[idleServer]++
				serverReserveMapMutex.Unlock()
				//TODO: log
				return
			}

			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				uuidRealUuidMap.Store(job.Uuid, "error")
				serverReserveMapMutex.Lock()
				serverReserveMap[idleServer]++
				serverReserveMapMutex.Unlock()
				//TODO: log
				return
			}

			var respData struct {
				Uuid string `json:"uuid"`
			}

			json.NewDecoder(resp.Body).Decode(&respData)

			uuidRealUuidMap.Store(job.Uuid, respData.Uuid)
			realUuidUuidMap.Store(respData.Uuid, job.Uuid)
			serverMap.Store(job.Uuid, idleServer)
		}(job)
	}
}

func handleSetResult(w http.ResponseWriter, r *http.Request) {
	var data struct {
		RealUuid string `json:"uuid"`
	}
	json.NewDecoder(r.Body).Decode(&data)

	uuid, ok := realUuidUuidMap.Load(data.RealUuid)
	if !ok {
		log.Printf("Foward set result error: wrong uuid")
		return
	}

	value, ok := serverMap.Load(uuid)
	if !ok {
		log.Printf("Foward set result error: wrong uuid")
		return
	}
	server := value.(Server)

	serverReserveMapMutex.Lock()
	serverReserveMap[server]++
	serverReserveMapMutex.Unlock()

	resp, err := http.Post(
		fmt.Sprintf("http://%s:%d/api/set-result", config.BackendServer.Adress, config.BackendServer.Port),
		"application/json",
		r.Body,
	)

	if err != nil {
		log.Printf("Report result fail!: %v", err)
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Printf("Report result fail with status code: %v", resp.StatusCode)
	}
}

func clearTerminal() {
	optSys := runtime.GOOS
	if optSys == "linux" {
		//执行clear指令清除控制台
		cmd := exec.Command("clear")
		cmd.Stdout = os.Stdout
		err := cmd.Run()
		if err != nil {
			log.Fatalf("cmd: clear error: %v\n", err.Error())
		}
	} else {
		//执行clear指令清除控制台
		cmd := exec.Command("cmd", "/c", "cls")
		cmd.Stdout = os.Stdout
		err := cmd.Run()
		if err != nil {
			log.Fatalf("cmd: cls error: %v\n", err.Error())
		}
	}
}

func monitorServerReserve() {
	ticker := time.NewTicker(300 * time.Millisecond) // 每 0.3 秒检查一次
	defer ticker.Stop()

	for range ticker.C {
		clearTerminal()

		serverReserveMapMutex.RLock()
		statusLine := ""
		for server, reserve := range serverReserveMap {
			// 在这里处理每个服务器的剩余可用量
			statusLine += fmt.Sprintf("Server: %s:%d, Remaining Jobs: %d\n", server.Adress, server.Port, reserve)
		}
		serverReserveMapMutex.RUnlock()
	}
}
