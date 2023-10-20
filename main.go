package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
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
	Servers []Server `json:"servers"`
}

type Job struct {
	Uuid string
	Body []byte
}

var serverReserveMap = make(map[Server]int)
var judgeQueue []Job
var realUuidMap = make(map[string]string)
var serverMap = make(map[string]Server)

var (
	serverReserveMapMutex sync.Mutex
	judgeQueueMutex       sync.Mutex
	realUuidMapMutex      sync.Mutex
	serverMapMutex        sync.Mutex
)

func main() {
	config, err := loadConfig()
	if err != nil {
		log.Fatalf(fmt.Sprintf("Read config.json fail: %v", err))
		os.Exit(1)
	}

	initMap(config)

	http.HandleFunc("/judge", handleJudgeRequest)
	http.HandleFunc("/state", handleStateRequest)

	go judgeWorker()
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
		return ServerConfig{nil}, _err
	}

	var config ServerConfig
	_err = json.Unmarshal(_data, &config)
	if _err != nil {
		return ServerConfig{nil}, _err
	}

	return config, nil
}

func handleJudgeRequest(w http.ResponseWriter, r *http.Request) {
	_uuid := uuid.New().String()

	realUuidMapMutex.Lock()
	realUuidMap[_uuid] = "queuing"
	realUuidMapMutex.Unlock()

	body, err := io.ReadAll(r.Body)
	var job Job
	if err == nil {
		job = Job{_uuid, body}
	}

	judgeQueueMutex.Lock()
	judgeQueue = append(judgeQueue, job)
	judgeQueueMutex.Unlock()

	var res struct {
		Uuid string `json:"uuid"`
	}

	res.Uuid = _uuid

	w.Header().Set("content-type", "text/json")
	json.NewEncoder(w).Encode(res)
}

func handleStateRequest(w http.ResponseWriter, r *http.Request) {
	var res struct {
		State   string `json:"state"`
		Message string `json:"message"`
	}

	query := r.URL.Query()
	uuid := query.Get("uuid")

	realUuidMapMutex.Lock()
	realUuid, ok := realUuidMap[uuid]
	realUuidMapMutex.Unlock()

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

	serverMapMutex.Lock()
	server := serverMap[uuid]
	serverMapMutex.Unlock()

	resp, err := http.Get(fmt.Sprintf("http://%s:%d/state?uuid=%s", server.Adress, server.Port, realUuid))
	if err != nil {
		log.Printf("Http get: http://%s:%d/state?uuid=%s fail", server.Adress, server.Port, realUuid)
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Printf("Http get: http://%s:%d/state?uuid=%s fail", server.Adress, server.Port, realUuid)
	}

	// var respBody struct {
	// 	State   string `json:"state"`
	// 	Message string `json:"message"`
	// }

	// json.NewDecoder(resp.Body).Decode(&respBody)
	w.Header().Set("Content-Type", "application/json")
	io.Copy(w, resp.Body)
}

func judgeWorker() {
	for {
		var continueFlag bool
		judgeQueueMutex.Lock()
		if len(judgeQueue) == 0 {
			continueFlag = true
		}
		judgeQueueMutex.Unlock()
		if continueFlag {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		judgeQueueMutex.Lock()
		job := judgeQueue[0]
		judgeQueue = judgeQueue[1:]
		judgeQueueMutex.Unlock()

		go func(job Job) {
			var idleServer Server
			serverReserveMapMutex.Lock()
			for server, reserve := range serverReserveMap {
				if reserve > 0 {
					idleServer = server
					serverReserveMap[server] -= 1
					break
				}
			}
			serverReserveMapMutex.Unlock()

			resp, err := http.Post(fmt.Sprintf("http://%s:%d/judge", idleServer.Adress, idleServer.Port), "application/json", bytes.NewBuffer(job.Body))
			if err != nil {
				realUuidMapMutex.Lock()
				realUuidMap[job.Uuid] = "error"
				realUuidMapMutex.Unlock()
				return
			}

			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				realUuidMapMutex.Lock()
				realUuidMap[job.Uuid] = "error"
				realUuidMapMutex.Unlock()
				return
			}

			var respData struct {
				Uuid string `json:"uuid"`
			}

			json.NewDecoder(resp.Body).Decode(&respData)

			realUuidMapMutex.Lock()
			realUuidMap[job.Uuid] = respData.Uuid
			realUuidMapMutex.Unlock()

			serverMapMutex.Lock()
			serverMap[job.Uuid] = idleServer
			serverMapMutex.Unlock()
		}(job)
	}
}
