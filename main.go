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

var judgeQueue = make(chan Job, 100)
var jobPool = sync.Pool{
	New: func() interface{} {
		return new(Job)
	},
}
var serverReserveMap sync.Map
var realUuidMap sync.Map
var serverMap sync.Map

func main() {
	config, err := loadConfig()
	if err != nil {
		log.Fatalf(fmt.Sprintf("Read config.json fail: %v", err))
		os.Exit(1)
	}

	initMap(config)
	go judgeWorker()

	http.HandleFunc("/judge", handleJudgeRequest)
	http.HandleFunc("/state", handleStateRequest)

	http.ListenAndServe(":7000", nil)
}

func initMap(config ServerConfig) {
	servers := config.Servers
	for _, s := range servers {
		serverReserveMap.Store(s, s.MaxJob)
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
	job := jobPool.Get().(*Job)
	defer jobPool.Put(job)
	job.Uuid = uuid.New().String()

	realUuidMap.Store(job.Uuid, "queuing")

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

	realUuid, ok := realUuidMap.Load(uuid)

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

			serverReserveMap.Range(func(key, value any) bool {
				server := key.(Server)
				reserve := value.(int)

				if reserve > 0 {
					idleServer = server
					serverReserveMap.Store(server, reserve-1)
					return false //break
				}

				return true //continue
			})

			if (idleServer == Server{}) {
				judgeQueue <- job
				return
			}

			resp, err := http.Post(fmt.Sprintf("http://%s:%d/judge", idleServer.Adress, idleServer.Port), "application/json", bytes.NewBuffer(job.Body))
			if err != nil {
				realUuidMap.Store(job.Uuid, "error")
				return
			}

			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				realUuidMap.Store(job.Uuid, "error")
				return
			}

			var respData struct {
				Uuid string `json:"uuid"`
			}

			json.NewDecoder(resp.Body).Decode(&respData)

			realUuidMap.Store(job.Uuid, respData.Uuid)
			serverMap.Store(job.Uuid, idleServer)
		}(job)
	}
}
