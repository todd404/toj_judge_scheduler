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
	Port          int `json:"port"`
	CheckInterval int `json:"check_interval"`
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
var transport = &http.Transport{
	MaxIdleConnsPerHost: 100,
}
var client = &http.Client{
	Transport: transport,
}

var judgeQueue = make(chan Job, 100)
var jobPool = sync.Pool{
	New: func() interface{} {
		return new(Job)
	},
}
var serverReserveMap = make(map[Server]int)
var serverAliveMap = make(map[Server]bool)
var uuidRealUuidMap sync.Map
var realUuidUuidMap sync.Map
var serverMap sync.Map

var serverReserveMapMutex sync.RWMutex
var serverAliveMapMutex sync.RWMutex

func main() {
	var err error
	config, err = loadConfig()
	if err != nil {
		log.Fatalf(fmt.Sprintf("Read config.json fail: %v", err))
		os.Exit(1)
	}

	initMap(config)

	go judgeWorker()
	go monitorServerAlive()

	http.HandleFunc("/judge", handleJudgeRequest)
	http.HandleFunc("/state", handleStateRequest)
	http.HandleFunc("/api/set-result", handleSetResult)
	http.HandleFunc("/server-status", handleServerStatus)

	http.ListenAndServe(fmt.Sprintf(":%d", config.Port), nil)
}

func initMap(config ServerConfig) {
	servers := config.Servers
	for _, s := range servers {
		serverReserveMap[s] = s.MaxJob
		serverAliveMap[s] = true
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

	resp, err := client.Get(fmt.Sprintf("http://%s:%d/state?uuid=%s", server.Adress, server.Port, realUuid))
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

func handleServerStatus(w http.ResponseWriter, r *http.Request) {
	type ServerStatusItem struct {
		Adress  string `json:"adress"`
		Port    int    `json:"port"`
		Alive   bool   `json:"alive"`
		Reserve int    `json:"reserve"`
	}

	var response struct {
		ServerStatusList []ServerStatusItem `json:"servers"`
	}

	for _, s := range config.Servers {
		status := ServerStatusItem{}

		status.Adress = s.Adress
		status.Port = s.Port
		serverAliveMapMutex.RLock()
		status.Alive = serverAliveMap[s]
		serverAliveMapMutex.RUnlock()

		serverReserveMapMutex.RLock()
		status.Reserve = serverReserveMap[s]
		serverAliveMapMutex.RUnlock()

		response.ServerStatusList = append(response.ServerStatusList, status)
	}

	json.NewEncoder(w).Encode(response)
}

func judgeWorker() {
	for job := range judgeQueue {
		go func(job Job) {
			var idleServer Server = Server{}

			serverReserveMapMutex.Lock()
			for server, reserve := range serverReserveMap {
				if reserve > 0 {
					serverAliveMapMutex.RLock()
					if serverAliveMap[server] {
						idleServer = server
						serverReserveMap[server]--
						break
					}
					serverAliveMapMutex.RUnlock()
				}
			}
			serverReserveMapMutex.Unlock()

			if idleServer == (Server{}) {
				//time.Sleep(5 * time.Millisecond)
				judgeQueue <- job
				return
			}

			resp, err := client.Post(
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

	resp, err := client.Post(
		fmt.Sprintf("http://%s:%d/api/set-result", config.BackendServer.Adress, config.BackendServer.Port),
		"application/json",
		r.Body,
	)

	if err != nil {
		log.Printf("Report result fail!: %v", err.Error())
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Printf("Report result fail with status code: %v", resp.StatusCode)
	}
}

func checkServerAlive() {
	serverChannel := make(chan Server)

	go func() {
		server := <-serverChannel

		url := fmt.Sprintf("http://%s:%d/ping", server.Adress, server.Port)

		resp, err := client.Get(url)
		if err != nil {
			serverAliveMapMutex.Lock()
			serverAliveMap[server] = false
			serverAliveMapMutex.Unlock()
			return
		}

		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			serverAliveMapMutex.Lock()
			serverAliveMap[server] = false
			serverAliveMapMutex.Unlock()
			return
		}

		var body []byte
		resp.Body.Read(body)
		s := string(body)

		if s != "pong" {
			serverAliveMapMutex.Lock()
			serverAliveMap[server] = false
			serverAliveMapMutex.Unlock()
			return
		}

		serverAliveMapMutex.Lock()
		serverAliveMap[server] = true
		serverAliveMapMutex.Unlock()
	}()

	for _, server := range config.Servers {
		serverChannel <- server
	}
}

func monitorServerAlive() {
	ticker := time.NewTicker(time.Duration(config.CheckInterval) * time.Second) // 每 1 秒检查一次
	defer ticker.Stop()

	for ; true; <-ticker.C {
		checkServerAlive()
	}
}
