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
var serverAliveMap sync.Map
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
		serverAliveMap.Store(s, true)
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
		alive, ok := serverAliveMap.Load(s)
		if !ok {
			log.Print("Read serverAliveMap error")
		}
		status.Alive = alive.(bool)

		serverReserveMapMutex.RLock()
		status.Reserve = serverReserveMap[s]
		serverReserveMapMutex.RUnlock()

		response.ServerStatusList = append(response.ServerStatusList, status)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func judgeWorker() {
	for job := range judgeQueue {
		go func(job Job) {
			var idleServer Server = Server{}

			serverReserveMapMutex.Lock()
			for server, reserve := range serverReserveMap {
				if reserve > 0 {
					alive, ok := serverAliveMap.Load(server)
					if !ok {
						//log
						continue
					}
					if alive.(bool) {
						idleServer = server
						serverReserveMap[server]--
						break
					}
				}
			}
			serverReserveMapMutex.Unlock()

			if idleServer == (Server{}) {
				time.Sleep(30 * time.Millisecond)
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

				time.Sleep(30 * time.Millisecond)
				judgeQueue <- job
				//TODO: log
				return
			}

			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				uuidRealUuidMap.Store(job.Uuid, "error")
				serverReserveMapMutex.Lock()
				serverReserveMap[idleServer]++
				serverReserveMapMutex.Unlock()

				time.Sleep(30 * time.Millisecond)
				judgeQueue <- job
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
	defer r.Body.Close()
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

var serverChannel = make(chan Server, 10)

func checkAliveWorker() {
	for server := range serverChannel {
		go func(server Server) {
			url := fmt.Sprintf("http://%s:%d/ping", server.Adress, server.Port)

			resp, err := client.Get(url)

			if err != nil {
				serverAliveMap.Store(server, false)
				return
			}

			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				serverAliveMap.Store(server, false)
				return
			}

			len := resp.ContentLength
			body := make([]byte, len)
			resp.Body.Read(body)
			s := string(body)

			if s != "pong" {
				serverAliveMap.Store(server, false)
				return
			}

			serverAliveMap.Store(server, true)
		}(server)
	}
}

func checkServerAlive() {
	go checkAliveWorker()

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
