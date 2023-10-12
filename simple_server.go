package main

import (
	"fmt"
	"net/http"
)

var server *http.Server

const serverPort = 5000

func start_server() {
	server = &http.Server{Addr: ":" + fmt.Sprint(serverPort)}
	println("Inside anomaly route")
	http.HandleFunc("/api/v1/anomaly", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("[]"))
	})

	http.HandleFunc("/api/v1/logstream/demo/config", func(w http.ResponseWriter, r *http.Request) {
		println("Inside config route")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("[]"))
	})

	go func() {
		fmt.Println("HTTP server started on :" + fmt.Sprint(serverPort))
		if err := server.ListenAndServe(); err != nil {
			fmt.Println("HTTP server error:", err)
		}
	}()
}

func stop_server() {
	if server != nil {
		fmt.Println("Shutting down simple server gracefully...")
		if err := server.Shutdown(nil); err != nil {
			fmt.Println("Error while shutting down the server:", err)
		}
	}
}

// func main() {
// 	start_server()
// 	i := 0

// 	for {
// 		fmt.Println("Hello")
// 		time.Sleep(1 * time.Second)
// 		i += 1
// 		println("i = ", i)
// 		if i == 5 {
// 			stop_server()
// 			break
// 		}
// 	}

// }
