package main

import (
	"strconv"
    "encoding/json"
    "strings"
	"github.com/julienschmidt/httprouter"
	"net/http"
)

var map_server_1 map[int] string
var map_server_2 map[int] string
var map_server_3 map[int] string

type Response struct{
	Key  int 		`json:"key"`
	Value string    `json:"value"`

}

func main(){
	map_server_1 = make(map[int] string)
  	map_server_2 = make(map[int] string)
  	map_server_3 = make(map[int] string)

	go func(){
		mux1 := httprouter.New()
	    mux1.PUT("/keys/:id/:value",put)
	    mux1.GET("/keys/:id",get)
	    mux1.GET("/keys",getall)
	    server1 := http.Server{
	            Addr:        "0.0.0.0:3000",
	            Handler: mux1,
    	}
    	server1.ListenAndServe()
	}()


    go func(){
	    mux2 := httprouter.New()
	    mux2.PUT("/keys/:id/:value",put)
	    mux2.GET("/keys/:id",get)
	    mux2.GET("/keys",getall)
	    server2 := http.Server{
	            Addr:        "0.0.0.0:3001",
	            Handler: mux2,
	    }
	    server2.ListenAndServe()
    }()


  	mux3 := httprouter.New()
    mux3.PUT("/keys/:id/:value",put)
    mux3.GET("/keys/:id",get)
    mux3.GET("/keys",getall)
    server3 := http.Server{
            Addr:        "0.0.0.0:3002",
            Handler: mux3,
    }
    server3.ListenAndServe()


}


func put(rw http.ResponseWriter, req *http.Request, p httprouter.Params){

	key := p.ByName("id")
	value := p.ByName("value")
  var port []string
	key_int, _ := strconv.Atoi(key)

  port = strings.Split(req.Host,":")
  if(port[1]=="3000"){
      map_server_1[key_int] = value

  } else if (port[1]=="3001"){
      map_server_2[key_int] = value

  } else{
      map_server_3[key_int] = value

    }

}

func get(rw http.ResponseWriter, req *http.Request, p httprouter.Params){

	key := p.ByName("id")
	key_int, _ := strconv.Atoi(key)
  var port []string
	var response Response
    port = strings.Split(req.Host,":")
  if(port[1]=="3000"){
      response.Key = key_int
      response.Value = map_server_1[key_int]

  } else if (port[1]=="3001"){
      response.Key = key_int
      response.Value = map_server_2[key_int]

  } else{
      response.Key = key_int
      response.Value = map_server_3[key_int]

    }
  	resp, err := json.Marshal(response)
  	if err != nil {
    	 http.Error(rw,"Error " , http.StatusInternalServerError)
     	return
  	}
  	rw.Header().Set("Content-Type", "application/json")
  	rw.Write(resp)
}


func getall(rw http.ResponseWriter, req *http.Request, p httprouter.Params){
	var response []Response
	var key_pair Response
  var port []string
  port = strings.Split(req.Host,":")
  if(port[1]=="3000"){
      for key, value := range map_server_1 {
      key_pair.Key = key
      key_pair.Value = value
       response = append(response, key_pair)
      }

  } else if (port[1]=="3001"){
      for key, value := range map_server_2 {
      key_pair.Key = key
      key_pair.Value = value
       response = append(response, key_pair)
      }

  } else{
      for key, value := range map_server_3 {
      key_pair.Key = key
      key_pair.Value = value
       response = append(response, key_pair)
      }

    }


  	resp, err := json.Marshal(response)
  	if err != nil {
    	 http.Error(rw,"Error " , http.StatusInternalServerError)
     	return
  	}
  	rw.Header().Set("Content-Type", "application/json")
  	rw.Write(resp)
}
