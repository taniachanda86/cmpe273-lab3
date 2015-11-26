package main

//import statements
import (
  "fmt"
  "net/http"
  "github.com/julienschmidt/httprouter"
  "strconv"
  "encoding/json"
 )

 type KVal struct {
   Key int `json:"key_id"`
   Value string `json:"value"`
 }

 type Collection struct {
   KVList []KVal `json:"kvlist"`
 }

 var kvMap map[int]KVal

 func main() {
   kvMap = make(map[int]KVal)
   route1 := httprouter.New()
   route1.PUT("/keys/:key_id/:value", PutHandler1)
   route1.GET("/keys/:key_id", GetHandler1)
   route1.GET("/keys", GetAllHandler1)
   server1 := http.Server{
     Addr: "localhost:3000",
     Handler: route1,
   }
   server1.ListenAndServe()
 }

 func PutHandler1(rw http.ResponseWriter, req *http.Request, p httprouter.Params) {
   var kv KVal
   k, err := strconv.Atoi(p.ByName("key_id"))
   if err != nil {
     panic(err)
   }
   v := p.ByName("value")

   kv.Key = k
   kv.Value = v
   kvMap[k] = kv

   rw.Header().Set("Content-Type", "application/json")
   rw.WriteHeader(http.StatusOK)
   //fmt.Fprintf(rw, "%s", kvMap)
  }

 func GetHandler1(rw http.ResponseWriter, req *http.Request, p httprouter.Params) {
   k, err := strconv.Atoi(p.ByName("key_id"))
   if err != nil {
     panic(err)
   }

   var kv KVal

   for key, value := range kvMap {
     if key == k {
       kv.Key = k
       kv.Value = value.Value
     }
   }
   resp, _ := json.Marshal(kv)
   rw.Header().Set("Content-Type", "application/json")
   rw.WriteHeader(http.StatusOK)
   fmt.Fprintf(rw, "%s", resp)
  }

 func GetAllHandler1(rw http.ResponseWriter, req *http.Request, p httprouter.Params) {
  	  var kvList []KVal
      for key, value := range kvMap {
        temp := KVal { key, value.Value, }
        kvList = append(kvList, temp)
    }

    var coll Collection
    coll.KVList = kvList

    resp, _ := json.Marshal(coll)
    rw.Header().Set("Content-Type", "application/json")
    rw.WriteHeader(http.StatusOK)
    fmt.Fprintf(rw, "%s", resp)
  }