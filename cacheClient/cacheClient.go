package main

//import statements
import (
	"fmt"
	"net/http"
	"hash/crc32"
	"sort"
	"strings"
	"strconv"
	"io/ioutil"
)

const KEY_VAL_PAIRS = 10

type KVal struct {
	Key int `json:"key_id, omitempty"`
	Value string `json:"value, omitempty"`
}

type Collection struct {
	KVList []KVal `json:"kvlist"`
}

type HashRing []uint32

func (hr HashRing) Len() int {
	return len(hr)
}

func (hr HashRing) Less(i, j int) bool {
	return hr[i] < hr[j]
}

func (hr HashRing) Swap(i, j int) {
	hr[i], hr[j] = hr[j], hr[i]
}

type Node struct {
	Id     int
	Ip     string
	Weight int
}

//
func NewNode(id int, ip string, weight int) *Node {
	return &Node{
		Id:     id,
		Ip:     ip,
		Weight: weight,
	}
}

type ConstHash struct {
	Nodes map[uint32]Node
	IsResource map[int]bool
	Ring HashRing
	numReps int
}

func NewConstHash() *ConstHash {
	return &ConstHash{
		Nodes: make(map[uint32]Node),
		IsResource: make(map[int]bool),
		Ring: HashRing{},
		numReps: KEY_VAL_PAIRS,
	}
}

func main() {
	cHash := NewConstHash()
	cHash.Add(NewNode(0, "http://localhost:3000", 1))
	cHash.Add(NewNode(1, "http://localhost:3001", 1))
	cHash.Add(NewNode(2, "http://localhost:3002", 1))

	map1 := make(map[KVal]string)
	var arr []KVal
	arr = append(arr, KVal{1, "a"})
	arr = append(arr, KVal{2, "b"})
	arr = append(arr, KVal{3, "c"})
	arr = append(arr, KVal{4, "d"})
	arr = append(arr, KVal{5, "e"})
	arr = append(arr, KVal{6, "f"})
	arr = append(arr, KVal{7, "g"})
	arr = append(arr, KVal{8, "h"})
	arr = append(arr, KVal{9, "i"})
	arr = append(arr, KVal{10, "j"})

	for count := 0; count < 10; count++ {
		temp := cHash.Get(arr[count].Value)
		map1[arr[count]] = temp.Ip
	}

	//Make PUT call
	for key, val := range map1 {
		if strings.Contains(val, "3000") {
			PutFunc(key, "3000")
			//fmt.Println()
			//GetFunc(key, "3000")
	} else if strings.Contains(val, "3001") {
		PutFunc(key, "3001")
		//fmt.Println()
		//GetFunc(key, "3001")
	} else if strings.Contains(val, "3002") {
		PutFunc(key, "3002")
		//fmt.Println()
		//GetFunc(key, "3002")
	} else {
		fmt.Println("Incorrect/Invalid Port!")
	}
}
fmt.Println()
//Make GET call
fmt.Println("Making GET calls to retrieve key-value pairs")
for key, val := range map1 {
	if strings.Contains(val, "3000") {
		GetFunc(key, "3000")
} else if strings.Contains(val, "3001") {
	GetFunc(key, "3001")
} else if strings.Contains(val, "3002") {
	GetFunc(key, "3002")
} else {
	fmt.Println("Incorrect/Invalid Port!")
}
}
}

func (ring *ConstHash) Add(node *Node) bool {
	if _, ok := ring.IsResource[node.Id]; ok {
		return false
	}

	count := ring.numReps * node.Weight

	for i:= 0; i<count; i++ {
		str := ring.JoinStr(i, node)
		ring.Nodes[ring.HashStr(str)] = *(node)
	}
	ring.IsResource[node.Id] = true
	ring.SortHashRing()
	return true
}

func (ring *ConstHash) JoinStr(i int, node *Node) string {
	return node.Ip + "*" + strconv.Itoa(node.Weight) + "-" + strconv.Itoa(i) + "-" + strconv.Itoa(node.Id)
}

func (ring *ConstHash) SortHashRing() {
	ring.Ring = HashRing{}
	for k := range ring.Nodes {
		ring.Ring = append(ring.Ring, k)
	}
	sort.Sort(ring.Ring)
}

func (ring *ConstHash) ReturnNodeIP(node *Node) string {
	return node.Ip
}

func (ring *ConstHash) HashStr(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func (ring *ConstHash) Get(key string) Node {
	hash := ring.HashStr(key)
	i := ring.NodeSearch(hash)
	return ring.Nodes[ring.Ring[i]]
}

func (ring *ConstHash) NodeSearch(hash uint32) int {

	i := sort.Search(len(ring.Ring), func(i int) bool { return ring.Ring[i] >= hash })
	if i < len(ring.Ring) {
		if i == len(ring.Ring)-1 {
			return 0
		} else {
			return i
		}
	} else {
		return len(ring.Ring) - 1
	}
}

//Retrieve key value pair from all 3 server instances
func GetFunc(kv KVal, in string) {
	ipAddr := "http://localhost:"
	ipAddr += in
	ipAddr += "/keys/" + strconv.Itoa(kv.Key) + "/" + kv.Value
	//fmt.Println("GET URL Address :: ", ipAddr)

	fmt.Println("Getting key ", strconv.Itoa(kv.Key),
	"and value ", kv.Value,
	" from port ", in)

	req,_ := http.NewRequest("GET", ipAddr, nil)
	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error while PUT request", err)
		panic(err)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err!= nil {
		fmt.Printf(string(body))
	}
	defer resp.Body.Close()
}


//PUT call to any of 3 instances of localhost
func PutFunc(kv KVal, in string) {
	ipAddr := "http://localhost:"
	ipAddr += in
	ipAddr += "/keys/" + strconv.Itoa(kv.Key) + "/" + kv.Value
	fmt.Println("PUT URL Address :: ", ipAddr)

	req, err := http.NewRequest("PUT", ipAddr, nil)
	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error while PUT request", err)
		panic(err)
	} else {
		defer resp.Body.Close()
		//fmt.Println("Successful PUT request!")
	}
}