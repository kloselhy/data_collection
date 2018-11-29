package main

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"fmt"      //add for output
	"sort"      //add for sort slice
)

var (
	// number of client
	clientNums = 2
	// number of messages, simpify program implementation
	messageNums = 20
	// assume dataStreaming has unlimited capacity
	dataStreaming []chan data

	token int64
	step  int64

	maxSleepInterval int64 = 5
	maxGap           int64 = 10

	wg sync.WaitGroup

	//add for answer
	minTop = make([]minGroup, clientNums)      //save minGroup for each client
	haveData = make([]bool, clientNums)        //save the status for each client, true means this client still have data to process
	groupMinIndex = make([]int, clientNums)    //save the min's index for each minGroup
	debug = true                              //debug switch
)

type data struct {
	kind    string
	prepare int64
	commit  int64
}

/* 
 * definition of type minGroup, and function about sort
 */
type minGroup []data
func (g minGroup) Len() int {
        return len(g)
}
func (g minGroup) Less(i, j int) bool {
        return g[i].commit < g[j].commit
}
func (g minGroup) Swap(i, j int) {
        g[i], g[j] = g[j], g[i]
}

func init() {
	dataStreaming = make([]chan data, clientNums)
	for i := 0; i < clientNums; i++ {
		dataStreaming[i] = make(chan data, messageNums)
	}
}

/* 1. please implement collect code
 *    u can add some auxiliary structures, variables and functions
 *    dont modify any definition
 * 2. implement flow control for the collect code
 */
func main() {
	wg.Add(clientNums*2 + 1)
	// genrateDatas and collect are parallel
	for i := 0; i < clientNums; i++ {
		go func(index int) {
			defer wg.Done()
			generateDatas(index)
		}(i)
		go func(index int) {
			defer wg.Done()
			generateDatas(index)
		}(i)
	}

	go func() {
		defer wg.Done()
		collect()
	}()
	wg.Wait()
}

/* function to get data from the appointed channel(with enough timeout)
 * timeout means the channel has no data(in reality,it means the data source exit or dead)
 */
func getcommitdata(index int)(data,bool){
	timeout:=false
	var v1 data
	select{
		case v1=<-dataStreaming[index]:
			break
		case <-time.After(time.Duration(20*maxSleepInterval)*time.Millisecond):
			timeout=true
			break
	}
	return v1,timeout
}

/* function to get minGroup from the appointed channel, the minGroup only contains commit data and is already sorted.
 * because of multi-thread, each channel's data is not strictly sorted
 * but when the prepare and the commit data pair up, all of them(form a group) are definitely less than the following data (in the same channel)
 */
func getgroup(index int)(minGroup){
	timeout:=false
	var v1 data
	var tmpgroup minGroup
	preparecnt:=0
	commitcnt:=0
	for{
		v1,timeout=getcommitdata(index)
		if timeout{
			break
		}
		if v1.kind=="prepare"{
			preparecnt++
		}else{
			commitcnt++
			tmpgroup=append(tmpgroup,v1)
		}
		if preparecnt==commitcnt{
			break
		}
	}
	//return a sorted minGroup
	sort.Sort(tmpgroup)
	return tmpgroup
}
/*
 * 1 assume dataStreamings are endless => we have infinitely many datas;
 * because it's a simulation program, it has a limited number of datas, but the assumption is not shoul be satisfied
 * 2 sort commit kind of datas that are from multiple dataStreamings by commit ascending
 * and output them in the fastest way you can think
 */
func collect() {
	count:=0
	//get every channel's first minGroup
	for i := 0; i < clientNums; i++ {
		tmpgroup:=getgroup(i)
		if len(tmpgroup)== 0{
			haveData[i]=false
			continue
		}else{
			haveData[i]=true
			groupMinIndex[i]=0
		}
		minTop[i]=tmpgroup;
	}

	var lastOutput int64=0
	//start processing
	for{
		for{
			var minValue int64=9223372036854775807
			clientMinIndex:=-1
			for i:=0;i<clientNums;i++{
				if haveData[i]==false{
					continue
				}
				if minTop[i][groupMinIndex[i]].commit<=minValue{
					minValue=minTop[i][groupMinIndex[i]].commit
					clientMinIndex=i
				}
			}
			if clientMinIndex!=-1{
				if minValue<lastOutput && debug{
					fmt.Println("==========================Attention->ERROR==============================")
				}
				fmt.Println(minTop[clientMinIndex][groupMinIndex[clientMinIndex]])
				lastOutput=minValue
				count++
				groupMinIndex[clientMinIndex]++
				if(groupMinIndex[clientMinIndex]>=len(minTop[clientMinIndex])){
					//one channel's data run out, need get new data
					break
				}
				
			}else{
				//no min:return
				break
			}
		}
		stillok:=false
		for i := 0; i < clientNums; i++ {
			if haveData[i]==false{
				continue
			}
			
			//remove data already output
			minTop[i]=append([]data{},minTop[i][groupMinIndex[i]:]...)
			groupMinIndex[i]=0

			//get new data
			tmpgroup:=getgroup(i)
			minTop[i]=append(minTop[i],tmpgroup...)
			if len(minTop[i])==0{
				haveData[i]=false
				if debug{
					fmt.Println("client have no data, client index:",i)
				}
				continue
			}
			/*if debug{
				fmt.Println(i,":",minTop[i])
			}*/
			stillok=true
		}
		if stillok==false{
			break
		}
	}
	if debug{
		fmt.Println("count:",count)
	}
}

/*
 * generate prepare and commit datas.
 * assume max difference of send time between prepare and commit data is 2*maxSleepInterval(millisecond),
 * thus u would't think some extreme cases about thread starvation.
 */
func generateDatas(index int) {
	for i := 0; i < messageNums; i++ {
		prepare := incrementToken()
		sleep(maxSleepInterval)

		dataStreaming[index] <- data{
			kind:    "prepare",
			prepare: prepare,
		}
		sleep(maxSleepInterval)

		commit := incrementToken()
		sleep(maxSleepInterval)

		dataStreaming[index] <- data{
			kind:    "commit",
			prepare: prepare,
			commit:  commit,
		}
		sleep(10 * maxSleepInterval)
	}
}

func incrementToken() int64 {
	return atomic.AddInt64(&token, rand.Int63()%maxGap+1)
}

func sleep(factor int64) {
	interval := atomic.AddInt64(&step, 3)%factor + 1
	waitTime := time.Duration(rand.Int63() % interval)
	time.Sleep(waitTime * time.Millisecond)
}
