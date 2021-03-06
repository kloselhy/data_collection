package main

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"fmt"
	"sort"
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
	tMax = time.Duration(4*maxSleepInterval)*time.Millisecond       //time threshold for sending
	minGroupStreaming []chan minGroup          //every channel's mingGroup channel,new minGroup will send to it
	minTop = make([]minGroup, clientNums)      //save minGroup for each client for multi-channel join
	haveData = make([]bool, clientNums)        //save the status for each client, true means this client still have data to process
	groupMinIndex = make([]int, clientNums)    //save the min's index for each minGroup
	wg2 sync.WaitGroup                         //a new lock for collector
	debug = false                              //debug switch
)

type data struct {
	kind    string
	prepare int64
	commit  int64
}

type dataGroup []data

/* 
 * definition of type minGroup, and function about sort
 */
type minGroup dataGroup
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
	//add for answer
	minGroupStreaming = make([]chan minGroup, clientNums)
	for i := 0; i < clientNums; i++ {
		minGroupStreaming[i] = make(chan minGroup, messageNums/2)
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

/* 
 * function to get data from the appointed channel in tMax
 */
func getDataInT(index int)(dataGroup){
	timeOut := time.After(tMax)
	var tmpGroup dataGroup
	var v1 data
	needquit:=false
	for{
		select{
			case v1=<-dataStreaming[index]:
				tmpGroup=append(tmpGroup,v1)
				break
			case <-timeOut:
				needquit=true
				break
		}
		if needquit{
			break
		}
		select{
           		case <-timeOut:
				needquit=true
            			break
            		default:
            			break
	    	}
	   	if needquit{
			break
		}
	}
	return tmpGroup
}

/* 
 * function to generate minGroup from the appointed channel continuesly.
 * the minGroup only contains commit data and is already sorted.
 */
func getGroup(index int){
	var Cslice minGroup
	Pmap := make(map[int64]data)
    	Cmap := make(map[int64]data)
	
	var Cmaxlast int64=-9223372036854775808
	var Pmaxlast int64=-9223372036854775808
	var Cmax int64=-9223372036854775808
	var Pmax int64=-9223372036854775808

	noresult:=0
	for{
		tmpgroup:=getDataInT(index)
		if len(tmpgroup)==0{
			noresult++
			if noresult>10 {
				break
			}
		}else{
			noresult=0
		}
		var tmpPmax int64=Pmax
		var tmpCmax int64=Cmax
		var Cslicenew minGroup
		Pmapnew := make(map[int64]data)
        	Cmapnew := make(map[int64]data)
		for _,v := range tmpgroup{
			if v.kind=="prepare"{
				Pmapnew[v.prepare]=v
				if v.prepare>tmpPmax{
					tmpPmax=v.prepare
				}
			}
		}
		for _,v := range tmpgroup{
			if v.kind=="commit"{
				if v.commit>tmpCmax{
					tmpCmax=v.commit
				}
				if vp,ok:=Pmapnew[v.prepare];ok{
					delete(Pmapnew,v.prepare)
					if vp.prepare<Pmaxlast || v.commit<Cmaxlast{
						//timeout
						continue
					}
					if v.commit<Cmax{
						Cslice=append(Cslice,v)
					}else{
						Cslicenew=append(Cslicenew,v)
					}

				}else{
					Cmapnew[v.prepare]=v
				}
			}
        	}
		for prepare:= range Pmapnew{
			if prepare<Pmaxlast{
				//timeout
				delete(Pmapnew,prepare)
			}
			if vc,ok :=Cmap[prepare];ok{
				if vc.commit>Cmaxlast{
					Cslice=append(Cslice,vc)
				}
				delete(Pmapnew,prepare)
			}
		}
		for prepare,vc:= range Cmapnew{
			if vc.commit<Cmaxlast{
				//timeout
				delete(Cmapnew,prepare)
				continue
		    	}
			if _,ok :=Pmap[prepare];ok{
				if vc.commit<Cmax{
					Cslice=append(Cslice,vc)
				}else{
					Cslicenew=append(Cslicenew,vc)
				}	
				delete(Cmapnew,prepare)
			}
       		}
		if len(Cslice)>0{
			sort.Sort(Cslice)
			tmpslice:=Cslice
			minGroupStreaming[index]<-tmpslice
		}
		Cslice=Cslicenew
		Pmap=Pmapnew
		Cmap=Cmapnew
		Pmaxlast=Pmax
		Cmaxlast=Cmax
		Pmax=tmpPmax
		Cmax=tmpCmax
	}
	if debug{
		fmt.Println("quit getgroup:",index)
	}
}
/*
 * 1 assume dataStreamings are endless => we have infinitely many datas;
 * because it's a simulation program, it has a limited number of datas, but the assumption is not shoul be satisfied
 * 2 sort commit kind of datas that are from multiple dataStreamings by commit ascending
 * and output them in the fastest way you can think
 */
func collect() {
	wg2.Add(clientNums)
	for i := 0; i < clientNums; i++ {
		go func(index int) {
		    defer wg2.Done()
		    getGroup(index)
		}(i)
	}
	count:=0
	//get every channel's first minGroup
	for i := 0; i < clientNums; i++ {
		select{
           		case minTop[i]=<-minGroupStreaming[i]:
				groupMinIndex[i]=0
				haveData[i]=true
                		break
            		case <-time.After(tMax*20):
                		haveData[i]=false
                		break
        	}
	}

	var lastOutput int64=0
	//start processing
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
				select{
					case minTop[clientMinIndex]=<-minGroupStreaming[clientMinIndex]:
						groupMinIndex[clientMinIndex]=0
						break
					case <-time.After(tMax*20):
						haveData[clientMinIndex]=false
						break
               			}					
			}
				
		}else{
			//no min
			break
		}
	}
	if debug{
		fmt.Println("count:",count)
	}
	wg2.Wait()
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
