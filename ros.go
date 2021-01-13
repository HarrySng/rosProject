package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/batchatco/go-native-netcdf/netcdf"
	"github.com/batchatco/go-native-netcdf/netcdf/api"
)

var file string
var routines int

func main() {
	routines = 10
	file = os.Args[1] // Netcdf file name provided by shell script

	start := time.Now()

	swe := getData("SWE")
	rain := getData("RAINF")
	snow := getData("SNOWF")
	runoff := getData("RUNOFF")

	var wg sync.WaitGroup
	sem := make(chan int, routines)
	for i := 0; i < len(swe); i++ {
		wg.Add(1) // Send a signal to the workgroup that an iteration has initiated
		f := strconv.Itoa(i+1) + ".txt"
		go calcROS(sem, swe[i], rain[i], snow[i], runoff[i], f, &wg)
	}
	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("The call took %v to run.\n", elapsed)
}

func getData(varName string) []interface{} {

	nc, err := netcdf.Open(file)
	handleError(err)

	defer nc.Close()

	latLen := getLen(nc, "lat")
	lonLen := getLen(nc, "lon")

	vg, err := nc.GetVariable(varName)
	handleError(err)
	vf := vg.Values
	vr := reflect.ValueOf(vf)

	var d []interface{}

	for lat := 0; lat < latLen; lat++ { // Loop across 1st dim
		for lon := 0; lon < lonLen; lon++ { // Loop across 2nd dim
			d = append(d, vr.Index(lat).Interface().([][]float32)[lon])
		}
	}
	return d
}

func getLen(nc api.Group, d string) int {
	vr, err := nc.GetVariable(d)
	handleError(err)
	dim := vr.Values.([]float64)
	return len(dim)
}

func calcROS(sem chan int, sweall interface{}, rainall interface{}, snowall interface{}, runoffall interface{}, f string, wg *sync.WaitGroup) {

	defer wg.Done()
	sem <- 1
	defer func() {
		<-sem
	}()

	v0 := reflect.ValueOf(sweall)
	v1 := reflect.ValueOf(rainall)
	v2 := reflect.ValueOf(snowall)
	v3 := reflect.ValueOf(runoffall)

	var ros []int
	for i := 0; i < v0.Len()-1; i++ {
		swe := v0.Index(i).Interface().(float32)
		sweNext := v0.Index(i + 1).Interface().(float32)
		rain := v1.Index(i).Interface().(float32)

		if swe > 10.0 && rain > 1.0 && sweNext < swe {
			ros = append(ros, 1)
		} else {
			ros = append(ros, 0)
		}
	}
	file, err := os.Create(f)
	handleError(err)
	defer file.Close()

	w := csv.NewWriter(file)
	defer w.Flush()
	for i := 0; i < len(ros); i++ {
		_, err := file.WriteString(fmt.Sprintf("%v, %v, %v, %v, %v\n", ros[i], v0.Index(i), v1.Index(i), v2.Index(i), v3.Index(i)))
		handleError(err)
	}

}

func handleError(err error) {
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}
	return
}
