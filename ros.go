package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/batchatco/go-native-netcdf/netcdf"
	"github.com/batchatco/go-native-netcdf/netcdf/api"
)

var file string
var routines int

func main() {

	routines = 1000
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

	// Get lengths of lat, lon to iterate over
	latLen := getLen(nc, "lat")
	lonLen := getLen(nc, "lon")

	vg, err := nc.GetVariable(varName) // Extract variable interface
	handleError(err)
	vf := vg.Values           // Extract values from variable interface
	vr := reflect.ValueOf(vf) // Reflect the values to iterate over

	ind, err := getGridIndex("../gridIndex.txt") // Indices of liard grids
	handleError(err)

	var d []interface{} // Empty interface to store 3rd dim as vector

	i := 0
	for lat := 0; lat < latLen; lat++ { // Loop across 1st dim
		for lon := 0; lon < lonLen; lon++ { // Loop across 2nd dim
			i++
			if contains(ind, i) { // Proceed only if liard grid index
				d = append(d, vr.Index(lat).Interface().([][]float32)[lon])
			}
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
		swe := v0.Index(i).Interface().(float32)         // SWE on current day
		sweNext := v0.Index(i + 1).Interface().(float32) // SWE on next day
		rain := v1.Index(i).Interface().(float32)        // Rain on current day

		/*
			RoS logic:
			If SWE on ground above threshold on current day
			If it rains above threshold on current day
			If SWE reduces next day
		*/
		if swe > 10.0 && rain > 1.0 && sweNext < swe {
			ros = append(ros, 1) // RoS event happened if all true
		} else {
			ros = append(ros, 0) // Not a RoS event
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

func getGridIndex(f string) ([]int, error) {
	data, err := ioutil.ReadFile(f) // Read file
	handleError(err)
	r := strings.NewReader(string(data)) // From byte to string
	handleError(err)
	scanner := bufio.NewScanner(r) // Use bufio
	scanner.Split(bufio.ScanWords)
	var result []int
	for scanner.Scan() {
		x, err := strconv.Atoi(scanner.Text()) // Convert to int
		handleError(err)
		result = append(result, x-1)
		/*
			CRITICAL: x-1 because go index starts from 0
			while index.txt was made in R with count starting from 1
		*/
	}
	return result, scanner.Err()
}

func contains(ind []int, v int) bool {
	for _, i := range ind {
		if i == v {
			return true
		}
	}
	return false
}

func handleError(err error) {
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}
	return
}
