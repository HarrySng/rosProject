package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type fluxData struct {
	rain   float64
	snow   float64
	swe    float64
	runoff float64
}

func main() {
	start := time.Now()
	files, err := filepath.Glob("./*")
	handleError(err)

	processGrids(files)

	routines := 500
	var wg sync.WaitGroup
	sem := make(chan int, routines)

	for i := range files {
		wg.Add(1)
		newFile := "../data/" + strconv.Itoa(i+1) + ".csv"
		go processFlux(sem, files[i], newFile, &wg)
	}
	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("The call took %v to run.\n", elapsed)
}

func processGrids(files []string) {
	/*
		Create a 2D slice with
			n rows (n = num files)
			2 columns (lat, lon)
		Iterate over file names, split each name
		Extract lat, lon and convert from string to float
		Append lat,lon as a row to 2D slice
	*/

	latLon := make([][]float64, len(files)) // 2D slice to hold lat lon in n rows and 2 columns

	for i := range latLon {
		s := strings.Split(files[i], "_")        // Split on underscore
		lat, err := strconv.ParseFloat(s[1], 64) // String to float (lat)
		handleError(err)
		lon, err := strconv.ParseFloat(s[2], 64) // String to float (lon)
		handleError(err)
		latLon[i] = []float64{lat, lon} // Append a row to 2D slice (lat, lon)
	}

	// Write to file
	f, err := os.Create("../data/grid.txt")
	handleError(err)
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	for i := range latLon {
		_, err := f.WriteString(fmt.Sprintf("%v,%v\n", latLon[i][0], latLon[i][1]))
		handleError(err)
	}
}

func processFlux(sem chan int, fluxFile string, newFile string, wg *sync.WaitGroup) {
	/*
		The data is tab separated
		Iterate over each row of file
		Extract the required column numbers (5,7,8,9)
		Convert from string to float
		Put all 4 values from each row into custom struct
		Append custom struct to a slice of structs
		Final output is a []fluxData (len = 55152)
		Where each fluxData is {rain, snow, swe, runoff}
	*/

	defer wg.Done()
	sem <- 1
	defer func() {
		<-sem
	}()

	ff, err := os.Open(fluxFile)
	handleError(err)

	defer ff.Close()

	reader := csv.NewReader(ff)
	reader.Comma = '\t' // Change the delimiter to tab instead of comma (default for csv)

	allData, err := reader.ReadAll()
	handleError(err)

	var oneRow fluxData // Hold a single row
	var data []fluxData // Slice to hold all rows

	for _, row := range allData {
		oneRow.rain = getVal(row, 8)
		oneRow.snow = getVal(row, 9)
		oneRow.swe = getVal(row, 7)
		oneRow.runoff = getVal(row, 5)
		data = append(data, oneRow)
	}

	// Now write it to file
	nf, err := os.Create(newFile)
	handleError(err)
	defer nf.Close()

	w := csv.NewWriter(nf)
	defer w.Flush()

	// Calculate ros on the fly and write to file
	var ros int
	for i := range data {
		ros = 0
		if i == 0 {
			i = 1
		}
		if data[i].rain > 0 && data[i].swe > 0 && data[i].swe < data[i-1].swe {
			ros = 1
		}
		_, err := nf.WriteString(fmt.Sprintf("%v,%v,%v,%v,%v\n", data[i].rain, data[i].snow, data[i].swe, data[i].runoff, ros))
		handleError(err)
	}
}

func getVal(r []string, i int) float64 {
	stringVal := r[i]
	// Data is separated by a tab+space
	s := strings.Split(stringVal, " ")       // Splitting will put the required numeric on index 1
	val, err := strconv.ParseFloat(s[1], 64) // Convert it t float
	handleError(err)
	return val
}

func handleError(err error) {
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}
	return
}
