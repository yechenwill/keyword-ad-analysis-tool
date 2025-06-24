package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
)

func main() {
	inputFile := "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Wayfair/wayfair_admarketplace.csv"
	outputFile := "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Wayfair/wayfair_admarketplace.tsv"

	inFile, err := os.Open(inputFile)
	if err != nil {
		panic(err)
	}
	defer inFile.Close()

	outFile, err := os.Create(outputFile)
	if err != nil {
		panic(err)
	}
	defer outFile.Close()

	scanner := bufio.NewScanner(inFile)
	writer := bufio.NewWriter(outFile)

	var wg sync.WaitGroup
	lineChan := make(chan string, 10000)

	// Writer Goroutine
	go func() {
		for line := range lineChan {
			writer.WriteString(line + "\n")
		}
		writer.Flush()
	}()

	// Reader Goroutines
	for scanner.Scan() {
		wg.Add(1)
		line := scanner.Text()
		go func(l string) {
			defer wg.Done()
			tsvLine := strings.ReplaceAll(l, ",", "\t")
			lineChan <- tsvLine
		}(line)
	}

	wg.Wait()
	close(lineChan)

	fmt.Println("✅ CSV converted to TSV successfully!")
}