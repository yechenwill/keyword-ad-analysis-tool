package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"os"
	"strings"
)

func searchRecord(inputFile, keyword string) {
	// Open the compressed file
	file, err := os.Open(inputFile)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// Create a gzip reader
	gzReader, err := gzip.NewReader(file)
	if err != nil {
		panic(err)
	}
	defer gzReader.Close()

	// Scan line by line
	scanner := bufio.NewScanner(gzReader)
	lineCount := 0
	titleIndex := -1

	// Read and process the header
	if scanner.Scan() {
		header := strings.Split(scanner.Text(), "\t")
		for i, col := range header {
			if col == "GTIN" {
				titleIndex = i
				break
			}
		}
		if titleIndex == -1 {
			fmt.Println("❌ 'title' column not found")
			return
		}
	}

	// Search for the record
	for scanner.Scan() {
		line := scanner.Text()
		columns := strings.Split(line, "\t")

		if titleIndex < len(columns) && strings.Contains(columns[titleIndex], keyword) {
			fmt.Println("✅ Found Record:", line)
			return
		}

		lineCount++
		if lineCount%100000 == 0 {
			fmt.Printf("🔍 Searched %d lines...\n", lineCount)
		}
	}

	fmt.Println("❌ Record not found")
}

func main() {
	inputFile := "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/BedBathBeyond/amp_klarna_bedbathbeyond.tsv.gz"
	searchRecord(inputFile, "729106358388")
}
