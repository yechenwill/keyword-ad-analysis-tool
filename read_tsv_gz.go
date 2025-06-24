package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"os"
)

// Read and print the first 30 rows of a TSV.GZ file
func readFirst30Rows(filePath string) {
	// Open the compressed .tsv.gz file
	file, err := os.Open(filePath)
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

	// Create a scanner to read the decompressed content line by line
	scanner := bufio.NewScanner(gzReader)

	fmt.Println("📌 First 30 Rows of", filePath, ":")
	lineCount := 0
	for scanner.Scan() {
		fmt.Println(scanner.Text()) // Print each line
		lineCount++
		if lineCount >= 30 {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	fmt.Printf("\n✅ Displayed first %d rows.\n", lineCount)
}

func main() {
	// Change this path to your compressed TSV file
	filePath := "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Wayfair/amp_klarna_wayfair.tsv.gz"
	readFirst30Rows(filePath)
}
