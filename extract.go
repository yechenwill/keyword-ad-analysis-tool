package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"os"
	"strings"
)

// Extracts the first 30 rows from a TSV.GZ file and saves as TSV or CSV
func extractFirst30Rows(inputFile, outputFile string, isCSV bool) {
	// Open the compressed .tsv.gz file
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

	// Create a scanner to read the decompressed content line by line
	scanner := bufio.NewScanner(gzReader)

	// Create output file
	outFile, err := os.Create(outputFile)
	if err != nil {
		panic(err)
	}
	defer outFile.Close()

	writer := bufio.NewWriter(outFile)
	defer writer.Flush()

	fmt.Println("📌 Extracting first 30 rows...")

	lineCount := 0
	for scanner.Scan() {
		line := scanner.Text()

		// Convert TSV to CSV if needed
		if isCSV {
			line = strings.ReplaceAll(line, "\t", ",")
		}

		// Write to output file
		_, err := writer.WriteString(line + "\n")
		if err != nil {
			panic(err)
		}

		lineCount++
		if lineCount >= 30 {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	fmt.Printf("\n✅ Extracted first %d rows to %s\n", lineCount, outputFile)
}

func main() {
	// Input and output file paths
	inputFile := "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Wayfair/amp_klarna_wayfair.tsv.gz"
	outputFile := "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Wayfair/wayfair_first_30.tsv"

	// Set `true` for CSV output, `false` for TSV output
	extractFirst30Rows(inputFile, outputFile, false)
}
