package main

import (
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"os"
	"time"
)

func convertMultilineCsvGzToTsv(gzFilePath, outputTsvPath string, chunkSize int) error {
	start := time.Now()
	fmt.Println("Conversion started at:", start.Format("2006-01-02 15:04:05"))

	// Open gzip file
	file, err := os.Open(gzFilePath)
	if err != nil {
		return fmt.Errorf("failed to open gzip file: %w", err)
	}
	defer file.Close()

	// Create gzip reader
	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer gzReader.Close()

	// Create output TSV file
	outFile, err := os.Create(outputTsvPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	// Create a CSV reader on the gzip stream
	reader := csv.NewReader(gzReader)
	reader.Comma = ','
	reader.LazyQuotes = true
	reader.FieldsPerRecord = -1 // allow variable-length records

	// Set up TSV writer
	writer := csv.NewWriter(outFile)
	writer.Comma = '\t'

	chunk := [][]string{}
	recordCount := 0

	for {
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			fmt.Printf("Skipping malformed row %d: %v\n", recordCount+1, err)
			continue
		}
		chunk = append(chunk, record)
		recordCount++

		if len(chunk) >= chunkSize {
			if err := writer.WriteAll(chunk); err != nil {
				return fmt.Errorf("error writing chunk: %w", err)
			}
			chunk = nil
		}
	}

	if len(chunk) > 0 {
		if err := writer.WriteAll(chunk); err != nil {
			return fmt.Errorf("error writing final chunk: %w", err)
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return fmt.Errorf("flush error: %w", err)
	}

	end := time.Now()
	fmt.Println("Conversion completed at:", end.Format("2006-01-02 15:04:05"))
	fmt.Printf("Total records processed: %d\n", recordCount)
	return nil
}

func main() {
	err := convertMultilineCsvGzToTsv(
		"C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Houzz/Houzz_PLA.txt.gz",
		"C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Houzz/houzz_admarketplace.tsv",
		10000,
	)
	if err != nil {
		fmt.Println("❌ Error:", err)
	} else {
		fmt.Println("✅ Conversion finished successfully.")
	}
}
