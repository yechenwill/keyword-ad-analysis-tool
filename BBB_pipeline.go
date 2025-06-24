package main

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"time"
)

func main() {
	start := time.Now()

	// Step 1: Unzip .gz CSV file to intermediate .tsv file
	gzInput := "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/BedBathBeyond/BedBathBeyond_PLA.csv.gz"
	intermediateTSV := "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/BedBathBeyond/BBB_admarketplace.tsv"
	outputGZ := "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/BedBathBeyond/amp_klarna_bedbathbeyond_us.tsv.gz"
	baseURL := "https://klarnashoppingads.ampxdirect.com/?plid=9z0zxe52a9&ctaid=25116&v=1.3&source=als_tiles"

	if err := convertGzCsvToCleanTSV(gzInput, intermediateTSV); err != nil {
		fmt.Println("❌ Error during CSV to TSV conversion:", err)
		return
	}

	if err := appendEncodedLinks(intermediateTSV, outputGZ, baseURL); err != nil {
		fmt.Println("❌ Error during link encoding:", err)
		return
	}

	fmt.Printf("✅ All processing done in %.2f seconds\n", time.Since(start).Seconds())
}

func convertGzCsvToCleanTSV(gzFilePath, tsvPath string) error {
	gzFile, err := os.Open(gzFilePath)
	if err != nil {
		return err
	}
	defer gzFile.Close()

	gzReader, err := gzip.NewReader(gzFile)
	if err != nil {
		return err
	}
	defer gzReader.Close()

	outFile, err := os.Create(tsvPath)
	if err != nil {
		return err
	}
	defer outFile.Close()

	reader := csv.NewReader(gzReader)
	reader.Comma = ','
	reader.LazyQuotes = true
	writer := csv.NewWriter(outFile)
	writer.Comma = '\t'

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("⚠️ Skipping malformed row:", err)
			continue
		}
		for i, field := range record {
			record[i] = strings.ReplaceAll(field, "\"", "") // remove double quotes
		}
		writer.Write(record)
	}

	writer.Flush()
	return writer.Error()
}

func appendEncodedLinks(inputTSVPath, outputGZPath, baseURL string) error {
	inFile, err := os.Open(inputTSVPath)
	if err != nil {
		return err
	}
	defer inFile.Close()

	outFile, err := os.Create(outputGZPath)
	if err != nil {
		return err
	}
	defer outFile.Close()

	gzipWriter := gzip.NewWriter(outFile)
	defer gzipWriter.Close()

	writer := bufio.NewWriter(gzipWriter)
	reader := csv.NewReader(bufio.NewReader(inFile))
	reader.Comma = '\t'
	reader.LazyQuotes = true

	header, err := reader.Read()
	if err != nil {
		return err
	}

	linkIdx := -1
	for i, col := range header {
		if strings.EqualFold(col, "Link") {
			linkIdx = i
			break
		}
	}

	if linkIdx == -1 {
		return fmt.Errorf("'Link' column not found")
	}

	writer.WriteString(strings.Join(header, "\t") + "\n")
	rowCount := 0

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil || len(record) <= linkIdx {
			fmt.Printf("⚠️ Skipping malformed row at line %d: %v\n", rowCount+2, err)
			continue
		}

		original := strings.Split(record[linkIdx], "?")[0]
		encoded := url.QueryEscape(original)
		record[linkIdx] = fmt.Sprintf("%s&cu=%s&fbu=%s", baseURL, encoded, encoded)
		writer.WriteString(strings.Join(record, "\t") + "\n")
		rowCount++
	}

	writer.Flush()
	return nil
}
