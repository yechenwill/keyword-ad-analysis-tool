package main

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"
)

// Renaming and Modifying URL Function
func processCSV(inputFile, outputFile string) {
	startTime := time.Now()
	fmt.Println("⏳ Processing started...")

	columnMapping := map[string]string{
		"id":                      "SKU/id",
		"item_group_id":           "GroupId",
		"title":                   "Name",
		"brand":                   "Manufacturer",
		"link":                    "URL",
		"price":                   "Price",
		"sale_price":              "Sale Price",
		"description":             "Description",
		"image_link":              "Image URL",
		"mpn":                     "Manufacturer SKU / MPN",
		"gtin":                    "EAN/GTIN",
		"availability":            "Stock status",
		"condition":               "Condition",
		"google_product_category": "Category",
	}

	baseURL := "https://klarnashoppingads.ampxdirect.com/?plid=9z0zxe52a9&ctaid=1169&v=1.3&source=als_tiles"

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

	// Use gzip writer for compressed output
	gzipWriter := gzip.NewWriter(outFile)
	defer gzipWriter.Close()

	writer := bufio.NewWriter(gzipWriter)
	defer writer.Flush()

	reader := csv.NewReader(bufio.NewReader(inFile))
	reader.Comma = ','
	reader.LazyQuotes = true

	// Read and process header
	header, err := reader.Read()
	if err != nil {
		panic(err)
	}
	for i, col := range header {
		if newName, exists := columnMapping[col]; exists {
			header[i] = newName
		}
	}
	writer.WriteString(strings.Join(header, "\t") + "\n")

	// Process rows
	rowCount := 0
	for {
		row, err := reader.Read()
		if err != nil {
			break
		}

		// Modify the "URL" column
		for i, col := range header {
			if col == "URL" && len(row) > i {
				cleanLink := strings.Split(row[i], "?")[0] // Remove query parameters
				encodedLink := url.QueryEscape(cleanLink)  // Encode URL
				row[i] = fmt.Sprintf("%s&cu=%s&fbu=%s", baseURL, encodedLink, encodedLink)
			}
		}

		// Write to compressed file
		writer.WriteString(strings.Join(row, "\t") + "\n")
		rowCount++

		// Log progress every 100,000 rows
		if rowCount%100000 == 0 {
			fmt.Printf("Processed %d rows...\n", rowCount)
			writer.Flush()
		}
	}

	fmt.Printf("✅ Processing complete. Final row count: %d\n", rowCount)
	fmt.Printf("⏱️ Execution Time: %.2f seconds\n", time.Since(startTime).Seconds())
}

func main() {
	inputFile := "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Wayfair/wayfair_admarketplace.csv"
	outputFile := "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Wayfair/amp_klarna_wayfair.tsv.gz"

	processCSV(inputFile, outputFile)
}
