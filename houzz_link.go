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

// Renaming and Modifying URL Function
func processTSV(inputFile, outputFile string) {
	startTime := time.Now()
	fmt.Println("⏳ Processing started...")

	columnMapping := map[string]string{
		"id":            "SKU/id",
		"item_group_id": "GroupId",
		"title":         "Name",
		"brand":         "Manufacturer",
		"link":          "URL",
		"price":         "Price",
		"sale_price":    "Sale Price",
		"description":   "Description",
		"image_link":    "Image URL",
		"mpn":           "Manufacturer SKU / MPN",
		"gtin":          "EAN/GTIN",
		"availability":  "Stock status",
		"condition":     "Condition",
		// "product_type":            "Category",
		"google_product_category": "Category",
		"shipping":                "Shipping costs",
		"availability_date":       "Delivery time",
		"adult":                   "AdultContent",
		"age_group":               "AgeGroup",
		"color":                   "Color",
		"material":                "Material",
		"pattern":                 "Pattern",
		"size":                    "Size",
		"size_system":             "SizeSystem",
	}

	baseURL := "https://klarnashoppingads.ampxdirect.com/?plid=9z0zxe52a9&ctaid=1049&v=1.3&source=als_tiles"

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
	reader.Comma = '\t'
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
			if err == io.EOF {
				break
			}
			fmt.Printf("⚠️ Skipping malformed row at line %d: %v\n", rowCount+2, err)
			continue
		}

		// Modify the "URL" column
		for i, col := range header {
			if col == "URL" && len(row) > i {
				cleanLink := strings.Split(row[i], "?")[0]
				encodedLink := url.QueryEscape(cleanLink)
				row[i] = fmt.Sprintf("%s&cu=%s&fbu=%s", baseURL, encodedLink, encodedLink)
			}
		}

		// Write to compressed file
		writer.WriteString(strings.Join(row, "\t") + "\n")
		rowCount++

		if rowCount%100000 == 0 {
			fmt.Printf("Processed %d rows...\n", rowCount)
			writer.Flush()
		}
	}

	fmt.Printf("✅ Processing complete. Final row count: %d\n", rowCount)
	fmt.Printf("⏱️ Execution Time: %.2f seconds\n", time.Since(startTime).Seconds())
}

func main() {
	inputFile := "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Houzz/houzz_admarketplace.tsv"
	outputFile := "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Houzz/amp_klarna_houzz_us.tsv.gz"

	processTSV(inputFile, outputFile)
}
