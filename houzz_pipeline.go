package main

import (
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"time"
)

func processAndConvert(gzInputPath, outputGzPath string, chunkSize int) error {
	start := time.Now()
	fmt.Println("🚀 Processing and conversion started at:", start.Format("2006-01-02 15:04:05"))

	// Input gzip file
	inFile, err := os.Open(gzInputPath)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer inFile.Close()

	// Create gzip reader
	gzReader, err := gzip.NewReader(inFile)
	if err != nil {
		return fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer gzReader.Close()

	// Output gzip file
	outFile, err := os.Create(outputGzPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	gzipWriter := gzip.NewWriter(outFile)
	defer gzipWriter.Close()

	writer := csv.NewWriter(gzipWriter)
	writer.Comma = '\t'
	defer writer.Flush()

	reader := csv.NewReader(gzReader)
	reader.Comma = ','
	reader.LazyQuotes = true
	reader.FieldsPerRecord = -1

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

	var headers []string
	rowCount := 0

	for {
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("⚠️ Skipping malformed row %d: %v\n", rowCount+1, err)
			continue
		}

		// Handle header renaming
		if headers == nil {
			headers = make([]string, len(record))
			for i, col := range record {
				if newCol, ok := columnMapping[col]; ok {
					headers[i] = newCol
				} else {
					headers[i] = col
				}
			}
			if err := writer.Write(headers); err != nil {
				return fmt.Errorf("failed to write headers: %w", err)
			}
			continue
		}

		// Rewrite URL if applicable
		for i, col := range headers {
			if col == "URL" && len(record) > i {
				cleanLink := strings.Split(record[i], "?")[0]
				encodedLink := url.QueryEscape(cleanLink)
				record[i] = fmt.Sprintf("%s&cu=%s&fbu=%s", baseURL, encodedLink, encodedLink)
			}
		}

		if err := writer.Write(record); err != nil {
			fmt.Printf("⚠️ Failed to write row %d: %v\n", rowCount+1, err)
			continue
		}

		rowCount++
		if rowCount%100000 == 0 {
			writer.Flush()
			fmt.Printf("Processed %d rows...\n", rowCount)
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return fmt.Errorf("flush error: %w", err)
	}

	end := time.Now()
	fmt.Printf("✅ Finished. Total rows processed: %d\n", rowCount)
	fmt.Printf("⏱️ Total processing time: %.2f seconds\n", end.Sub(start).Seconds())
	return nil
}

func main() {
	err := processAndConvert(
		"C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Houzz/Houzz_PLA.txt.gz",
		"C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Houzz/amp_klarna_houzz_us.tsv.gz",
		10000,
	)
	if err != nil {
		fmt.Println("❌ Error:", err)
	} else {
		fmt.Println("✅ All done.")
	}
}
