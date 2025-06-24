package main

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var targetColumnsInfo = map[string][]string{
	"SKU/id":                    {"id", "sku", "product_id", "productid", "item_id", "item_sku"},
	"Name":                      {"title", "product_name", "name", "item_name", "product_title"},
	"Price":                     {"price", "regular_price", "current_price", "base_price"},
	"Shipping costs":            {"shipping", "shipping_cost", "shipping_price", "shipping_fee"},
	"Stock status":              {"availability", "in_stock", "stock_status", "inventory_status", "stock"},
	"Delivery time":             {"delivery_time", "shipping_time", "estimated_delivery", "delivery_estimate"},
	"Manufacturer":              {"brand", "manufacturer", "vendor", "supplier"},
	"EAN/GTIN":                  {"gtin", "ean", "upc", "isbn", "barcode"},
	"Manufacturer SKU / MPN":    {"mpn", "manufacturer_sku", "manufacturer_part_number", "mfr_part_no"},
	"URL":                       {"link", "product_url", "url", "item_url", "product_link"},
	"Image URL":                 {"image_link", "image_url", "image", "primary_image", "main_image"},
	"Category":                  {"google_product_category", "category", "product_category", "product_type", "categories"},
	"Description":               {"description", "product_description", "long_description", "full_description"},
	"Condition":                 {"condition", "product_condition", "item_condition"},
	"Sale Price":                {"sale_price", "discounted_price", "special_price", "promo_price"},
	"Sale Price Effective Date": {"sale_price_effective_date", "sale_start_date", "promotion_date"},
	"Color":                     {"color", "item_color", "product_color", "variant_color"},
	"Size":                      {"size", "item_size", "product_size", "variant_size"},
	"SizeSystem":                {"size_system", "sizesystem", "size_type"},
	"AdultContent":              {"adult", "adult_content", "is_adult", "adult_product"},
	"AgeGroup":                  {"age_group", "agegroup", "target_age"},
	"Bundled":                   {"bundled", "is_bundle", "bundle"},
	"EnergyEfficiencyClass":     {"energy_efficiency_class", "energy_class", "energy_rating"},
	"Gender":                    {"gender", "target_gender", "gender_audience"},
	"GroupId":                   {"group_id", "groupid", "variant_group_id", "parent_id"},
	"Material":                  {"material", "product_material", "fabric"},
	"Multipack":                 {"multipack", "is_multipack", "multipack_quantity"},
	"Pattern":                   {"pattern", "product_pattern", "design_pattern"},
}

func getBestMapping(inputHeaders []string) map[string]string {
	mapping := make(map[string]string)
	lowercaseHeaders := make([]string, len(inputHeaders))
	for i, header := range inputHeaders {
		lowercaseHeaders[i] = strings.ToLower(strings.TrimSpace(header))
	}
	for i, header := range lowercaseHeaders {
		matched := false
		for targetColumn, alternatives := range targetColumnsInfo {
			if matched {
				break
			}
			if strings.ToLower(targetColumn) == header {
				mapping[inputHeaders[i]] = targetColumn
				matched = true
				continue
			}
			for _, alt := range alternatives {
				if alt == header {
					mapping[inputHeaders[i]] = targetColumn
					matched = true
					break
				}
			}
		}
		if !matched {
			mapping[inputHeaders[i]] = inputHeaders[i]
		}
	}
	return mapping
}

func processCSV(inputFile, outputFile, baseURL string) {
	startTime := time.Now()
	fmt.Println("⏳ Processing started...")

	inFile, err := os.Open(inputFile)
	if err != nil {
		fmt.Printf("Error opening input file: %v\n", err)
		return
	}
	defer inFile.Close()

	outputDir := filepath.Dir(outputFile)
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		fmt.Printf("Error creating output directory: %v\n", err)
		return
	}

	outFile, err := os.Create(outputFile)
	if err != nil {
		fmt.Printf("Error creating output file: %v\n", err)
		return
	}
	defer outFile.Close()

	gzipWriter := gzip.NewWriter(outFile)
	defer gzipWriter.Close()
	writer := bufio.NewWriter(gzipWriter)
	defer writer.Flush()

	reader := csv.NewReader(bufio.NewReader(inFile))
	reader.Comma = '\t' // Set for tab-delimited input
	reader.LazyQuotes = true
	reader.FieldsPerRecord = -1 // Allow inconsistent field counts

	header, err := reader.Read()
	if err != nil {
		fmt.Printf("Error reading header: %v\n", err)
		return
	}

	columnMapping := getBestMapping(header)
	outputHeaders := make([]string, len(header))
	for i, h := range header {
		outputHeaders[i] = columnMapping[h]
	}

	urlColIndex := -1
	for i, col := range outputHeaders {
		if col == "URL" {
			urlColIndex = i
			break
		}
	}

	writer.WriteString(strings.Join(outputHeaders, "\t") + "\n")

	rowCount := 0
	for {
		row, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("Error reading row %d: %v\n", rowCount+2, err) // +2 to include header and 1-based row
			continue
		}

		for len(row) < len(outputHeaders) {
			row = append(row, "")
		}

		if urlColIndex >= 0 && urlColIndex < len(row) && row[urlColIndex] != "" {
			cleanLink := strings.Split(row[urlColIndex], "?")[0]
			encodedLink := url.QueryEscape(cleanLink)
			if baseURL != "" {
				row[urlColIndex] = fmt.Sprintf("%s&cu=%s&fbu=%s", baseURL, encodedLink, encodedLink)
			}
		}

		writer.WriteString(strings.Join(row, "\t") + "\n")
		rowCount++

		if rowCount%100000 == 0 {
			fmt.Printf("Processed %d rows...\n", rowCount)
			writer.Flush()
		}
	}

	writer.Flush()
	gzipWriter.Flush()

	fmt.Printf("✅ Processing complete. Final row count: %d\n", rowCount)
	fmt.Printf("⏱️ Execution Time: %.2f seconds\n", time.Since(startTime).Seconds())

	fmt.Println("\nColumn mapping used:")
	for original, mapped := range columnMapping {
		if original != mapped {
			fmt.Printf("  '%s' → '%s'\n", original, mapped)
		}
	}
}

func main() {
	inputFileFlag := flag.String("input", "", "Input TSV/TXT file path")
	outputFileFlag := flag.String("output", "", "Output TSV.GZ file path")
	baseURLFlag := flag.String("baseurl", "https://klarnashoppingads.ampxdirect.com/?plid=9z0zxe52a9&ctaid=1065&v=1.3&source=als_tiles", "Base URL for link rewriting")
	flag.Parse()

	inputFile := *inputFileFlag
	outputFile := *outputFileFlag
	baseURL := *baseURLFlag

	if inputFile == "" {
		inputFile = "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Nike/UK/nike_UK_en_cpa2.txt"
		fmt.Println("No input file specified, using:", inputFile)
	}
	if outputFile == "" {
		outputFile = "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Nike/UK/amp_klarna_nike_UK.tsv.gz"
		fmt.Println("No output file specified, using:", outputFile)
	}

	processCSV(inputFile, outputFile, baseURL)
}
