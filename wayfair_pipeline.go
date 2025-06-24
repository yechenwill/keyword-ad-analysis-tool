package main

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

func main() {
	startTime := time.Now()
	fmt.Println("🚀 Pipeline started...")

	// // Step 1: Extract CSV from archive
	// stepStart := time.Now()
	// extractCSV()
	// fmt.Printf("✅ Step 1 completed in %.2f seconds\n", time.Since(stepStart).Seconds())

	// Step 2: Process CSV and convert to TSV.GZ in parallel
	stepStart = time.Now()
	processCSVParallel()
	fmt.Printf("✅ Step 2 completed in %.2f seconds\n", time.Since(stepStart).Seconds())

	// // Step 3: Upload processed file to SFTP
	// stepStart = time.Now()
	// uploadSFTP()
	// fmt.Printf("✅ Step 3 completed in %.2f seconds\n", time.Since(stepStart).Seconds())

	// fmt.Printf("🚀 Pipeline completed in %.2f seconds\n", time.Since(startTime).Seconds())
}

// // 📂 Extract CSV from ZIP
// func extractCSV() {
// 	fmt.Println("📂 Extracting CSV from archive...")
// 	archivePath := "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Wayfair/downloaded_archive.zip"
// 	outputDir := "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Wayfair/extracted"

// 	os.MkdirAll(outputDir, os.ModePerm)

// 	r, err := zip.OpenReader(archivePath)
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer r.Close()

// 	for _, f := range r.File {
// 		if filepath.Ext(f.Name) == ".csv" {
// 			outPath := filepath.Join(outputDir, f.Name)
// 			rc, err := f.Open()
// 			if err != nil {
// 				panic(err)
// 			}
// 			defer rc.Close()

// 			outFile, err := os.Create(outPath)
// 			if err != nil {
// 				panic(err)
// 			}
// 			defer outFile.Close()

// 			_, err = io.Copy(outFile, rc)
// 			if err != nil {
// 				panic(err)
// 			}
// 			fmt.Println("✅ Extraction complete:", outPath)
// 		}
// 	}
// }

// 📄 Process CSV in Parallel & Convert to TSV.GZ
func processCSVParallel() {
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
		"product_type":            "Category",
		"google_product_category": "Shipping costs",
	}
	fmt.Println("📄 Processing CSV in parallel...")
	inputFile := "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Wayfair/extracted/wayfair_data.csv"
	outputFile := "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Wayfair/processed/wayfair_output.tsv.gz"

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

	gzipWriter := gzip.NewWriter(outFile)
	defer gzipWriter.Close()

	writer := bufio.NewWriter(gzipWriter)
	defer writer.Flush()

	reader := csv.NewReader(bufio.NewReader(inFile))
	reader.Comma = ','
	reader.LazyQuotes = true

	numWorkers := runtime.NumCPU()
	fmt.Printf("🚀 Running with %d parallel workers\n", numWorkers)

	var wg sync.WaitGroup
	lineChan := make(chan []string, 10000)

	// Worker Goroutines
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for row := range lineChan {
				cleanLink := strings.Split(row[1], "?")[0]
				encodedLink := url.QueryEscape(cleanLink)
				row[1] = "https://klarnashoppingads.ampxdirect.com/?partner=klarnashoppingads&sub1=shoppingads&ctaid=74894&v=1.3&source=als_tiles&cu=" + encodedLink + "&fbu=" + encodedLink // Modify URL column
				writer.WriteString(strings.Join(row, "\t") + "\n")
			}
			writer.Flush()
		}()
	}

	// Read CSV rows
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

	rowCount := 0
	for {
		row, err := reader.Read()
		if err != nil {
			break
		}
		lineChan <- row
		rowCount++
		if rowCount%100000 == 0 {
			fmt.Printf("Processed %d rows...\n", rowCount)
		}
	}

	close(lineChan)
	wg.Wait()
	fmt.Printf("✅ Processing complete. Final row count: %d\n", rowCount)
}

// // 📤 Upload Processed File to SFTP
// func uploadSFTP() {
// 	fmt.Println("📤 Uploading processed file to SFTP...")

// 	sftpHost := "sftp.server.com"
// 	sftpUser := "username"
// 	sftpPass := "password"

// 	localFile := "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Wayfair/processed/wayfair_output.tsv.gz"
// 	remotePath := "/remote/folder/wayfair_output.tsv.gz"

// 	config := &ssh.ClientConfig{
// 		User: sftpUser,
// 		Auth: []ssh.AuthMethod{ssh.Password(sftpPass)},
// 		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
// 	}

// 	conn, err := ssh.Dial("tcp", sftpHost+":22", config)
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer conn.Close()

// 	client, err := sftp.NewClient(conn)
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer client.Close()

// 	srcFile, err := os.Open(localFile)
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer srcFile.Close()

// 	dstFile, err := client.Create(remotePath)
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer dstFile.Close()

// 	fmt.Println("Uploading file...")
// 	_, err = dstFile.ReadFrom(srcFile)
// 	if err != nil {
// 		panic(err)
// 	}

// 	fmt.Println("✅ Upload complete!")
// }
