package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
)

// Read and print the first 30 rows of a CSV file (efficient for large files)
func readFirst30Rows(filePath string) {
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := csv.NewReader(bufio.NewReader(file))
	reader.Comma = ',' // Adjust if your CSV uses a different separator

	fmt.Println("📌 First 30 Rows of", filePath, ":")
	rowCount := 0
	for {
		record, err := reader.Read()
		if err != nil {
			break // Stop reading at the end of the file
		}

		fmt.Println(record) // Print the row
		rowCount++
		if rowCount >= 30 {
			break
		}
	}

	fmt.Printf("\n✅ Displayed first %d rows.\n", rowCount)
}

func main() {
	// Change this to your CSV file path
	filePath := "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Wayfair/wayfair_admarketplace.csv"
	readFirst30Rows(filePath)
}
