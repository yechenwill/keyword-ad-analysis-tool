package main

import (
	"bufio"
	"fmt"
	"os"
)

// Read and print the first 30 rows of a TSV file
func readFirst30Rows(filePath string) {
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	fmt.Println("📌 First 30 Rows:")
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
	// Change this path to your processed TSV file
	filePath := "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Wayfair/wayfair_output.tsv"
	readFirst30Rows(filePath)
}
