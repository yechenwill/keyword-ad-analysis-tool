package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"os"
	"strings"
)

func main() {
	// Replace with your actual file path
	filePath := "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Houzz/houzz_full_catalog.txt.gz"

	// Open the .gz file
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	// Create a gzip reader
	gzReader, err := gzip.NewReader(file)
	if err != nil {
		fmt.Println("Error creating gzip reader:", err)
		return
	}
	defer gzReader.Close()

	// Read lines from the decompressed file
	scanner := bufio.NewScanner(gzReader)
	lines := []string{}
	maxLines := 10
	for scanner.Scan() && len(lines) < maxLines {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
		return
	}

	// If no lines found
	if len(lines) == 0 {
		fmt.Println("No lines read from file.")
		return
	}

	// Candidate delimiters to test
	delimiters := []rune{'\t', ',', '|', ';', ':'}

	// Detect most likely delimiter
	type delimScore struct {
		delim rune
		score int
	}
	var best delimScore

	for _, d := range delimiters {
		counts := make(map[int]int)
		for _, line := range lines {
			fields := strings.Split(line, string(d))
			counts[len(fields)]++
		}

		// Score: how consistent is the field count
		mostCommonCount := 0
		for _, c := range counts {
			if c > mostCommonCount {
				mostCommonCount = c
			}
		}

		if mostCommonCount > best.score {
			best = delimScore{delim: d, score: mostCommonCount}
		}
	}

	fmt.Printf("Detected delimiter: '%c'\n\n", best.delim)
	fmt.Println("Sample lines:")
	for _, line := range lines {
		fmt.Println(line)
	}
}
