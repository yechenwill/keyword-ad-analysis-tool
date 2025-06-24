package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	bucketName  = "amp-general-sftp-external"
	s3Folder    = "l_klarnapricerun/files/" // e.g., "klarna-uploads/"
	region      = "us-east-1"               // change if needed
	maxParallel = 5
)

var advertisers = map[string]string{
	"LookFantastic_UK": "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Look Fantastic/UK/",
	"LookFantastic_FR": "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Look Fantastic/FR/",
	"LookFantastic_IT": "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Look Fantastic/IT/",
	"Sephora_UK":       "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Sephora/",
	"MyProtein_UK":     "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/My Protein/UK/",
	"Vodafone_UK":      "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Vodafone/",
	"BedBathBeyond_US": "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/BedBathBeyond/",
	"Bloomingdales_US": "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Bloomingdales/",
	"HarryDavid_US":    "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/HarryDavid/",
	"Houzz_US":         "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Houzz/",
	"NewBalance_US":    "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/NewBalance/",
	"TheHomeDepot_US":  "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/TheHomeDepot/",
	"TommyBahama_US":   "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/TommyBahama/",
	"Ulta_US":          "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Ulta/",
	"Verizon_US":       "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Verizon/",
	"Wayfair_US":       "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Wayfair/",
	"Zappos_US":        "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Zappos/",
	"UnderArmour_US":   "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/UnderArmour/",
	"JCPenney_US":      "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/JCPenney/",
	// "Nike_UK":          "C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Nike/UK/",
}

func uploadToS3(s3Client *s3.Client, localPath, s3Key string, wg *sync.WaitGroup, sem chan struct{}) {
	defer wg.Done()
	sem <- struct{}{}
	defer func() { <-sem }()

	file, err := os.Open(localPath)
	if err != nil {
		log.Printf("❌ Failed to open file: %s | %v", localPath, err)
		return
	}
	defer file.Close()

	_, err = s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(s3Key),
		Body:   file,
	})
	if err != nil {
		log.Printf("❌ Upload to S3 failed: %s | %v", s3Key, err)
		return
	}

	log.Printf("✅ Uploaded: %s -> s3://%s/%s", localPath, bucketName, s3Key)
}

func main() {
	start := time.Now()

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
		log.Fatalf("❌ Failed to load AWS config: %v", err)
	}
	s3Client := s3.NewFromConfig(cfg)

	var wg sync.WaitGroup
	sem := make(chan struct{}, maxParallel)

	for advertiser, folder := range advertisers {
		pattern := fmt.Sprintf("amp_klarna_%s.tsv.gz", strings.ToLower(advertiser))
		matches, err := filepath.Glob(filepath.Join(folder, pattern))
		if err != nil {
			log.Printf("⚠️  Failed to search files for %s: %v", advertiser, err)
			continue
		}
		if len(matches) == 0 {
			log.Printf("⚠️  No files found for %s", advertiser)
			continue
		}

		for _, file := range matches {
			info, err := os.Stat(file)
			if err != nil || info.IsDir() {
				log.Printf("⚠️  Skipping: %s", file)
				continue
			}

			s3Key := s3Folder + filepath.Base(file)

			wg.Add(1)
			go uploadToS3(s3Client, file, s3Key, &wg, sem)
		}
	}

	wg.Wait()
	log.Printf("🚀 All uploads completed in %.2fs\n", time.Since(start).Seconds())
}
