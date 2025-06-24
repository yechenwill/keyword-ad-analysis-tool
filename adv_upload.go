package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// Configuration
const (
	sftpHost     = "sftp.admarketplace.net"
	sftpPort     = 22
	sftpUser     = "l_klarnapricerun"
	sftpPassword = "9ir5nukn2JGEPDC5AZsiett4"
	sftpTarget   = "/files"
	maxParallel  = 5 // Adjust for concurrency
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

func uploadFile(client *sftp.Client, localPath, remotePath string, wg *sync.WaitGroup, sem chan struct{}) {
	defer wg.Done()
	sem <- struct{}{}        // Acquire
	defer func() { <-sem }() // Release

	srcFile, err := os.Open(localPath)
	if err != nil {
		log.Printf("❌ Failed to open local file: %s | %v", localPath, err)
		return
	}
	defer srcFile.Close()

	dstFile, err := client.Create(remotePath)
	if err != nil {
		log.Printf("❌ Failed to create remote file: %s | %v", remotePath, err)
		return
	}
	defer dstFile.Close()

	if _, err := dstFile.ReadFrom(srcFile); err != nil {
		log.Printf("❌ Failed to upload file: %s | %v", localPath, err)
		return
	}

	log.Printf("✅ Uploaded: %s -> %s", localPath, remotePath)
}

func main() {
	start := time.Now()

	// Setup SSH client config
	config := &ssh.ClientConfig{
		User:            sftpUser,
		Auth:            []ssh.AuthMethod{ssh.Password(sftpPassword)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         10 * time.Second,
	}

	// Dial SSH
	addr := fmt.Sprintf("%s:%d", sftpHost, sftpPort)
	sshConn, err := ssh.Dial("tcp", addr, config)
	if err != nil {
		log.Fatalf("❌ Failed to connect to SFTP server: %v", err)
	}
	defer sshConn.Close()

	// Create SFTP client
	sftpClient, err := sftp.NewClient(sshConn)
	if err != nil {
		log.Fatalf("❌ Failed to start SFTP session: %v", err)
	}
	defer sftpClient.Close()

	// Precompute start of today
	now := time.Now()
	todayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())

	// Upload with concurrency
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxParallel) // Semaphore for concurrency control

	for advertiser, folder := range advertisers {
		pattern := fmt.Sprintf("amp_klarna_%s.tsv.gz", strings.ToLower(advertiser))
		matches, err := filepath.Glob(filepath.Join(folder, pattern))
		if err != nil {
			log.Printf("⚠️  Failed to search files for %s: %v", advertiser, err)
			continue
		}
		if len(matches) == 0 {
			log.Printf("⚠️  No matching files for %s", advertiser)
			continue
		}

		for _, file := range matches {
			info, err := os.Stat(file)
			if err != nil || info.IsDir() {
				log.Printf("⚠️  Skipping non-file: %s", file)
				continue
			}

			remotePath := sftpTarget + "/" + filepath.Base(file)

			// ─── NEW: Check if remote file exists and was uploaded today ───
			if remoteInfo, err := sftpClient.Stat(remotePath); err == nil {
				// file exists on server
				if remoteInfo.ModTime().After(todayStart) || remoteInfo.ModTime().Equal(todayStart) {
					log.Printf("⏭  Skipping %s; already uploaded today (remote modtime: %s)",
						filepath.Base(file), remoteInfo.ModTime().Format("2006-01-02 15:04:05"))
					continue
				}
			} else if !os.IsNotExist(err) {
				// Stat failed for an unexpected reason
				log.Printf("⚠️  Could not stat remote file %s: %v", remotePath, err)
				continue
			}
			// ───────────────────────────────────────────────────────────────────

			wg.Add(1)
			go uploadFile(sftpClient, file, remotePath, &wg, sem)
		}
	}

	wg.Wait()
	log.Printf("🚀 Uploads completed in %dm %ds\n", time.Since(start).Minutes(), time.Since(start).Seconds())
}
