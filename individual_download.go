package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// SFTP connection details (common for all advertisers)
const (
	sftpHost = "sftp.admarketplace.net"
	sftpPort = 22
)

// AdvertiserConfig holds the unique SFTP credentials and file patterns for each advertiser.
type AdvertiserConfig struct {
	Username    string
	Password    string
	SFTPPath    string
	LocalPath   string
	FilePattern interface{} // Can be string or func(string) string
	FinalName   string      // The final name for the local file (overwrites)
}

// --- Helper Functions (Unchanged) ---

// isLocalFileModifiedToday checks if a file exists at the local path and if its modification date is today.
// Returns true and the modification time if modified today, false otherwise.
func isLocalFileModifiedToday(localPath string) (bool, time.Time) {
	fileInfo, err := os.Stat(localPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, time.Time{} // File doesn't exist, not an error in this context
		}
		// Other error (e.g., permission denied)
		log.Printf("WARN: Could not stat local file '%s': %v", localPath, err)
		return false, time.Time{}
	}

	modTime := fileInfo.ModTime()
	// Use current system time - be mindful of potential time zone differences
	// if comparing against server time without normalization.
	// For "same day check", using local time is usually sufficient.
	now := time.Now()

	// Compare year, month, and day
	if modTime.Year() == now.Year() && modTime.Month() == now.Month() && modTime.Day() == now.Day() {
		return true, modTime
	}

	return false, modTime // Exists, but not modified today
}

// isSftpFileModifiedToday checks if a file exists at the remote path and if its modification date is today.
// Returns true and the modification time if modified today, false otherwise.
// NOTE: Compares remote file mod time (usually UTC) with local system's current date.
// This might lead to edge cases around midnight if server and client are in different timezones.
// For simplicity, we compare the date part only.
func isSftpFileModifiedToday(client *sftp.Client, remotePath string) (bool, time.Time) {
	fileInfo, err := client.Stat(remotePath)
	if err != nil {
		// Don't treat "not found" as a critical error here, just log info
		log.Printf("INFO: Could not stat remote file '%s': %v", remotePath, err)
		return false, time.Time{} // File likely doesn't exist or is inaccessible
	}

	modTime := fileInfo.ModTime()
	now := time.Now() // Use local time for comparison date

	// Compare year, month, and day based on the client's perspective of "today"
	// Consider using UTC if consistent timezone handling is critical:
	// modTimeUTC := modTime.UTC()
	// nowUTC := time.Now().UTC()
	// if modTimeUTC.Year() == nowUTC.Year() ... etc.
	if modTime.Year() == now.Year() && modTime.Month() == now.Month() && modTime.Day() == now.Day() {
		return true, modTime
	}

	// File exists, but wasn't modified today (from client's perspective)
	log.Printf("INFO: Remote file '%s' exists but was last modified %s (not today based on local clock).", remotePath, modTime.Format(time.RFC3339))
	return false, modTime
}

// --- Main Function ---

func main() {
	startTime := time.Now()
	// Use UTC date for file patterns for consistency, regardless of where the script runs
	currentDate := startTime.UTC().Format("20060102") // Go's specific reference date format YYYYMMDD
	log.Printf("Starting SFTP download process for date %s (UTC)...", currentDate)

	advertisers := map[string]AdvertiserConfig{
		"Bloomingdales": {
			Username:    "l_bloomingdales",
			Password:    "hA1HncToLmGEUu5zA9aYySvJ",
			SFTPPath:    "/",
			LocalPath:   "/Volumes/T9/AMP/KlarnaShoppingAds/Bloomingdales/",
			FilePattern: func(date string) string { return fmt.Sprintf("%s_Bloomingdales_PLA.csv", date) },
			FinalName:   "Bloomingdales_PLA.csv",
		},
		"Verizon": {
			Username:    "l_verizon",
			Password:    "WD/nGa0YKO3LWPyFCYpu09QD",
			SFTPPath:    "/files/files/",
			LocalPath:   "/Volumes/T9/AMP/KlarnaShoppingAds/Klarna/Verizon/",
			FilePattern: "verizon_devices_admarketplace.csv", // Static filename
			FinalName:   "Verizon_PLA.csv",
		},
		"BedBathBeyond": {
			Username:    "l_BedBathBeyond",
			Password:    "nZeorxTgL/ckgP+KBjRw6VqI",
			SFTPPath:    "/files/",
			LocalPath:   "/Volumes/T9/AMP/KlarnaShoppingAds/BedBathBeyond/",
			FilePattern: func(date string) string { return fmt.Sprintf("%s_BedBathAndBeyond_PLA.csv.gz", date) },
			FinalName:   "BedBathBeyond_PLA.csv.gz",
		},
		"HarryDavid": {
			Username:    "l_HarryDavid",
			Password:    "gQQKcIMoExljSPzapMSMw6A8",
			SFTPPath:    "/files/",
			LocalPath:   "/Volumes/T9/AMP/KlarnaShoppingAds/HarryDavid/",
			FilePattern: "hd_admarketplace.csv", // Static filename
			FinalName:   "HarryDavid_PLA.csv",
		},
		"TommyBahama": {
			Username:    "l_Tommybahama",
			Password:    "MEvkJWVKyvHtBg3XfdOfUgwz",
			SFTPPath:    "/files/",
			LocalPath:   "/Volumes/T9/AMP/KlarnaShoppingAds/TommyBahama/",
			FilePattern: func(date string) string { return fmt.Sprintf("%s_TommyBahama_PLA.csv", date) },
			FinalName:   "TommyBahama_PLA.csv",
		},
		"Houzz": {
			Username:    "l_houzz",
			Password:    "71NZp8BsksE8hhiutbf+PPol",
			SFTPPath:    "/files/pla_feed/",
			LocalPath:   "/Volumes/T9/AMP/KlarnaShoppingAds/Houzz/",
			FilePattern: "houzz_full_catalog.txt.gz", // Static filename
			FinalName:   "Houzz_PLA.txt.gz",
		},
		"Zappos": {
			Username:    "l_Zappos-1",
			Password:    "wjNAnigAo4GmNXR0zTAsK7nN",
			SFTPPath:    "/files/",
			LocalPath:   "/Volumes/T9/AMP/KlarnaShoppingAds/Zappos/",
			FilePattern: "zappos_adsmarketplace.txt.gz", // Static filename - NOTE: typo in original? "zapoos"
			FinalName:   "Zappos_PLA.txt.gz",
		},
		"HomeDepot": {
			Username:    "l_homedepot",
			Password:    "CEKCkD6WBsO7e6YaS2duWSVZ",
			SFTPPath:    "/files/",
			LocalPath:   "/Volumes/T9/AMP/KlarnaShoppingAds/TheHomeDepot/",
			FilePattern: func(date string) string { return fmt.Sprintf("%s_TheHomeDepot_029A.csv.gz", date) },
			FinalName:   "HomeDepot_PLA.csv.gz",
		},
		"NewBalance": {
			Username:    "l_Newbalance",
			Password:    "QatylO/peIl3Q/3E1j4EoGPN",
			SFTPPath:    "/files/",
			LocalPath:   "/Volumes/T9/AMP/KlarnaShoppingAds/NewBalance/",
			FilePattern: func(date string) string { return fmt.Sprintf("%s_NewBalance_ PLA.csv", date) }, // Note: potential space before PLA
			FinalName:   "NewBalance_PLA.csv",
		},
		"Wayfair": {
			Username:    "wayfair",
			Password:    "off-launched-perm-shelling",
			SFTPPath:    "/files/",
			LocalPath:   "/Volumes/T9/AMP/KlarnaShoppingAds/Wayfair/",
			FilePattern: "admarketplace_full_catalog_en_us.csv.gz", // Static filename
			FinalName:   "Wayfair_PLA.csv",
		},
		"UnderArmour": {
			Username:    "underarmour",
			Password:    "supports-GALE-mobility-postman",
			SFTPPath:    "/files/",
			LocalPath:   "/Volumes/T9/AMP/KlarnaShoppingAds/UnderArmour/",
			FilePattern: "UA_AMP.csv", // Static filename
			FinalName:   "UnderArmour_PLA.csv",
		},
		"JCPenney": {
			Username:    "jcpenney",
			Password:    "veins-RECORDS-olympus-figure",
			SFTPPath:    "/",
			LocalPath:   "/Volumes/T9/AMP/KlarnaShoppingAds/JCPenny/",
			FilePattern: "JCP_adMarketplace.csv", // Static filename
			FinalName:   "JCPenney_PLA.csv",
		},
		// "IKEA": {
		// 	Username:    "ikea",
		// 	Password:    "LINE-maverick-denise-hobbies",
		// 	SFTPPath:    "/files/pla_feed/",
		// 	LocalPath:   "/Volumes/T9/AMP/KlarnaShoppingAds/LandsEnd/",
		// 	FilePattern: "admarketplace_full_catalog_en_us.csv.gz", // Static filename
		// 	FinalName:   "LandsEnd_PLA.csv",
		// },
	}

	// Process all advertisers
	for advertiser, config := range advertisers {
		// *** Start of Anonymous Function Wrapper ***
		// This ensures that resources (like SFTP/SSH clients) opened within this
		// function are cleaned up (via defer) when this function returns,
		// effectively closing them after each advertiser.
		func(advertiser string, config AdvertiserConfig) {
			fmt.Printf("Processing advertiser: %s\n", advertiser)

			sftpFolder := config.SFTPPath
			localFolder := config.LocalPath
			filePattern := config.FilePattern
			finalFilename := config.FinalName // Static name for overwriting

			// --- Determine SFTP filename ---
			var sftpFilename string
			switch fp := filePattern.(type) {
			case func(string) string:
				sftpFilename = fp(currentDate) // Generate filename using today's UTC date
			case string:
				sftpFilename = fp // Use the static filename
			default:
				log.Printf("[%s] ERROR: Unsupported file pattern type '%T'. Skipping.", advertiser, filePattern)
				return // Exit anonymous function for this advertiser
			}
			// Use forward slashes for SFTP paths and sanitize
			remoteFilePath := filepath.ToSlash(filepath.Clean(filepath.Join(sftpFolder, sftpFilename)))

			// --- Determine Local filepath ---
			localFilePath := filepath.Join(localFolder, finalFilename) // Always save as the fixed final name

			// --- Ensure local directory exists ---
			err := os.MkdirAll(localFolder, 0755) // 0755 permissions: rwxr-xr-x
			if err != nil {
				log.Printf("[%s] ERROR: Failed to create local directory '%s': %v. Skipping.", advertiser, localFolder, err)
				return // Exit anonymous function for this advertiser
			}

			// --- Check if local file exists and was modified today ---
			localExists, localModTime := isLocalFileModifiedToday(localFilePath)
			if localExists {
				fmt.Printf("[%s] INFO: Local file '%s' already exists and was modified today (%s). Skipping download.\n",
					advertiser, finalFilename, localModTime.Format(time.RFC3339))
				return // Exit anonymous function for this advertiser
			}

			// --- Establish SFTP Connection ---
			sshConfig := &ssh.ClientConfig{
				User: config.Username,
				Auth: []ssh.AuthMethod{
					ssh.Password(config.Password),
				},
				// WARNING: Insecure! Use only for testing. Replace with proper host key verification in production.
				// For production consider using ssh.KnownHosts helper or ssh.FixedHostKey.
				HostKeyCallback: ssh.InsecureIgnoreHostKey(),
				Timeout:         30 * time.Second, // Increased timeout slightly
			}
			sshAddr := fmt.Sprintf("%s:%d", sftpHost, sftpPort)

			fmt.Printf("[%s] INFO: Connecting to %s...\n", advertiser, sshAddr)
			sshClient, err := ssh.Dial("tcp", sshAddr, sshConfig)
			if err != nil {
				log.Printf("[%s] ERROR: Failed to connect to SSH server '%s': %v. Skipping.", advertiser, sshAddr, err)
				return // Exit anonymous function for this advertiser
			}
			// Defer closing the SSH client *within this anonymous function's scope*
			defer func() {
				fmt.Printf("[%s] INFO: Closing SSH connection to %s.\n", advertiser, sshAddr)
				sshClient.Close()
			}()

			sftpClient, err := sftp.NewClient(sshClient)
			if err != nil {
				log.Printf("[%s] ERROR: Failed to create SFTP client: %v. Skipping.", advertiser, err)
				// sshClient.Close() will be called by its defer just before returning
				return // Exit anonymous function for this advertiser
			}
			// Defer closing the SFTP client *within this anonymous function's scope*
			defer func() {
				fmt.Printf("[%s] INFO: Closing SFTP client.\n", advertiser)
				sftpClient.Close()
			}()
			fmt.Printf("[%s] INFO: SFTP connection established.\n", advertiser)

			// --- Check if SFTP file exists and was modified today ---
			sftpExists, sftpModTime := isSftpFileModifiedToday(sftpClient, remoteFilePath)
			if !sftpExists {
				// isSftpFileModifiedToday already logs details
				fmt.Printf("[%s] INFO: Remote file '%s' condition not met (not found or not modified today). Skipping download.\n", advertiser, remoteFilePath)
				// sftpClient and sshClient Close() will be called by their defers
				return // Exit anonymous function for this advertiser
			}
			fmt.Printf("[%s] INFO: Remote file '%s' found, modified at %s.\n", advertiser, remoteFilePath, sftpModTime.Format(time.RFC3339))

			// --- Download the file ---
			fmt.Printf("[%s] INFO: Starting download from '%s' to '%s'...\n", advertiser, remoteFilePath, localFilePath)

			// Open remote file
			remoteFile, err := sftpClient.Open(remoteFilePath)
			if err != nil {
				log.Printf("[%s] ERROR: Failed to open remote file '%s': %v. Skipping.", advertiser, remoteFilePath, err)
				// sftpClient and sshClient Close() will be called by their defers
				return // Exit anonymous function for this advertiser
			}
			// Defer closing the remote file *within this anonymous function's scope*
			defer remoteFile.Close()

			// Create local file (or truncate if somehow exists)
			localFile, err := os.Create(localFilePath)
			if err != nil {
				log.Printf("[%s] ERROR: Failed to create local file '%s': %v. Skipping.", advertiser, localFilePath, err)
				// remoteFile, sftpClient, sshClient Close() will be called by their defers
				return // Exit anonymous function for this advertiser
			}
			// Defer closing the local file *within this anonymous function's scope*
			defer localFile.Close()

			// Copy data
			bytesCopied, err := io.Copy(localFile, remoteFile)
			if err != nil {
				log.Printf("[%s] ERROR: Failed to copy file from SFTP '%s' to local '%s': %v", advertiser, remoteFilePath, localFilePath, err)
				// Attempt to remove potentially incomplete local file
				// We need to close the local file *before* removing it on some OS (like Windows)
				localFile.Close() // Explicitly close before removing
				if removeErr := os.Remove(localFilePath); removeErr != nil {
					log.Printf("[%s] WARNING: Failed to remove incomplete local file '%s' after copy error: %v", advertiser, localFilePath, removeErr)
				}
				// Other defers (remoteFile, sftp, ssh) will still run before returning
				return // Exit anonymous function for this advertiser
			}

			// Explicitly close local file here to ensure data is flushed before success message (though defer would likely work)
			if err := localFile.Close(); err != nil {
				log.Printf("[%s] WARNING: Error closing local file '%s' after successful copy: %v", advertiser, localFilePath, err)
			}

			fmt.Printf("[%s] SUCCESS: Downloaded %d bytes to '%s'.\n", advertiser, bytesCopied, localFilePath)
			fmt.Printf("[%s] Finished processing.\n\n", advertiser)

			// When this anonymous function returns (implicitly here), the deferred calls
			// for sftpClient.Close() and sshClient.Close() will execute for this advertiser.

		}(advertiser, config) // *** Immediately call the anonymous function ***

	} // *** End of advertiser loop ***

	// --- Calculate and Print Total Execution Time ---
	duration := time.Since(startTime)
	log.Printf("SFTP download process finished. Total execution time: %s\n", duration)
}
