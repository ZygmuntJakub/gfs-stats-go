package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	baseURL     = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod"
	resolution  = "0p25"
	outputDir   = "./gfs_data"
	maxParallel = 4
	maxRetries  = 3
	retryDelay  = 5 * time.Second
)

// tmpOutputDir will be set during runtime
var tmpOutputDir string

var forecastHours = []string{"000", "001", "002", "003", "004", "005", "006", "007", "008", "009", "010", "011", "012", "013", "014", "015", "016", "017", "018", "019", "020", "021", "022", "023", "024"}

// var forecastHours = []string{"000"}

func main() {
	if err := run(); err != nil {
		log.Fatalf("Error: %v", err)
	}
}

func run() error {
	// Create a temporary directory for downloads
	if err := os.MkdirAll(filepath.Dir(outputDir), 0755); err != nil {
		return fmt.Errorf("failed to create parent directory: %w", err)
	}

	var err error
	tmpOutputDir, err = os.MkdirTemp(filepath.Dir(outputDir), "gfs_download_")
	if err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}

	// Clean up temp dir if anything fails
	defer func() {
		if _, err := os.Stat(tmpOutputDir); err == nil {
			os.RemoveAll(tmpOutputDir)
		}
	}()

	date, cycle := getCurrentCycle()
	log.Printf("Downloading GFS forecast: %s %sZ", date, cycle)
	log.Printf("Resolution: 0.25° (~27 km)")
	log.Printf("Temp output: %s", tmpOutputDir)
	log.Println("----------------------------------------")

	client := &http.Client{
		Timeout: 30 * time.Minute,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jobs := make(chan string, len(forecastHours))
	results := make(chan error, len(forecastHours))

	var wg sync.WaitGroup
	for i := 0; i < maxParallel; i++ {
		wg.Add(1)
		go func() {
			worker(ctx, client, date, cycle, jobs, results)
			wg.Done()
		}()
	}

	for _, hour := range forecastHours {
		jobs <- hour
	}
	close(jobs)

	go func() {
		wg.Wait()
		close(results)
	}()

	var failed int
	for err := range results {
		if err != nil {
			log.Printf("Download failed: %v", err)
			failed++
		}
	}

	log.Println("----------------------------------------")
	if failed > 0 {
		return fmt.Errorf("%d download(s) failed", failed)
	}

	// All downloads successful, now move the temp directory to the final location
	if _, err := os.Stat(outputDir); err == nil {
		if err := os.RemoveAll(outputDir); err != nil {
			return fmt.Errorf("failed to remove old output directory: %w", err)
		}
	}

	if err := os.Rename(tmpOutputDir, outputDir); err != nil {
		return fmt.Errorf("failed to move temp directory to final location: %w", err)
	}

	log.Println("All downloads completed and moved to", outputDir)
	return nil
}

func worker(ctx context.Context, client *http.Client, date, cycle string, jobs <-chan string, results chan<- error) {
	for hour := range jobs {
		select {
		case <-ctx.Done():
			results <- ctx.Err()
			return
		default:
			results <- downloadWithRetry(ctx, client, date, cycle, hour)
		}
	}
}

func downloadWithRetry(ctx context.Context, client *http.Client, date, cycle, hour string) error {
	filename := fmt.Sprintf("gfs.t%sz.pgrb2.%s.f%s", cycle, resolution, hour)
	url := fmt.Sprintf("%s/gfs.%s/%s/atmos/%s", baseURL, date, cycle, filename)
	outputPath := filepath.Join(tmpOutputDir, filename)
	tempPath := filepath.Join(tmpOutputDir, filename+".tmp")

	if info, err := os.Stat(outputPath); err == nil {
		if info.Size() > 1<<20 { // 1MB
			log.Printf("✓ %sh exists (%s)", hour, formatSize(info.Size()))
			return nil
		}
	}

	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		log.Printf("[%sh] Downloading (attempt %d/%d)...", hour, attempt, maxRetries)

		err := downloadFile(ctx, client, url, tempPath)
		if err == nil {
			if info, err := os.Stat(tempPath); err == nil && info.Size() > 1<<20 {
				if err := os.Rename(tempPath, outputPath); err != nil {
					os.Remove(tempPath)
					return fmt.Errorf("failed to rename temp file: %w", err)
				}
				log.Printf("✓ %sh complete (%s)", hour, formatSize(info.Size()))
				return nil
			}
			lastErr = fmt.Errorf("file too small")
		} else {
			lastErr = fmt.Errorf("download failed: %w", err)
		}

		if attempt < maxRetries {
			time.Sleep(retryDelay)
		}
	}

	os.Remove(tempPath)
	return fmt.Errorf("%sh failed after %d attempts: %v", hour, maxRetries, lastErr)
}

func downloadFile(ctx context.Context, client *http.Client, url, outputPath string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	out, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("creating output file: %w", err)
	}
	defer out.Close()

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("making request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	if _, err := io.Copy(out, resp.Body); err != nil {
		return fmt.Errorf("downloading file: %w", err)
	}

	return nil
}

func getCurrentCycle() (string, string) {
	now := time.Now().UTC()
	hour := now.Hour()
	switch {
	case hour >= 23:
		return now.Format("20060102"), "18"
	case hour >= 17:
		return now.Format("20060102"), "12"
	case hour >= 11:
		return now.Format("20060102"), "06"
	case hour >= 5:
		return now.Format("20060102"), "00"
	default:
		yesterday := now.Add(-24 * time.Hour)
		return yesterday.Format("20060102"), "18"
	}
}

func formatSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
