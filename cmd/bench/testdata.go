package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	// HuggingFace Dataset Viewer API endpoint
	datasetAPI    = "https://datasets-server.huggingface.co/rows?dataset=davidfant/wikipedia-simple&config=default&split=train&offset=%d&length=%d"
	cacheFile     = "testdata/wikipedia.json"
	progressFile  = "testdata/progress.csv"
	defaultTarget = 10000 
)

// Doc represents a Wikipedia document
type Doc struct {
	ID     string
	Fields map[string]any
}

// hfResponse is the HuggingFace API response structure
type hfResponse struct {
	Rows []struct {
		Row struct {
			ID       int    `json:"id"`
			URL      string `json:"url"`
			Title    string `json:"title"`
			Text     string `json:"text"`
			Markdown string `json:"markdown"`
		} `json:"row"`
	} `json:"rows"`
}

// DownloadProgress tracks download state
type DownloadProgress struct {
	Downloaded int       // Number of docs successfully downloaded
	Target     int       // Target number of docs
	LastUpdate time.Time // When progress was last updated
}

// LoadWikipediaDocs loads Wikipedia documents, using cache if available
func LoadWikipediaDocs(benchmarkDir string, count int) ([]Doc, error) {
	cachePath := filepath.Join(benchmarkDir, cacheFile)

	// Try to load from cache
	docs, err := loadFromCache(cachePath)
	if err == nil && len(docs) >= count {
		fmt.Printf("Loaded %d articles from cache (%d available)\n", count, len(docs))
		return docs[:count], nil
	}

	if len(docs) > 0 {
		fmt.Printf("Cache has %d articles, need %d\n", len(docs), count)
	}

	return docs, nil
}

// DownloadMoreDocs downloads additional Wikipedia documents
// Call this to expand the dataset. It will resume from where it left off.
func DownloadMoreDocs(benchmarkDir string, targetCount int) error {
	cachePath := filepath.Join(benchmarkDir, cacheFile)
	progressPath := filepath.Join(benchmarkDir, progressFile)

	// Load existing docs
	existingDocs, _ := loadFromCache(cachePath)
	currentCount := len(existingDocs)

	if currentCount >= targetCount {
		fmt.Printf("Already have %d docs (target: %d)\n", currentCount, targetCount)
		return nil
	}

	// Load progress to get the API offset
	progress := loadProgress(progressPath)
	apiOffset := progress.Downloaded
	if apiOffset < currentCount {
		apiOffset = currentCount // Use larger of progress or cache
	}

	fmt.Printf("Download Status:\n")
	fmt.Printf("  Cached docs:  %d\n", currentCount)
	fmt.Printf("  API offset:   %d\n", apiOffset)
	fmt.Printf("  Target:       %d\n", targetCount)
	fmt.Printf("  To download:  %d\n", targetCount-currentCount)
	fmt.Println()

	// Download remaining
	newDocs, finalOffset, err := downloadWithProgress(apiOffset, targetCount-currentCount, cachePath, existingDocs)

	// Always save progress, even on error
	saveProgress(progressPath, DownloadProgress{
		Downloaded: finalOffset,
		Target:     targetCount,
		LastUpdate: time.Now(),
	})

	if err != nil {
		// Save partial results
		if len(newDocs) > 0 {
			allDocs := append(existingDocs, newDocs...)
			saveToCache(cachePath, allDocs)
			fmt.Printf("\nSaved %d docs before stopping (total: %d)\n", len(newDocs), len(allDocs))
		}
		return fmt.Errorf("download stopped: %w\nRun again to resume from offset %d", err, finalOffset)
	}

	// Save all docs
	allDocs := append(existingDocs, newDocs...)
	if err := saveToCache(cachePath, allDocs); err != nil {
		return fmt.Errorf("failed to save cache: %w", err)
	}

	fmt.Printf("\nDownload complete! Total docs: %d\n", len(allDocs))
	return nil
}

func downloadWithProgress(startOffset, count int, cachePath string, existingDocs []Doc) ([]Doc, int, error) {
	var docs []Doc
	batchSize := 100
	maxRetries := 5
	currentOffset := startOffset

	for len(docs) < count {
		remaining := count - len(docs)
		fetchCount := batchSize
		if remaining < batchSize {
			fetchCount = remaining
		}

		var batch []Doc
		var err error

		// Retry with exponential backoff
		for retry := 0; retry < maxRetries; retry++ {
			batch, err = fetchBatch(currentOffset, fetchCount)
			if err == nil {
				break
			}

			// Check if rate limited (429)
			if strings.Contains(err.Error(), "429") {
				waitTime := time.Duration(2<<retry) * time.Second
				if retry < maxRetries-1 {
					fmt.Printf("  Rate limited, waiting %v (retry %d/%d)...\n", waitTime, retry+1, maxRetries)
					time.Sleep(waitTime)
					continue
				}
			} else {
				// Non-429 error, don't retry
				return docs, currentOffset, err
			}
		}

		if err != nil {
			// Rate limit exhausted, graceful shutdown
			fmt.Printf("\n  Rate limit exceeded after %d retries. Saving progress...\n", maxRetries)
			return docs, currentOffset, fmt.Errorf("rate limited at offset %d", currentOffset)
		}

		if len(batch) == 0 {
			fmt.Println("  No more data available from API")
			break
		}

		docs = append(docs, batch...)
		currentOffset += len(batch)
		fmt.Printf("  Downloaded %d/%d (offset: %d)\n", len(docs), count, currentOffset)

		// Save checkpoint every 500 docs
		if len(docs)%500 == 0 {
			allDocs := append(existingDocs, docs...)
			if err := saveToCache(cachePath, allDocs); err == nil {
				fmt.Printf("  [checkpoint saved: %d docs]\n", len(allDocs))
			}
		}

		// Delay between requests to avoid rate limiting
		time.Sleep(300 * time.Millisecond)
	}

	return docs, currentOffset, nil
}

func loadFromCache(path string) ([]Doc, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var docs []Doc
	if err := json.Unmarshal(data, &docs); err != nil {
		return nil, err
	}

	return docs, nil
}

func saveToCache(path string, docs []Doc) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	data, err := json.Marshal(docs)
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0644)
}

func loadProgress(path string) DownloadProgress {
	f, err := os.Open(path)
	if err != nil {
		return DownloadProgress{}
	}
	defer f.Close()

	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	if err != nil || len(records) < 2 {
		return DownloadProgress{}
	}

	// CSV format: downloaded,target,last_update
	row := records[1] // Skip header
	downloaded, _ := strconv.Atoi(row[0])
	target, _ := strconv.Atoi(row[1])
	lastUpdate, _ := time.Parse(time.RFC3339, row[2])

	return DownloadProgress{
		Downloaded: downloaded,
		Target:     target,
		LastUpdate: lastUpdate,
	}
}

func saveProgress(path string, progress DownloadProgress) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	writer := csv.NewWriter(f)
	writer.Write([]string{"downloaded", "target", "last_update"})
	writer.Write([]string{
		strconv.Itoa(progress.Downloaded),
		strconv.Itoa(progress.Target),
		progress.LastUpdate.Format(time.RFC3339),
	})
	writer.Flush()
	return writer.Error()
}

func fetchBatch(offset, length int) ([]Doc, error) {
	url := fmt.Sprintf(datasetAPI, offset, length)

	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API returned %d: %s", resp.StatusCode, string(body))
	}

	var hfResp hfResponse
	if err := json.NewDecoder(resp.Body).Decode(&hfResp); err != nil {
		return nil, err
	}

	docs := make([]Doc, len(hfResp.Rows))
	for i, row := range hfResp.Rows {
		fields := make(map[string]any)

		fields["title"] = row.Row.Title
		fields["body"] = row.Row.Text

		if summary := extractSummary(row.Row.Text); summary != "" {
			fields["summary"] = summary
		}

		if row.Row.URL != "" {
			fields["url"] = row.Row.URL
		}

		docs[i] = Doc{
			ID:     fmt.Sprintf("wiki_%d", row.Row.ID),
			Fields: fields,
		}
	}

	return docs, nil
}

func extractSummary(text string) string {
	if idx := strings.Index(text, "\n\n"); idx > 0 && idx < 1000 {
		return strings.TrimSpace(text[:idx])
	}
	if len(text) > 500 {
		text = text[:500]
		if idx := strings.LastIndex(text, " "); idx > 0 {
			text = text[:idx]
		}
		return strings.TrimSpace(text) + "..."
	}
	return ""
}
