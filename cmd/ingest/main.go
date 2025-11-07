package main

import (
	"fmt"
	"log"
	"math"
	"net/http"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/labstack/echo"
)

type ForecastData struct {
	Timestamp time.Time `json:"timestamp"`
	Temp2m    float64   `json:"temp_2m"`
	UWind10m  float64   `json:"u_wind_10m"`
	VWind10m  float64   `json:"v_wind_10m"`
	WindGust  float64   `json:"wind_gust"`
	WindSpeed float64   `json:"wind_speed"`
	WindDir   float64   `json:"wind_direction"`
}

type ForecastOutput struct {
	Timestamp string  `json:"time"`
	TempC     float64 `json:"temp_c"`
	WindKt    float64 `json:"wind_kt"`
	GustKt    float64 `json:"gust_kt"`
	Direction string  `json:"direction"`
}

func main() {
	e := echo.New()

	e.GET("/forecast", func(c echo.Context) error {
		lon := c.QueryParam("lon")
		lat := c.QueryParam("lat")

		lonFloat, err := strconv.ParseFloat(lon, 64)
		if err != nil {
			return c.JSON(http.StatusBadRequest, err)
		}
		latFloat, err := strconv.ParseFloat(lat, 64)
		if err != nil {
			return c.JSON(http.StatusBadRequest, err)
		}

		forecast, err := Ingest(lonFloat, latFloat)
		if err != nil {
			return c.JSON(http.StatusInternalServerError, err)
		}

		return c.JSON(http.StatusOK, forecast)
	})

	e.Logger.Fatal(e.Start(":8080"))
}

func Ingest(lon, lat float64) ([]ForecastOutput, error) {
	// Check if wgrib2 is installed
	if _, err := exec.LookPath("wgrib2"); err != nil {
		log.Fatal("wgrib2 must be installed")
	}

	// Get list of GFS files
	files, err := filepath.Glob("./gfs_data/gfs.t*z.pgrb2.0p25.f*")
	if err != nil {
		log.Fatalf("Error finding GFS files: %v", err)
	}

	// Sort files by forecast hour
	sort.Slice(files, func(i, j int) bool {
		// Extract forecast hour from filename
		hi := getForecastHour(files[i])
		hj := getForecastHour(files[j])
		return hi < hj
	})

	// Process files in parallel
	var wg sync.WaitGroup
	var mu sync.Mutex
	var forecasts []ForecastData

	// Limit concurrency to avoid overwhelming the system
	maxWorkers := 4
	sem := make(chan struct{}, maxWorkers)

	for _, file := range files {
		wg.Add(1)
		sem <- struct{}{} // Acquire semaphore

		go func(f string) {
			defer wg.Done()
			defer func() { <-sem }() // Release semaphore when done

			data, err := processGFSFile(f, lon, lat)
			if err != nil {
				log.Printf("Error processing %s: %v", f, err)
				return
			}

			mu.Lock()
			forecasts = append(forecasts, data)
			mu.Unlock()
		}(file)
	}

	// Wait for all workers to finish
	wg.Wait()

	// Sort forecasts by timestamp
	sort.Slice(forecasts, func(i, j int) bool {
		return forecasts[i].Timestamp.Before(forecasts[j].Timestamp)
	})

	// Create a map to store the latest forecast for each timestamp
	timestampMap := make(map[string]ForecastOutput)

	// Process all forecasts, keeping only the latest entry for each timestamp
	for _, f := range forecasts {
		tempC := f.Temp2m - 273.15 // Convert K to °C
		// Calculate wind speed (magnitude)
		windSpeed := math.Sqrt(f.UWind10m*f.UWind10m + f.VWind10m*f.VWind10m)

		// Calculate wind direction (meteorological convention - where wind is coming from)
		// 0° is from North, 90° from East, 180° from South, 270° from West
		// U is eastward wind, V is northward wind
		windDirRad := math.Atan2(-f.UWind10m, -f.VWind10m) // Negate both components to get direction wind is coming from
		windDir := (windDirRad * 180 / math.Pi)            // Convert to degrees
		if windDir < 0 {
			windDir += 360 // Ensure positive angle
		}

		// Convert m/s to knots (1 m/s = 1.94384 knots)
		windSpeedKnots := windSpeed * 1.94384
		windGustKnots := f.WindGust * 1.94384

		cardinalDir := degreesToCardinal(windDir)

		timestampStr := f.Timestamp.Format("2006-01-02 15:04")

		// Always take the latest data for each timestamp
		timestampMap[timestampStr] = ForecastOutput{
			Timestamp: timestampStr,
			TempC:     tempC,
			WindKt:    windSpeedKnots,
			GustKt:    windGustKnots,
			Direction: cardinalDir,
		}
	}

	// Convert the map back to a slice and sort by timestamp
	var output []ForecastOutput
	for _, data := range timestampMap {
		output = append(output, data)
	}

	// Sort the output by timestamp
	sort.Slice(output, func(i, j int) bool {
		t1, _ := time.Parse("2006-01-02 15:04", output[i].Timestamp)
		t2, _ := time.Parse("2006-01-02 15:04", output[j].Timestamp)
		return t1.Before(t2)
	})

	fmt.Println("Coordinates:", lon, lat)
	fmt.Println("Timestamp\tTempC\tWindKt\tGustKt\tDirection")
	for _, data := range output {
		fmt.Printf("%s\t%.1f\t\t%.1f\t\t%.1f\t\t%s\n",
			data.Timestamp,
			data.TempC,
			data.WindKt,
			data.GustKt,
			data.Direction,
		)
	}

	return output, nil
}

// degreesToCardinal converts wind direction in degrees to cardinal direction
func degreesToCardinal(degrees float64) string {
	// Normalize degrees to be within 0-360
	for degrees < 0 {
		degrees += 360
	}
	for degrees >= 360 {
		degrees -= 360
	}

	directions := []string{"N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
		"S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"}
	index := int((degrees+11.25)/22.5) % 16
	return directions[index]
}

func getForecastHour(filename string) int {
	// Extract forecast hour from filename (e.g., "gfs.t00z.pgrb2.0p25.f003" -> 3)
	base := filepath.Base(filename)
	parts := strings.Split(base, ".")
	if len(parts) < 5 {
		return -1
	}
	hourStr := strings.TrimPrefix(parts[4], "f")
	hour, _ := strconv.Atoi(hourStr)
	return hour
}

func processGFSFile(file string, lon, lat float64) (ForecastData, error) {
	var data ForecastData
	var err error

	// Extract timestamp from filename
	base := filepath.Base(file)
	parts := strings.Split(base, ".")
	if len(parts) < 5 {
		return data, fmt.Errorf("invalid filename format: %s", file)
	}

	// In the processGFSFile function, replace the timestamp parsing part with:

	// Parse timestamp (format: gfs.t00z.pgrb2.0p25.f000)
	cycleHour := strings.TrimSuffix(strings.TrimPrefix(parts[1], "t"), "z")
	cycleTime, err := time.Parse("200601021504", time.Now().Format("20060102")+cycleHour+"00")
	if err != nil {
		return data, fmt.Errorf("error parsing cycle time: %v", err)
	}

	// Add forecast hour
	forecastHour := getForecastHour(file)
	if forecastHour < 0 {
		return data, fmt.Errorf("invalid forecast hour in filename: %s", file)
	}
	data.Timestamp = cycleTime.Add(time.Duration(forecastHour) * time.Hour)

	// Run wgrib2 and pipe to awk to extract values
	wgrib2Cmd := exec.Command("wgrib2", file, "-match", ":(TMP:2 m above ground|UGRD:10 m above ground|VGRD:10 m above ground|GUST:surface):", "-lon", fmt.Sprintf("%.3f", lon), fmt.Sprintf("%.3f", lat))
	awkCmd := exec.Command("awk", "-Fval=", "{print $2}")

	// Create a pipe to connect wgrib2 output to awk input
	pipe, err := wgrib2Cmd.StdoutPipe()
	if err != nil {
		return data, fmt.Errorf("error creating pipe: %v", err)
	}

	// Set awk's input to come from the pipe
	awkCmd.Stdin = pipe

	// Start wgrib2 command
	if err := wgrib2Cmd.Start(); err != nil {
		return data, fmt.Errorf("error starting wgrib2: %v", err)
	}

	// Run awk and capture output
	output, err := awkCmd.Output()
	if err != nil {
		return data, fmt.Errorf("error running awk: %v", err)
	}

	// Wait for wgrib2 to finish
	if err := wgrib2Cmd.Wait(); err != nil {
		return data, fmt.Errorf("wgrib2 error: %v", err)
	}

	// Parse output lines and extract values
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	if len(lines) < 4 { // We expect at least 4 values (TMP, UGRD, VGRD, GUST)
		return data, fmt.Errorf("insufficient data in file: %s", file)
	}

	data.WindGust, err = strconv.ParseFloat(lines[0], 64)
	if err != nil {
		return data, fmt.Errorf("error parsing wind gust: %v", err)
	}

	data.Temp2m, err = strconv.ParseFloat(lines[1], 64)
	if err != nil {
		return data, fmt.Errorf("error parsing temperature: %v", err)
	}

	data.UWind10m, err = strconv.ParseFloat(lines[2], 64)
	if err != nil {
		return data, fmt.Errorf("error parsing U wind: %v", err)
	}

	data.VWind10m, err = strconv.ParseFloat(lines[3], 64)
	if err != nil {
		return data, fmt.Errorf("error parsing V wind: %v", err)
	}

	return data, nil
}
