detailed-variable-info:
	wgrib2 $(FILE) -v

location-forecast:
	@wgrib2 $(FILE) -match ":TMP:2 m above ground:" -lon 115.744 -32.304 | awk -F'val=' '{print $$2}'
	@wgrib2 $(FILE) -match ":UGRD:10 m above ground:" -lon 115.744 -32.304 | awk -F'val=' '{print $$2}'
	@wgrib2 $(FILE) -match ":VGRD:10 m above ground:" -lon 115.744 -32.304 | awk -F'val=' '{print $$2}'
	@wgrib2 $(FILE) -match ":GUST:surface:" -lon 115.744 -32.304 | awk -F'val=' '{print $$2}'

localtion-forecast-parallel:
	@wgrib2 $(FILE) -match ":(TMP:2 m above ground|UGRD:10 m above ground|VGRD:10 m above ground|GUST:surface):" -lon 115.744 -32.304 | awk -F'val=' '{print $$2}'
run-ingest:
	go run cmd/ingest/main.go

download-data:
	@echo "Building download tool..."
	@go build -o bin/download ./cmd/download
	@echo "Running download..."
	@./bin/download
