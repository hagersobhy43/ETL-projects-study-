# Automated Weather ETL Pipeline

A containerized ETL (Extract, Transform, Load) pipeline designed to automate daily weather data collection and storage using Bash scripting and Docker.

## Project Description
This project implements a Proof of Concept (POC) for a weather data pipeline. It extracts real-time observed temperatures and next-day forecasts for Cairo, Egypt, processes the raw data into a structured format, and appends the records to a historical log file.

## Technical Architecture
*   **Operating System:** Linux (Debian 12 Slim) via Docker.
*   **Language:** Bash Scripting.
*   **Automation:** Cron (system scheduler).
*   **Data Source:** `wttr.in` (External weather service).
*   **Processing Tools:** `curl`, `grep`, `cut`, `head`, `tail`.

## ETL Process Workflow
1.  **Extract:** The script fetches weather data in a text-based format using `curl`.
2.  **Transform:**
    *   Specific lines are isolated using `head` and `tail`.
    *   Regular Expressions (Regex) are used to extract numerical temperature values.
    *   Time-zone-specific dates are generated using the `TZ='Africa/Cairo'` environment variable.
3.  **Load:** The transformed record is structured into a tab-separated format and appended to a persistent log file (`rx_poc.log`).

## Docker Environment & Connectivity
To maintain a clean and isolated development environment, the pipeline is entirely containerized. 

### Network Troubleshooting (DNS)
During development, a connectivity issue occurred where the container could not resolve external URLs. 
*   **Root Cause:** The Docker Engine's default DNS settings were unable to communicate with the host's network.
*   **Solution:** Configured the Docker Engine JSON settings to use Google's Public DNS (`8.8.8.8` and `8.8.4.4`). This enabled the container to fetch weather data successfully.

## How to Run

### 1. Build the Image
```bash
docker build -t weather_image .
```
### 2. Run the Container
```bash
docker run -d --name weather_runner -v "$(pwd)":/app weather_image
```
### 3. Schedule the Cron Job
```bash
docker exec -it weather_runner bash
crontab -e
# Add the following line to run the script daily at noon:
0 12 * * * /bin/bash /app/rx_poc.sh
```
### Row Data Sample
<img width="1802" height="1039" alt="Screenshot 2026-04-16 at 4 52 33 AM" src="https://github.com/user-attachments/assets/c55e2bc9-35b5-4774-a01f-ed32bfd227d7" />

### Log Sample Output
<img width="826" height="181" alt="Screenshot 2026-04-16 at 4 52AM" src="https://github.com/user-attachments/assets/2c6bdc77-cdaf-41d4-b357-0912343bf4d7" />





