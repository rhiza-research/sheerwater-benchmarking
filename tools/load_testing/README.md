# Load Testing Tools

This directory contains JMeter test plans for load testing various components of the system.

## Installing JMeter

### On macOS

1. Install Java if you haven't already:
   ```bash
   brew install openjdk
   ```

2. Install JMeter using Homebrew:
   ```bash
   brew install jmeter
   ```

   This will automatically add JMeter to your PATH.


## Running the Terracotta Load Test

The `terracotta.jmx` test plan is designed to load test the Terracotta tile server by simulating multiple concurrent users requesting tiles.

### Test Plan Overview
- Simulates 30 users requesting tiles from the Terracotta server
- Tests different zoom levels and tile coordinates
- Measures response times and throughput

### Running the Test

1. GUI Mode (for test development and debugging):
   ```bash
   # On macOS (if installed via Homebrew)
   jmeter -t terracotta.jmx
   ```

2. CLI Mode (for actual load testing):
   ```bash
   # On macOS (if installed via Homebrew)
   jmeter -n -t terracotta.jmx -l results.jtl -e -o report
   ```

   Where:
   - `-n`: Run in non-GUI mode
   - `-t terracotta.jmx`: Test plan file
   - `-l results.jtl`: Log file to save results
   - `-e`: Generate report dashboard
   - `-o report`: Output directory for the report

### Configuring the Test

Before running the test, you may want to modify these parameters in the test plan:

1. Open `terracotta.jmx` in JMeter GUI
2. Modify the following steps in the test plan:
    - Enable/disable the `Bandwidth Limit Timer` steps to control bandwidth usage
    - Enable/disable the `Random Variable` step to control the variables used in the test
    - Modify the `Ramp-up period` and `Test duration` to change the test duration
    - Modify the `Number of threads` to change the number of concurrent users


### Viewing Results

After running the test in CLI mode:
1. Open the generated report:
   ```bash
   # On macOS
   open report/index.html
   ```

2. Key metrics to look for:
   - Response times (average, median, 90th percentile)
   - Throughput (requests per second)
   - Error rate
   - Response time distribution

### Best Practices

1. Start with a small number of threads (users) and gradually increase
2. Monitor server resources during the test
3. Run tests multiple times to ensure consistent results
4. Clear server caches between test runs if needed
5. Use CLI mode for actual load testing to minimize resource overhead
