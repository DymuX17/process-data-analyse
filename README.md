# Engineering Project for Process Data Acquisition, Analysis, and Visualization – Process Data Analysis Module

This project is the second part of a larger system for industrial process data acquisition, analysis, and visualization.  
It focuses on analyzing control error data (uchyb regulacji) retrieved from a time-series database and computing key performance metrics.

## Project Overview  
### 2. Process Data Analysis  
The `process-data-analyse` module connects to an InfluxDB database to retrieve control error data from simulated industrial processes. It performs integral and statistical analysis on the error signal, calculating both integral performance indices and statistical characteristics. The results of the analysis are then written back to the InfluxDB database for further use in visualization.

## Features  
- **InfluxDB Integration**: Connects to an InfluxDB time-series database to retrieve and store data.  
- **Error Signal Analysis**: Computes integral performance indices commonly used in control systems:  
  - **IAE** (Integral of Absolute Error)  
  - **ISE** (Integral of Squared Error)  
- **Statistical Analysis**: Calculates key statistical indicators of the error signal:  
  - **Average (avg)**  
  - **Standard Deviation (std)**  
  - **Minimum (min)**  
  - **Maximum (max)**  
- **Data Persistence**: Stores the computed metrics back into the InfluxDB database for long-term availability and integration with other system components.


## Technologies Used  
- **Python 3.9**  
- **InfluxDB Python Client**  
- **NumPy / Pandas** – numerical processing  
- **SciPy** – calculation of integral performance indices (IAE, ISE)  
- **python-dotenv** – environment variable management (.env support)

## Usage  
To run the analysis module:  
1. Ensure the InfluxDB service is running and accessible (best to run as Docker Container).   
2. After setting up `data-generation-for-visualisation` run the main analysis script.  
3. Check the InfluxDB database for newly written performance indicators.
