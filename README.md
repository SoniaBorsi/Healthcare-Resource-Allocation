# Healthcare Resource Allocation

The primary goal of the application is to provide a comprehensive, data-driven view of the Australian healthcare ecosystem, revealing trends and variations in resource utilisation across hospitals and regions.

<br>

<p align="center">
  <img src="https://github.com/SoniaBorsi/Healthcare-Resource-Allocation/blob/7da3dd9f425534fce06b3f21a67059a9697cf7b8/logo.png?raw=true" width="400"/>  
</p>

Designed for government decision-makers, this app enables informed policy-making by providing a clear, real-time snapshot of healthcare performance.

## Contents:
- [Project Structure](https://github.com/SoniaBorsi/Healthcare-Resource-Allocation/tree/main?tab=readme-ov-file#project-structure)
- [Getting Started](https://github.com/SoniaBorsi/Healthcare-Resource-Allocation/tree/main?tab=readme-ov-file#getting-started)
- [Dashboard Demo](https://github.com/SoniaBorsi/Healthcare-Resource-Allocation/tree/main?tab=readme-ov-file#dashboard-demo)
- [Data Sources](https://github.com/SoniaBorsi/Healthcare-Resource-Allocation/tree/main?tab=readme-ov-file#data-sources)
- [Authors](https://github.com/SoniaBorsi/Healthcare-Resource-Allocation/tree/main?tab=readme-ov-file#authors)

## Project Structure

The repository is organized as follows:

- `**docker/**`: Dockerfiles and related configuration for each service (DB, Message Queue, Spark Master/Worker, Dashboard).
 `**config/**`: Configuration files for logging, analytics, and other settings.
- `**scripts/**`: Python scripts for setting up the environment, running the ETL pipeline, and utility functions.
- `**src/**`: Source code for the dashboard application and analytics modules.
- `**data/**`: Directory for storing raw, processed, and supplementary data files.
- `**logs/**`: Log files generated by the ETL processes and dashboard.

## Getting Started

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/SoniaBorsi/Healthcare-Resource-Allocation.git
   cd Healthcare-Resource-Allocation
2. **Run the application**:
   ```bash
   bash run.sh
3. **Wait for the the data to be loaded into the db**
   This usually takes about 1 hour, depending on your internet connection
4. **Access the dashboard**
   Got to localhost:8080 and explore all the analytics

## Dashboard Demo
This application is designed to facilitate healthcare resource allocation through data-driven insights. Navigate through the various sections using the sidebar to explore different metrics and tools available to you:
- `*Measures*`: for detailed metrics for various healthcare measures across different states in Australia
- `*Hospitals*`: for a comprehensive analysis of hospitals across different states
- `*Budget*`: for a a comprehensive visual analysis of healthcare expenditure data

<table border="0">
<tr>
    <td>
    <img src="https://github.com/SoniaBorsi/Healthcare-Resource-Allocation/blob/dbae5fe6ec43d075a8c1b61d76fe9a312faec0ae/Dashboard%20demo.gif" width="100%" />
    </td>
    <td>
    <img src="https://github.com/SoniaBorsi/Healthcare-Resource-Allocation/blob/b9d838cc5ddf62201667c8bbc80f4005dc64ebe5/Dashboard%20measures.gif", width="100%" />
    </td>
</tr>
</table>

<table border="0">
<tr>
    <td>
    <img src="https://github.com/SoniaBorsi/Healthcare-Resource-Allocation/blob/b9d838cc5ddf62201667c8bbc80f4005dc64ebe5/Dashboard%20hospitals.gif" width="100%" />
    </td>
    <td>
    <img src="https://github.com/SoniaBorsi/Healthcare-Resource-Allocation/blob/b9d838cc5ddf62201667c8bbc80f4005dc64ebe5/Dashboard%20budget.gif", width="100%" />
    </td>
</tr>
</table>


## Data Sources

Healthcare data utilized in this project is primarily extracted from the [Australian Institute of Health and Welfare (AIHW) API](https://www.aihw.gov.au/reports-data/myhospitals/content/api), an open and freely accessible resource. The available data includes:

- **Hospital Data**
- **Multi-Level Data**
- **Geographic Data**

In addition, this project integrates healthcare data with supplementary datasets provided by the [Australian Bureau of Statistics (ABS)](https://www.abs.gov.au) to enrich the analysis and ensure comprehensive insights.

<br>

<p align="center">
  <img src="https://github.com/SoniaBorsi/Healthcare-Resource-Allocation/blob/5bb61624bfaddd9e336dd68eefd9d855e7db5a79/AIHW_logo.png?raw=true" width="400"/>  
</p>

### Authors

- [Borsi Sonia](https://github.com/SoniaBorsi/)
- [Filippo Costamagna](https://github.com/pippotek)
