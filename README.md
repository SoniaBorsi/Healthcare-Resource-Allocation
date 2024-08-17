# Healthcare Resource Allocation
## Big Data Technologies Project

The primary goal of the application is to provide a comprehensive, data-driven view of the Australian healthcare ecosystem, revealing trends and variations in resource utilisation across hospitals and regions.

<br>

<p align="center">
  <img src="https://github.com/SoniaBorsi/Healthcare-Resource-Allocation/blob/7da3dd9f425534fce06b3f21a67059a9697cf7b8/logo.png?raw=true" width="400"/>  
</p>

Designed for government decision-makers, this app enables informed policy-making by providing a clear, real-time snapshot of healthcare performance.

## Contents:
- [Project Structure](https://github.com/SoniaBorsi/Healthcare-Resource-Allocation/tree/main?tab=readme-ov-file#project-structure)
- [Getting Started](https://github.com/SoniaBorsi/Healthcare-Resource-Allocation/tree/main?tab=readme-ov-file#getting-started)
- [Workflow](https://github.com/SoniaBorsi/Healthcare-Resource-Allocation/tree/main?tab=readme-ov-file#workflow)
- [Dashboard Demo](https://github.com/SoniaBorsi/Healthcare-Resource-Allocation/tree/main?tab=readme-ov-file#dashboard-demo)
- [Data Sources](https://github.com/SoniaBorsi/Healthcare-Resource-Allocation/tree/main?tab=readme-ov-file#data-sources)
- [Authors](https://github.com/SoniaBorsi/Healthcare-Resource-Allocation/tree/main?tab=readme-ov-file#authors)


## Project Structure

The repository is organized as follows:
```
.
├── LICENSE
├── README.md
├── data
│   ├── Admitted Patients.xlsx
│   ├── Expediture.xlsx
│   ├── Hospital-resources-tables-2022-23.xlsx
│   ├── images
│   │   └── logo.png
│   └── myhosp-mapping-details-extract.xlsx
├── docker-compose.yml
├── dockerfiles
│   ├── Dockerfile
│   ├── Dockerfile2
│   └── Dockerfile3
├── media
│   ├── ABS_logo.jpeg
│   ├── AIHW_logo.png
│   ├── Dashboard budget.gif
│   ├── Dashboard demo.gif
│   ├── Dashboard hospitals.gif
│   ├── Dashboard measures.gif
│   └── logo.png
├── run.sh
└── src
    ├── app
    │   ├── dashboard.py
    │   └── requirements_dashboard.txt
    └── processing
        ├── ETL.py
        ├── jars
        │   └── postgresql-42.7.3.jar
        ├── requirements_spark.txt
        ├── setup.py
        └── utilities
            ├── budget_lm.py
            ├── tables.py
            ├── tools.py
            ├── values.py
            └── values_lm.py
```

- **`data/`**: Directory for storing images supplementary data files (ABS data).
- **`dockerfiles/`**: Dockerfiles and related configuration for each service of the app.
- **`src/`**: Source code for the dashboard application and analytics modules.
- **`media/`**: Files for the repository.

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


## Workflow
The development of the Australian Efficient Resource Allocation App has been supported by a carefully selected suite of big data technologies, each chosen for its ability to address specific aspects of the system's architecture.

<p align="center">
  <img src="https://github.com/SoniaBorsi/Healthcare-Resource-Allocation/blob/26f81704c13bc23eaac4f085db01d614d50dc531/media/App%20Workflow.png?raw=true" width="512"/>  
</p>

These technologies work seamlessly together to create a robust, scalable application capable of efficiently processing large volumes of health data. 

## Dashboard Demo
This application is designed to facilitate healthcare resource allocation through data-driven insights. Navigate through the various sections using the sidebar to explore different metrics and tools available to you:

<p align="center">
  <img src="https://github.com/SoniaBorsi/Healthcare-Resource-Allocation/blob/dbae5fe6ec43d075a8c1b61d76fe9a312faec0ae/Dashboard%20demo.gif?raw=true" width="512"/>  
</p>

- **Measures**: for detailed metrics for various healthcare measures across different states in Australia
<p align="center">
  <img src="https://github.com/SoniaBorsi/Healthcare-Resource-Allocation/blob/b9d838cc5ddf62201667c8bbc80f4005dc64ebe5/Dashboard%20measures.gif?raw=true" width="512"/>  
</p>

- **Hospitals**: for a comprehensive analysis of hospitals across different states
<p align="center">
  <img src="https://github.com/SoniaBorsi/Healthcare-Resource-Allocation/blob/b9d838cc5ddf62201667c8bbc80f4005dc64ebe5/Dashboard%20hospitals.gif?raw=true" width="512"/>  
</p>

- **Budget**: for a a comprehensive visual analysis of healthcare expenditure data
<p align="center">
  <img src="https://github.com/SoniaBorsi/Healthcare-Resource-Allocation/blob/b9d838cc5ddf62201667c8bbc80f4005dc64ebe5/Dashboard%20budget.gif?raw=true" width="512"/>  
</p>


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
