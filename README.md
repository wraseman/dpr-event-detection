# Event Detection Framework for Potable Reuse
Code for event detection framework for proactive and rapid response to critical control point (CCP) failures for direct potable reuse (DRP) facilities. 

# Background
DPR is the planned introduction of recycled water into a public drinking water system or upstream of a raw water intake to a drinking water plant. The key feature distinguishing direct potable reuse (DPR) from indirect potable reuse (IPR) is the loss of an environmental buffer. Due to the lack of environmental buffer in DPR, treated water from these facilities can reach customers in minutes or hours, defined as the retention time. Therefore, CCP failures must be detected and resolved in less than the retention time to avoid sending out of spec water to customers. The event detection framework aims to identify software-based solutions to support proactive and rapid responses to DPR failures. To do so, the framework is designed to increase lead time and decrease time for event detection and resolution.

# Files (Core)
- `main.py`: event detection script
- `library.py`: functions used in main.py
- `config.xlsx`: data about tags and events used in main.py

# Files (Dashboard Styling)
- `style1.css`: styling for the event summary page of the dashboard
- `style2.css`: styling for the event detail page of the dashboard
- `logo_all.png`: project team logos

# Data
For security reasons, all identifying information about databases have been removed from this repository. But for reproducibility of results, data has been exported to CSV files and stored in this repository. To run the code, either 1) upload CSV to a database and update the database with database connection information or 2) modify the code to read the CSV data directly. 

# Dependencies
Required Python packages include: 
- datetime
- json
- loguru
- matplotlib
- numpy
- os
- pandas
- pecos
- plyer
- seaborn
- sqlalchemy
- sys
- urllib
- warnings

See `env.yml` for the Python environment (package versions) used to run the scripts. 

# Funding
Research funded through Water Research Foundation Project 4954 "Integration of high-frequency performance data for microbial and chemical compounds control in potable reuse treatment systems" https://www.waterrf.org/research/projects/integration-high-frequency-performance-data-microbial-and-chemical-compounds. 
