# Event Detection Framework for Direct Potable Reuse
Code for event detection framework for proactive and rapid response to critical control point (CCP) failures for direct potable reuse (DRP) facilities. 

![image](https://github.com/wraseman/dpr-event-detection/assets/14048152/9708750b-5421-4f2a-b645-132ea502e271)

# Background
DPR is the planned introduction of recycled water into a public drinking water system or upstream of a raw water intake to a drinking water plant. The key feature distinguishing DPR from indirect potable reuse (IPR) is the loss of an environmental buffer. Without a buffer, treated water from DPR facilities can reach customers in hours or minutes. Therefore, CCP failures must be detected and resolved in less than that time to avoid sending out off spececification water to customers. The event detection framework aims to identify software-based solutions to support proactive and rapid responses to DPR failures. To do so, the framework is designed to increase lead time and decrease time for event detection and resolution.

# Files (Core)
- `main.py`: event detection script. *User inputs needed.* Can be configured to run in "live" or "historical" mode. Live mode supports real-time analysis and historical mode allows users to perform retrospective analysis.
- `library.py`: functions used in main.py
- `config.xlsx`: data about tags and events used in main.py.

# Files (Dashboard Styling)
- `style1.css`: styling for the event summary page of the dashboard
- `style2.css`: styling for the event detail page of the dashboard
- `logo_all.png`: project team logos

# Data
For security reasons, all identifying information about databases have been removed from this repository. But for reproducibility of results, data has been exported to CSV files (`data1.csv` and `data2.csv` and stored in this repository). To reduce the volume of the dataset, the data is stored across two files. Throughout the operation of the demonstration facility, tags were added and modified. Therefore, some tags included in the event detection logic code may be missing from earlier time periods.

The following tables summarize the example events highlighted in the final report for Water Research Foundation Project 4954.

Example Events in Dataset 1 (`data1.csv`):
| Event                                     | Start datetime | End datetime   |
|-------------------------------------------|----------------|----------------|
| RO Process (Membrane Breach)              | 12/20/22 8:00  | 12/20/22 10:09 |
| RO Monitoring (Feed TOC Drift)            | 12/20/22 10:22 | 12/20/22 11:41 |
| MF Process (High Turbidity)               | 12/20/22 11:30 | 12/20/22 13:30 |
| UV Process (Low UV Dose)                  | 3/24/23 9:00   | 3/24/23 12:00  |
| UV Monitoring (UV Intensity Stagnant)     | 3/24/23 9:00   | 3/24/23 12:00  |
| UV Water Quality (Low UV Feed UVT)        | 3/24/23 9:00   | 3/24/23 12:00  |

Example Events in Dataset 2 (`data2.csv`):
| Event                                     | Start datetime | End datetime   |
|-------------------------------------------|----------------|----------------|
| MF Monitoring (Stagnant)                  | 6/8/23 15:30   | 6/8/23 16:12   |
| RO Water Quality (Chemical Peak)          | 6/8/23 15:30   | 6/8/23 17:00   |
| Ozone Process (Generator Failure)         | 6/30/23 13:25  | 6/30/23 14:24  |
| Ozone Water Quality (High Demand)         | 6/30/23 15:30  | 7/1/23 14:00   |
| Ozone Monitoring (Meter Drift)            | 8/31/23 11:00  | 8/31/23 19:00  |

# Outputs
- `dashboard.log`: log of all status updates, warnings, alerts, and errors encountered by the script. Resource for debugging, troubleshooting, and record keeping.
- `events.json`: log of events identified by the script from previous run. This file is most useful for live mode. 
- `dashboard.html`: local dashboard that can be opened with any web browser.
- `/dashboard`: resource files for `dashboard.html`. If these files are moved or modified, links to the dashboard may break.

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
