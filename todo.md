1. Prep Data
[ ] Use UV to create a working py env for this project.

# Data Cleaning & Staging
## Dimensions:

#### Skills:
[X] Format data to acceptable format 
[X] Change job skills into a compile list of unique values from all the records. Then give a unique ID for all the job skills within the dataset. Keep a standardized format of all the job skills. Do that for all the the datasets. 

#### Salary
[X] Ensure that the income is standardized to per year in all datasets (as the standard from AIJobs.net)

[ ] ~~Adjust salaries to 2025 USD format~~ Will do this in SQL for faster processing + inbuilt dimensions for the cube  

#### Companies:
[X] ~~Use AI to give you a standarized format~~  (*company names seem to be already in standardized format*)

#### Locations: 
[X] Use ~~AI~~ geopy to give you coordinates
[ ] Use 2 ISO-code to encode country data 
[ ] Decompose datasets as much as possible. 

#### Pushing to SQL:

[ ] Push data to SQL Database 
[ ] Create relationships in SQL Database
[ ] Harvest GDP per capita data 
[ ] Harvest industry relevance of skills
[ ] Need to extract coordinates from the map
[ ] Mapping for country names needs to be standardized.

# Reporting & Dashboard

1. Generate a Datastory
[ ] - Use streamlit to tell your data story 
[ ] - Adapt streamlit to PowerBI poster

1. Generate a Dashboard
[ ] - Use streamlit to create dashboard
[ ] - Adapt streamlit to PowerBI dashboard
[ ] - Create a "searchable" part where: create a template dashboard that houses premade charts; but the user will search for a job title. Query from SQL using WHERE -- LIKE statement to find the jobs we are interested in 

2. Table of metrics
| Metric Name          | Type    | Example Dimension         | Formula/Logic                          |
| -------------------- | ------- | ------------------------- | -------------------------------------- |
| Avg Salary by Title  | Numeric | `job_title`               | `AVG(salary)`                          |
| Skill Demand %       | Percent | `skill`                   | `skill_count / total_count`            |
| Job Growth (Monthly) | Percent | `job_title` or `location` | `(this_month - last_month)/last_month` |
| Posting Volume       | Count   | `location`, `company`     | `COUNT(*)`                             |
| Remote Job Ratio     | Percent | `job_type`                | `remote_count / total`                 |

# Automation/ ETL Pipeline

- Luke Barousse dataset and AiJobs.net dataset are both updated. So we can generate an web scraper to scrape AiJobs.net and then use API from kaggle to scrape the data and update our processes as such. 
- Probably would want to save a JSON of tabulated `unique_skills`, to reduce processing times throughout the pipeline for future processing. We'll need to make a function within class `Pipeline` to save and load 