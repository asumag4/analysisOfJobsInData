# Langchain
from langchain.prompts import PromptTemplate
from langchain_community.chat_models import ChatOpenAI

# Data Processing
import json
import re

import pandas as pd

# OpenAI 
# For API Key call and storage
import os
from dotenv import load_dotenv
load_dotenv()
open_api_key = os.getenv("OPENAI_API_KEY")
import openai

from openai import OpenAI

# Geolocation handling
google_api_key = os.getenv("GOOGLE_API_KEY")
from geopy.geocoders import GoogleV3
import pycountry

# Retrieiving saved files 
from pathlib import Path

# tqdm
from tqdm import tqdm
tqdm.pandas()

#========================================================================================================#

class Pipeline: 
    # This class object is meant to house flexible components regarding
    # job dataset features to standardize and extract features.

    def __init__(self, geolocator = None):
        
        # If no geolocator has been init, init one
        if geolocator is None: 
            geolocator = GoogleV3(google_api_key)
        self.geolocator = geolocator

    # --- SKILL SCANNER --- 
    def scan_for_new_skills(self, df, skill_col):
        
        # Refer to the obj instance's `unique_skills`
        unique_skills = self.unique_skills

        for row in df.itertuples():
            # Ignore "NaN" values in the dataframe
            if (isinstance(row[skill_col],float)):
                continue 
            # Parse through the string, separate on commas
            if (isinstance(row[skill_col],str)): 
                if "," in row[skill_col]: 
                    lst_values = row[skill_col].split(",")
                    # Parse to find new unique skills to be recorded
                    for skill in lst_values: 
                        skill = skill.strip().lower()
                        # Log the unique skill if not logged yet
                        if skill.lower() not in unique_skills.keys():
                            unique_skills[skill] = len(unique_skills) + 1
                        else: 
                            continue 
                # Handling single skill job skills
                else:
                    skill = row[skill_col].strip().lower()
                    if (len(skill) > 0):
                        if skill not in unique_skills:
                            unique_skills[skill] = len(unique_skills) + 1 
                    else: 
                        continue
        
        # Ensure that the obj instance `unique_skills` is saved
        self.unique_skills = unique_skills
        return unique_skills

    def convert_skill_to_tabulated_form(self, x):
        # Now convert the current values into new values 
        # NOTE: this function is meant to be a mapping function under `DataFrame.apply()` 
        
        # Refer to the obj instance's `unique_skills`
        unique_skills = self.unique_skills
        
        try:
            # Ignore "NaN" values in the dataframe
            if (isinstance(x,float)):
                return
            # Parse through the string, separate on commas
            if (isinstance(x,str)): 
                # If its a list of objects, 
                if "," in x:
                    lst_values = x.split(",")
                    lst_skills = []
                    # Parse to find new unique skills to be recorded
                    for skill in lst_values: 
                        skill = skill.strip().lower()
                        # Find the skill and append it to the `lst_skills`
                        lst_skills.append(unique_skills[skill])
                    # return ", ".join(lst_skills)
                    return str(lst_skills).strip("[").strip("]")
                else: 
                    # Only one skill found, find it
                    return unique_skills[x.strip().lower()]
        except:
            print(f'Failed on: {x}')
            return
        
    def get_tabulated_jobs(self):
        
        # Retrieve the saved data
        tab_skills_file = Path("../metadata/tabulated_skills.json")
        
        if tab_skills_file.is_file():
            with open(tab_skills_file, mode="r") as file:
                self.unique_skills = json.load(file)
            print(f"File {tab_skills_file} has been loaded into the pipeline.")
        else: 
            self.unique_skills = {}
            print(f"A new instance of tabulated skills data has been initiated")
    
    
    def save_tabulated_jobs(self):
        # Save the data via complete over-write 
        
        # Retrieve the saved data
        tab_skills_file = Path("../metadata/tabulated_skills.json")
        
        if tab_skills_file.is_file():
            print(f"The file {tab_skills_file} has been overwritten")
        else: 
            print(f"The file {tab_skills_file} has been created and saved")
            
        with open(tab_skills_file, mode="w") as file:
            json.dump(self.unique_skills, file)
    
    # --- SALARY EXTRACTOR ---
    
    def extract_salary_from_job_desc(self, job_description: str):
        # This is a function to extract salary ranges or discrete salaries from job postings 
        # We utilize AI in this instance to ensure that salaries are correctly extracted. This 
        # is because using Regex would be too tedious and too time consuming to handle all the 
        # cases/ formats of how companies have stated their salary ranges. Additionally, 
        # algorithms might extract non-salary data, such as bonuses, insurance coverages data. 

        # Initialize the client 
        client = OpenAI()

        # Declare the AI "agent", and extract out the data
        response = client.chat.completions.create(
            model="gpt-4.1-nano",  # or "gpt-3.5-turbo"
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are an expert data extraction assistant. Your task is to identify and extract salary information from job descriptions. "
                        "You always return the salary in a structured string format: \n"
                        "- If it's a salary range, return 'lowSalary, highSalary' (without single quotes)\n"
                        "- If it's a single salary value, return 'salary' (without single quotes)\n"
                        "- All values are returned as integers without symbols (e.g., '60000, 80000') (without single quotes)\n"
                        "- If no salary is mentioned, return nothing"
                    )
                },
                {
                    "role": "user",
                    "content": f"Extract the salary or salary range from this job posting:\n\n{job_description}"
                }
            ],
            temperature=0
        )
        return response.choices[0].message.content   
    
    def create_avg_salary(self, salary): 
        # This function extracts the average salary from the AI-extracted salary from the job experience
        if salary: 
            salary = salary.split(',')
            salary = [int(x) for x in salary]
            lenSalaries = len(salary)

            if (lenSalaries > 1):
                return sum(salary) / lenSalaries
            return float(salary[0])
        else:
            return
    
    # --- GEOLOCATION HANDLERS ---

    def get_coordinates(self, location_str):
        # We need to extract the tuple coordinates from geopy, so we will use 
        # a function to explicitly extract it out. The general .apply() function
        # will not work. 

        location = self.geolocator.geocode(location_str)
        if location:
            return f"{location.latitude}, {location.longitude}"
        return None
    
    
    def get_country_iso(self, coordinates):
        # Retrieve the `str` location
        location = self.geolocator.reverse_geocode((coordinates[0], coordinates[1]))

        if location:
            address = location.raw.get('address', {})
            country_code_alpha2 = address.get('country_code')
            if country_code_alpha2:
                try:
                    country_alpha3 = pycountry.countries.get(alpha_2=country_code_alpha2).alpha_3
                    return country_alpha3
                except:
                    return None
        return None

    def get_unique_locations(self, df, loc_col_key):
    # To prevent calling the our GoogleV3 Maps API too much, this function 
    # is meant to create a new table/dataframe that will extract the coordinates 
    # NOTE: `location_column`, will have to be called as df[['column']]

        locations_df = pd.DataFrame(df[loc_col_key].unique()).rename(mapper = {0: 'locations'}, axis = 1)

        locations_df['coordinates'] = locations_df[loc_col_key].progress_apply(self.get_coordinates)
        locations_df['country_iso'] = locations_df['coordinates'].progress_apply(self.get_country_iso)

        return locations_df

        


