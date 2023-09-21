import csv
import json
import pandas as pd

def extract_features(json_data):
    extracted_data = []

    jobs = json_data.get("Jobs", {})
    for job_id, job_info in jobs.items():
        extracted_data.append([
            job_id, 
            job_info.get("resources_used", {}).get("cput", "N/A"),
            job_info.get("resources_used", {}).get("mem", "N/A"),
            job_info.get("resources_used", {}).get("ncpus", "N/A"),
            job_info.get("resources_used", {}).get("vmem", "N/A"),
            job_info.get("resources_used", {}).get("walltime", "N/A"),
            job_info.get("queue", "N/A"),
            job_info.get("ctime", "N/A"),
            job_info.get("Resource_List", {}).get("mpiprocs", "N/A"),
            job_info.get("Resource_List", {}).get("ncpus", "N/A"),
            job_info.get("Resource_List", {}).get("ngpus", "N/A"),
            job_info.get("Resource_List", {}).get("nodect", "N/A"),
            job_info.get("Resource_List", {}).get("walltime", "N/A"),
            job_info.get("Variable_List", {}).get("PBS_NCHUNKS", "N/A"),
            job_info.get("Variable_List", {}).get("PBS_NCPUS", "N/A"),
            job_info.get("Variable_List", {}).get("PBS_O_QUEUE", "N/A"),
            job_info.get("eligible_time", "N/A"),
            job_info.get("estimated", {}).get("start_time", "N/A")
        ])
    
    return extracted_data

def write_to_csv(data, filename):
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([
            "Job_ID",
            "CPUTime",
            "Memory",
            "NCpus",
            "VMem",
            "Walltime",
            "Queue",
            "Ctime",
            "Resource_List_mpiprocs",
            "Resource_List_ncpus",
            "Resource_List_ngpus",
            "Resource_List_nodect",
            "Resource_List_walltime",
            "PBS_NCHUNKS",
            "PBS_NCPUS",
            "PBS_O_QUEUE",
            "eligible_time",
            "Start_Time"
        ])
        writer.writerows(data)

def process_json_files(json_files, output_csv):
    for json_file in json_files:
        try:
            with open(json_file, 'r') as file:
                json_data = json.load(file)

            data = extract_features(json_data)
            write_to_csv(data, output_csv)
            print(f"Processed: {json_file}")
        except json.JSONDecodeError as e:
            print(f"Error processing JSON file: {json_file}")
            print(e)
        except Exception as e:
            print(f"An error occurred while processing {json_file}:")
            print(e)

# List of JSON files to process
json_files = [
r'C:\Users\anhad\Desktop\intenship\new json\extracted data\finishedjobs.20230811',
r'C:\Users\anhad\Desktop\intenship\new json\extracted data\finishedjobs.20230812',
r'C:\Users\anhad\Desktop\intenship\new json\extracted data\finishedjobs.20230813'
]

# Output CSV file
output_csv = 'test.csv'

# Process JSON files and write extracted data to the combined CSV file
process_json_files(json_files, output_csv)

# Load the combined CSV file using pandas (optional)
combined_df = pd.read_csv(output_csv)
