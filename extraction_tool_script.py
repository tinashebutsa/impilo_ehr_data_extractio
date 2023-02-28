import os
import re
import pandas as pd
import io
import time
import datetime
from csv import reader

# Set the folder path containing the SQL dump files
folder_path = r"path/to/folder"

# Create an empty dictionary to store the databases and tables
failed = {}
fac_tables = {}
# Loop through all files in the folder
for filename in os.listdir(folder_path):  
    start_time = time.time()
    db_tables = {}
    if filename.endswith(".sql"):
        # Extract the database name from the filename
        facility = re.match(r"(.+)\.sql", filename).group(1)
        # Initialize an empty list to store the table names
        # Open the file and read its contents
        with open(os.path.join(folder_path, filename), "r", encoding='ISO-8859-1') as f:
            content = f.read()
        database_statements = re.split(r'(?<=\n)CREATE DATABASE', content)
        for statement in database_statements[1:]:
            table_names = []
            database_name = re.findall(r'`(\w+)`', statement)[0]
            # if database_name == "mrs":
            #     continue
            # Use regular expressions to extract the table names
            pattern = re.compile(r"CREATE TABLE `(.+?)`", re.DOTALL)
            matches = pattern.findall(statement)
            # Loop through the matches and append the table names to the list
            for match in matches:
                table_names.append(match)
            # Create a dictionary of dataframes for the tables
            tables_dict = {}
            for table_name in table_names:
                
                table_str = re.search(rf'CREATE TABLE `{table_name}`.+?ENGINE=InnoDB DEFAULT CHARSET=utf8;', statement, re.DOTALL)
                if table_str is None:
                    continue
                table_str  = table_str.group(0).strip()
                columns = re.findall(r'`(\w+)` (\w+)', table_str)
                column_names = [column[0] for column in columns if not column[0].startswith("fk")]
                data = re.findall(rf'INSERT INTO `{table_name}` VALUES \(.+?\);', statement, re.DOTALL)
                rows = {}
                values_final = []
                if data:
                    values_list = []
                    for row in data:
                        row = re.findall(r'\((.*?)\)', row)
                        # Extract the values for this row
                        # values1 = [i.replace("'", "").split(",") for i in row]
                        # values2 = [re.split("(?<!'),(?!')", i) for i in row]
                        values2 = [ re.split(r',(?=")', s) for s in row]
                        # print(values2)
                        for item in values2:
                            item = item[0].replace('NULL', "'NULL'")
                            rec = [i.replace("'","").lstrip() for i in item.split("',")]
                            values_list.append(rec)
                    for column in column_names:
                        coulum_data = []
                        column_index = column_names.index(column)
                        for record in values_list:
                            if column_index < len(record):
                                coulum_data.append(record[column_index])
                            else:
                                coulum_data.append("")  

                            # print(record[column_index])
                        rows[column] = coulum_data

                    table_df = pd.DataFrame(rows)
                    table_df = table_df.to_dict(orient="records")
                    tables_dict[table_name] = table_df
                else:
                    table_df = pd.DataFrame(columns=column_names)
                    tables_dict[table_name] = table_df
                # Add the database and its tables to the dictionary
            db_tables[database_name] = tables_dict
        fac_tables[facility] = db_tables
        end_time = time.time() - start_time
        
        print(f"** PROCESSED >>> {facility} | time taken: {str(datetime.timedelta(seconds=end_time))} **")
       
fac_tables.keys()