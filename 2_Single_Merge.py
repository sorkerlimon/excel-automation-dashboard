from pathlib import Path
import os
import csv
import re
import pandas as pd
import time

starttime = time.time()
# Detect Current Path
path = Path(__file__).parent.absolute()  

# Loop through directory
for folder in os.listdir(path):  
    # Select only the matched directories
    if os.path.isdir(folder) and not folder.startswith("env") and not folder.startswith("Demo") and not folder.startswith("Combine") and folder.startswith("L"):
        header = False
        merged_lines = []

        
        print("\nCurrent Folder:       ", os.path.join(path, folder))

        # Loop through matched folder and search for excel/csv
        for excel in os.listdir(f"{path}\{folder}"):
            datafilename = os.path.join(path, folder, excel)
            print("\n\tCurrent File:       ", datafilename)

            # Read the excel/csv file 
            with open(datafilename, 'r') as infile:
                reader = csv.reader(infile)
                lines = list(reader)

                # Remove the extra contents at the begining
                if len(lines[0]) < 10:
                    del lines[0:7]

                    # Replace all the NIL occurrences with 0.0
                    for j in lines[:]:
                        for i in range(len(j)):
                            if j[i] == "NIL":
                                j[i] = 0.0

                # Detect the patern of cell name
                    cell_name_pattern = r"Cell Name=([\w\d]+)"
                    cell_elements = []
                    for i in lines[:]:
                        match = re.search(cell_name_pattern, i[3])
                        if match:
                            cell_name = match.group(1)
                            cell_elements.append(cell_name)

                    # Made a Cell Name column
                    lines[0].insert(3, 'Cell Name')

                    #  Split Date and Timo into seperate columns
                    first_elements = [sublist[0].split() for sublist in lines]
                    lines[0][0] = 'Date'
                    lines[0].insert(1, 'Time')

                    #  Place the Date, Time and Cellname to appropiate row of respective columns
                    for i in range(1, len(first_elements)):
                        lines[i].insert(3, cell_elements[i-1])
                        lines[i][0] = first_elements[i][1]
                        lines[i].insert(0, first_elements[i][0])

                    # Save CSV/Excel files
                    with open(datafilename, 'w', newline='') as outfile:
                        writer = csv.writer(outfile)
                        writer.writerows(lines)

                    # Merging all the csv from this folder into one file
                    name = f"{path}\\{folder}\\{folder}_merge.csv"
                    with open(os.path.join(path, folder, f"{folder}_merge.csv"), 'a', newline='') as mergedfile:
                        writer = csv.writer(mergedfile)
                        if not header:
                            writer.writerows(lines)
                            header = True
                        else:
                            writer.writerows(lines[1:])

                    print("\n\t\t*****Excel Updated*****\n" )
                else:
                    print("\n\t\t*****No Changes Made*****\n")


endtime = time.time()

print((endtime-starttime)/60)