import os
import glob

#define the folder containing the files to delete
output_folder = 'output/'

#get a list of all files in the output folder
files_to_delete = glob.glob(os.path.join(output_folder, '*'))

#iterate through the files and delete them
for file_path in files_to_delete:
    try:
        os.remove(file_path)
        print(f"Deleted: {os.path.basename(file_path)}")
    except Exception as e:
        print(f"Failed to delete {os.path.basename(file_path)}: {e}")