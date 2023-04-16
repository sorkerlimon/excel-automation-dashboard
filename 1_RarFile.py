from pathlib import Path
import os
import tarfile

path = Path(__file__).parent.absolute()

for folder in os.listdir(path):
    # check if folder is a directory and not starting with "env" or "Demo" and starting with "L"
    if os.path.isdir(os.path.join(path, folder)) and not folder.startswith("env") and not folder.startswith("Demo") and folder.startswith("L"):
        for file in os.listdir(os.path.join(path, folder)):
            # check if file is a .tar.gz file
            if file.endswith(".tar.gz"):
                # extract the file in the same directory
                with tarfile.open(os.path.join(path, folder, file), 'r:gz') as tar_ref:
                    tar_ref.extractall(os.path.join(path, folder))
                
                # delete the .tar.gz file after extraction
                os.remove(os.path.join(path, folder, file))
