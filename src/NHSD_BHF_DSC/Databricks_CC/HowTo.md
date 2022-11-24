# How to run this code on DataBricks Community Edition
The Databricks community edition does not support GitHub integration and token-based access. This how-to article explains the steps required to update the source files and fake data files in Databricks. 

*Note:* The use case if to match the permissions in NHS Digital's BHF Data Science Centre TRE. For example, user defined Python libraries are not supported in NHSD TRE; therefore all libraries must be included in a notebook and called using `%run ` command. 

## Load and update the fake data

- Click on `Data`
- Click on `Create Table`
- In the `Update File` tab, enter the `Fake_data` in the `DBFS Target Directory` text box.
- Upload the contets of `NHS_BHF_DSC` folder from the local repository to Databricks
- The data will be added to the `DBFS/Filestore/tables/Fake_data`
- Update the files after each local update

## Updating ipynb files from local py files
- In the local system/environment/interpreter, install ipynb-py-convert using `pip install ipynb-py-convert`
  - Note: If you use PySpark in Docker, install within the image and commit the container
- Considering that local `.py` files are in `nhsd_docker_pyspark_package` and `.ipynb` are in `nhsd_databricks_package`:
  - Run a bash script in the same folder as these folders
``` 
    #!/bin/bash

    ipynb-py-convert nhsd_docker_pyspark_package/ChangedFile.py nhsd_databricks_package/ChangedFile.ipynb
```
 - Commit to a public repository
 - Copy the file link on GitHub
 - Import the `.ipynb` file to the Databricks workspace