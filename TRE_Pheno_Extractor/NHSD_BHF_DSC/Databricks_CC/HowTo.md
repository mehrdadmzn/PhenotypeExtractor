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

