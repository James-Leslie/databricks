# databricks
Short, ideally self-contained, Databricks notebook tutorials

## Choosing the right Azure region for you Databricks workspace
When it comes to the step of creating a cluster in your workspace, you will need to choose which virtual machines you want to use as the driver and worker nodes.
These VMs need to be available to use in the Azure region you have chosen. Unfortunately, Databricks does not offer any hints as to which machines are/aren't available.
![cluster-screenshot](https://github.com/James-Leslie/databricks/blob/main/images/cluster-screenshot.png?raw=true)

Before creating your workspace, head to [this link](https://docs.microsoft.com/en-us/azure/azure-resource-manager/templates/error-sku-not-available#code-try-1
). Click on the "Try It" button and then enter the following command (change the --location and --size values as needed):
```bash
az vm list-skus --location australiaeast --size Standard_DS3_v2 --output table
```
