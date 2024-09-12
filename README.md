# Wide World Imporers Sales Analysis in Microsoft Fabric
For this project, i performed an ETL to ingest the WWI sample warehouse into a Micfrosoft Fabric workspace from an on premise sql server database and then connected powerBI to the workspace's lakehouse to create a sales dashboard<br>
#### These are the major tasks i completed:
- Ingested the WWI sample database into a Microsoft Fabric warehouse from SQL Server uing dataflow Gen2.
- Created shortcuts in a lakehouse that points to the WWI tables stored in a warehouse in the same workspace.
- Connected a notebook to the lakehouse and explored the datasets using SparkSQL.
- Created a Business Demand Overview and user story for a sales dashboard on the sample datasets.
- Created a semantuc models that includes all the tables needed for the dashboard.
- Connected PowerBI desktop to the semantic model and built a sales dashboard from it.
- Added data activator alerts to key metrics in the dashboard to notify stakeholder when specific target or treshold is reached.
- Created docimentation on how to use the dashboard for stakeholders.
