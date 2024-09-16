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

## ETL Flowchart 
![WWI ETL flowchart](https://github.com/user-attachments/assets/3d2877dd-500d-44f2-ae66-1a3baccc2e8d)

## Dasboard development 
![1723845672792](https://github.com/user-attachments/assets/c7e3cfba-6403-4abf-8073-adea8e99a52d)

PS: Read a more detailed blog post about this dashboard [here](https://medium.com/@mubaraqabdulmaleek/end-to-end-etl-and-sales-dashboard-on-wwi-dataset-in-microsoft-fabric-01953545172c) 

## Business Demand Overview

### Business Problem:
The sales team currently lacks a comprehensive dashboard to view sales insights, causing delays in decision-making and missed opportunities to optimize sales strategies.

### Objective:
Create a dynamic Sales Dashboard that visualizes key sales data from various dimensions (customer, product, region, and time) to allow stakeholders to monitor performance, identify trends, and take data-driven actions. The dashboard should be user-friendly, accessible, and updated in real-time.

### Stakeholders:
- **Sales Manager**: Needs to monitor overall sales performance and identify improvement areas.
- **Sales Representatives**: Require detailed insights into territories and customer segments.
- **Marketing Team**: Interested in understanding customer behavior and product popularity.
- **Logistics Team**: Needs delivery performance data to optimize operations.
- **Executives**: Require high-level overviews for strategic decision-making.

## User Stories & Features

1. **Sales Performance Overview (Sales Manager)**:
   - View **Total Sales**, **Total Profit**, and **Profit Margin** metrics.
   - Visual can be filtered by year, month, customer, product, and sales territory.
   - Indicator color changes to red if profit margin drops below 30%.

2. **Regional Sales Analysis (Regional Sales Director)**:
   - A map visual shows sales across different cities.
   - Ability to drill up/down between city and country level.
   - Tooltip displays profit for the hovered location.

3. **Sales Trends Analysis (Business Analyst)**:
   - Area chart visualizes sales and profit trends by month.
   - Drill up/down functionality between month and year.
   - Filterable by customer, product, and sales territory.

4. **Top-Selling Products (Marketing Analyst)**:
   - Bar chart lists the top 10 best-selling products.
   - Tooltip shows total quantity sold and unit price for each product.
   - Filterable by time, region, and other slicers.

5. **Delivery Performance (Logistics Manager)**:
   - Table shows average delivery days for each product compared to expected delivery days.
   - Filterable by region and time.
   - Data activator alert sends an email if average delivery days exceed the threshold.

## Data Gathering & Preparation

- The **WWI sample database** was restored in SQL Server and imported into Fabric.
- Data was structured by storing original tables in a **warehouse**, with a **shortcut** created in a **lakehouse** to explore the tables.
- A new `FactSales` view was created using sales and transactions tables to link the payment method key to each sales transaction.

### Steps:
1. Created a **cross-database query** to generate a view combining sales and transactions data.
2. Transformed the view into a table due to performance considerations.
3. Created shortcuts from the warehouse to the lakehouse for analysis.

## Creating the Semantic Model

- Built a **custom direct lake semantic model** in Fabric using the `FactSales` table and dimension tables.
- Defined relationships between the tables and created key measures.
- Connected the model to PowerBI Desktop for further visualization and report building.

## DashboardVisuals in PowerBI Desktop

- **Dynamic KPI cards** were created for the Sales Manager to display Total Sales, Total Profit, and Profit Margin.
- **Map visuals** for regional sales analysis.
- **Area charts** for trends analysis over time.
- **Bar charts** for top-selling products.
- **Table with alerts** for monitoring delivery performance.







---

Thanks for reading!

