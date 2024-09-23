#  Part 1: Setting Up the Environment

## Task 1: Create a Metastore
#### This is  done in the Databricks admin console

## Task 2: Create Department-Specific Catalogs
```sql{
        CREATE CATALOG marketing;
    CREATE CATALOG engineering;
    CREATE CATALOG operations;
}
```

## Task 3: Create Schemas for Each Department
```sql{
    CREATE SCHEMA marketing.ads_data;
    CREATE SCHEMA marketing.customer_data;
    CREATE SCHEMA engineering.projects;
    CREATE SCHEMA engineering.development_data;
    CREATE SCHEMA operations.logistics_data;
    CREATE SCHEMA operations.supply_chain;
}
```

# Part 2: Loading Data and Creating Tables

##  Task 4: Prepare Datasets
```python{
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize Spark session
spark = SparkSession.builder.appName("UnityDatasetPreparation").getOrCreate()

# Define schemas and create dataframes
# Marketing: Ad Performance
ad_performance_schema = StructType([
    StructField("ad_id", IntegerType(), False),
    StructField("campaign_name", StringType(), False),
    StructField("impressions", IntegerType(), False),
    StructField("clicks", IntegerType(), False),
    StructField("cost_per_click", DecimalType(10, 2), False),
    StructField("date", DateType(), False)
])

ad_performance_data = [
    (1, "Summer Sale", 10000, 500, 0.50, "2024-06-01"),
    (2, "Back to School", 15000, 750, 0.45, "2024-08-15"),
    (3, "Holiday Special", 20000, 1000, 0.60, "2024-12-01"),
    (4, "Spring Collection", 12000, 600, 0.55, "2024-03-15"),
    (5, "Flash Sale", 8000, 400, 0.40, "2024-05-01")
]

ad_performance_df = spark.createDataFrame(ad_performance_data, ad_performance_schema)

# Marketing: Customer Info
customer_info_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("signup_date", DateType(), False),
    StructField("last_purchase_date", DateType(), False)
])

customer_info_data = [
    (101, "Alice Johnson", "alice@example.com", "2023-01-15", "2024-06-20"),
    (102, "Bob Smith", "bob@example.com", "2023-03-22", "2024-07-05"),
    (103, "Charlie Brown", "charlie@example.com", "2023-05-10", "2024-08-12"),
    (104, "Diana Ross", "diana@example.com", "2023-07-18", "2024-09-03"),
    (105, "Edward Norton", "edward@example.com", "2023-09-30", "2024-10-15")
]

customer_info_df = spark.createDataFrame(customer_info_data, customer_info_schema)

# Engineering: Project Info
project_info_schema = StructType([
    StructField("project_id", IntegerType(), False),
    StructField("project_name", StringType(), False),
    StructField("start_date", DateType(), False),
    StructField("end_date", DateType(), False),
    StructField("status", StringType(), False),
    StructField("lead_engineer", StringType(), False)
])

project_info_data = [
    (201, "Mobile App Redesign", "2024-01-01", "2024-06-30", "In Progress", "Sarah Connor"),
    (202, "Cloud Migration", "2024-02-15", "2024-08-31", "Planning", "John Matrix"),
    (203, "AI Integration", "2024-03-01", "2024-12-31", "Not Started", "Dutch Schaefer"),
    (204, "Security Upgrade", "2024-04-10", "2024-09-30", "In Progress", "Ellen Ripley"),
    (205, "IoT Platform", "2024-05-01", "2025-04-30", "Not Started", "Rick Deckard")
]

project_info_df = spark.createDataFrame(project_info_data, project_info_schema)

# Save dataframes as tables in Unity Catalog
ad_performance_df.write.mode("overwrite").saveAsTable("marketing.ads_data.ad_performance")
customer_info_df.write.mode("overwrite").saveAsTable("marketing.customer_data.customer_info")
project_info_df.write.mode("overwrite").saveAsTable("engineering.projects.project_info")

# Perform SQL operations

# Query 1: Get top performing ads
spark.sql("""
    SELECT campaign_name, impressions, clicks, cost_per_click
    FROM marketing.ads_data.ad_performance
    WHERE clicks > 500
    ORDER BY clicks DESC
""").show()

# Query 2: Get recent customers
spark.sql("""
    SELECT name, email, last_purchase_date
    FROM marketing.customer_data.customer_info
    WHERE last_purchase_date >= '2024-08-01'
    ORDER BY last_purchase_date DESC
""").show()

# Query 3: Get ongoing engineering projects
spark.sql("""
    SELECT project_name, start_date, lead_engineer
    FROM engineering.projects.project_info
    WHERE status = 'In Progress'
    ORDER BY start_date
""").show()

# Query 4: Join marketing data to get customer ad performance
spark.sql("""
    SELECT c.name, c.email, a.campaign_name, a.clicks
    FROM marketing.customer_data.customer_info c
    JOIN marketing.ads_data.ad_performance a
    ON c.customer_id = a.ad_id
    ORDER BY a.clicks DESC
""").show()
}
```

```sql{
-- Task 5: Create Tables from the Datasets
CREATE TABLE marketing.ads_data.ad_performance (
    ad_id INT,
    impressions INT,
    clicks INT,
    cost_per_click DECIMAL(10, 2)
);

CREATE TABLE engineering.projects.project_info (
    project_id INT,
    project_name STRING,
    start_date DATE,
    end_date DATE
);

CREATE TABLE operations.logistics_data.shipments (
    shipment_id INT,
    origin STRING,
    destination STRING,
    status STRING
);

-- Insert sample data (you would typically use COPY INTO or other data loading methods)
INSERT INTO marketing.ads_data.ad_performance VALUES
(1, 1000, 50, 0.5),
(2, 2000, 100, 0.4),
(3, 1500, 75, 0.6);

INSERT INTO engineering.projects.project_info VALUES
(1, 'Project Alpha', '2024-01-01', '2024-06-30'),
(2, 'Project Beta', '2024-02-15', '2024-08-31'),
(3, 'Project Gamma', '2024-03-01', '2024-12-31');

INSERT INTO operations.logistics_data.shipments VALUES
(1, 'New York', 'Los Angeles', 'In Transit'),
(2, 'Chicago', 'Miami', 'Delivered'),
(3, 'Seattle', 'Boston', 'Processing');
}
```

#  Part 3: Data Governance Capabilities

## Task 6: Create Roles and Grant Access
```sql{
CREATE ROLE marketing_role;
CREATE ROLE engineering_role;
CREATE ROLE operations_role;
GRANT ALL PRIVILEGES ON CATALOG marketing TO marketing_role;
GRANT ALL PRIVILEGES ON CATALOG engineering TO engineering_role;
GRANT ALL PRIVILEGES ON CATALOG operations TO operations_role;
}
```

## Task 7: Configure Fine-Grained Access Control
```sql{
GRANT SELECT ON TABLE marketing.customer_data.* TO marketing_role;
GRANT SELECT ON TABLE engineering.projects.* TO engineering_role;

}
```
## Task 8: Enable and Explore Data Lineage
#### This is done in the Databricks UI

```sql{
-- Create a view to demonstrate lineage

CREATE VIEW marketing.ads_data.high_performing_ads AS
SELECT * FROM marketing.ads_data.ad_performance
WHERE clicks > 50;
}
```


## Task 9: Monitor Data Access and Modifications

#### This is typically done in the Databricks admin console

## Task 10: Explore Metadata in Unity Catalog
```sql
{
DESCRIBE TABLE EXTENDED marketing.ads_data.ad_performance;
DESCRIBE TABLE EXTENDED engineering.projects.project_info;
DESCRIBE TABLE EXTENDED operations.logistics_data.shipments;

-- Add descriptions to tables
COMMENT ON TABLE marketing.ads_data.ad_performance IS 'Table containing ad performance metrics';
COMMENT ON TABLE engineering.projects.project_info IS 'Table containing engineering project information';
COMMENT ON TABLE operations.logistics_data.shipments IS 'Table containing shipment tracking information';
}
```