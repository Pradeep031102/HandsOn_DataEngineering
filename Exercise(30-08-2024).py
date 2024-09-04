import pandas as pd

# Exercise 5: Handling Missing Values
data = {
    "Name": ["Amit", "Neha", "Raj", "Priya"],
    "Age": [28, None, 35, 29],
    "City": ["Delhi", "Mumbai", None, "Chennai"]
}
df = pd.DataFrame(data)

# 2. Fill missing values in the "Age" column with the average age
df['Age'] = df['Age'].fillna(df['Age'].mean())

# 3. Drop rows where any column has missing data
df = df.dropna()

# Exercise 6: Adding and Removing Columns
# 1. Add a new column "Salary"
df['Salary'] = [10000, 20000, 30000, 40000]

# 2. Remove the "City" column
df = df.drop(columns=['City'])

# Exercise 7: Sorting Data
# 1. Sort the DataFrame by "Age" in ascending order
df_sorted_age = df.sort_values(by='Age')

# 2. Sort the DataFrame first by "City" and then by "Age" in descending order
df_sorted_city_age = df.sort_values(by=['City', 'Age'], ascending=[True, False])

# Exercise 8: Grouping and Aggregation
# 1. Group by "City" and calculate the average "Age" for each city
grouped_avg_age = df.groupby('City')['Age'].mean()

# 2. Group by "City" and "Age", and count the number of occurrences for each group
grouped_count = df.groupby(['City', 'Age']).size()

# Exercise 9: Merging DataFrames
# 1. Create DataFrames
df1 = pd.DataFrame({
    "Name": ["Amit", "Neha", "Raj"],
    "Department": ["HR", "IT", "Finance"]
})

df2 = pd.DataFrame({
    "Name": ["Neha", "Raj", "Priya"],
    "Salary": [60000, 70000, 65000]
})

# 2. Merge df1 and df2 on "Name" using an inner join
merged_inner = pd.merge(df1, df2, on='Name', how='inner')

# 3. Merge df1 and df2 using a left join
merged_left = pd.merge(df1, df2, on='Name', how='left')
