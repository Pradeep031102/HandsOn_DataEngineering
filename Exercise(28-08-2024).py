import pandas as pd

# Creating a new dataset
data = {
    "Employee_ID": [101, 102, 103, 104, 105, 106],
    "Name": ["Rajesh", "Meena", "Suresh", "Anita", "Vijay", "Neeta"],
    "Department": ["HR", "IT", "Finance", "IT", "Finance", "HR"],
    "Age": [29, 35, 45, 32, 50, 28],
    "Salary": [70000, 85000, 95000, 64000, 120000, 72000],
    "City": ["Delhi", "Mumbai", "Bangalore", "Chennai", "Delhi", "Mumbai"]
}

df = pd.DataFrame(data)

# Exercise 1: Rename Columns
df = df.rename(columns={"Salary": "Annual Salary", "City": "Location"})
print(df)

# Exercise 2: Drop Columns
df = df.drop(columns=["Location"])
print(df)

# Exercise 3: Drop Rows
df = df[df["Name"] != "Suresh"]
print(df)

# Exercise 4: Handle Missing Data
mean_salary = df["Annual Salary"].mean()
df["Annual Salary"] = df["Annual Salary"].fillna(mean_salary)
print(df)

# Exercise 5: Create Conditional Columns
df["Seniority"] = df["Age"].apply(lambda x: "Senior" if x >= 40 else "Junior")
print(df)

# Exercise 6: Grouping and Aggregation
grouped_df = df.groupby("Department")["Annual Salary"].mean().reset_index()
print(grouped_df)
