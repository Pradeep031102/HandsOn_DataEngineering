import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Exercise 1
data = {
    'Product': ['Laptop', 'Mouse', 'Monitor', 'Keyboard', 'Phone'],
    'Category': ['Electronics', 'Accessories', 'Electronics', 'Accessories', 'Electronics'],
    'Price': [80000, 1500, 20000, 3000, 40000],
    'Quantity': [10, 100, 50, 75, 30]
}
df = pd.DataFrame(data)
print(df)

# Exercise 2
print(df.head(3))
print(df.columns)
print(df.index)
print(df.describe())

# Exercise 3
print(df[['Product', 'Price']])
print(df[df['Category'] == 'Electronics'])

# Exercise 4
print(df[df['Price'] > 10000])
print(df[(df['Category'] == 'Accessories') & (df['Quantity'] > 50)])

# Exercise 5
df['Total Value'] = df['Price'] * df['Quantity']
print(df)
df = df.drop(columns=['Category'])
print(df)

# Exercise 6
print(df.sort_values(by='Price', ascending=False))
print(df.sort_values(by=['Quantity', 'Price'], ascending=[True, False]))

# Exercise 7
print(df.groupby('Category')['Quantity'].sum())
print(df.groupby('Category')['Price'].mean())

# Exercise 8
df.loc[1:2, 'Price'] = [None, None]
mean_price = df['Price'].mean()
df['Price'].fillna(mean_price, inplace=True)
df = df[df['Quantity'] >= 50]
print(df)

# Exercise 9
df['Price'] = df['Price'] * 1.05
df['Discounted Price'] = df['Price'] * 0.90
print(df)

# Exercise 10
supplier_df = pd.DataFrame({
    'Product': ['Laptop', 'Mouse', 'Monitor', 'Keyboard', 'Phone'],
    'Supplier': ['Supplier A', 'Supplier B', 'Supplier C', 'Supplier D', 'Supplier E']
})
merged_df = pd.merge(df, supplier_df, on='Product')
print(merged_df)

# Exercise 11
pivot_table = pd.pivot_table(df, values='Quantity', index='Product', columns='Category', aggfunc='sum', fill_value=0)
print(pivot_table)

# Exercise 12
store_a = pd.DataFrame({
    'Product': ['Laptop', 'Mouse'],
    'Price': [80000, 1500],
    'Quantity': [10, 100]
})
store_b = pd.DataFrame({
    'Product': ['Monitor', 'Keyboard'],
    'Price': [20000, 3000],
    'Quantity': [50, 75]
})
combined_df = pd.concat([store_a, store_b])
print(combined_df)

# Exercise 13
dates = [datetime.now() - timedelta(days=i) for i in range(5)]
sales = np.random.randint(1000, 5000, size=5)
date_df = pd.DataFrame({'Date': dates, 'Sales': sales})
print(date_df)
print(date_df['Sales'].sum())

# Exercise 14
melt_df = pd.DataFrame({
    'Product': ['Laptop', 'Mouse'],
    'Region': ['North', 'South'],
    'Q1_Sales': [50000, 1500],
    'Q2_Sales': [70000, 2500]
})
melted_df = pd.melt(melt_df, id_vars=['Product', 'Region'], var_name='Quarter', value_name='Sales')
print(melted_df)

# Exercise 15
# Read from 'products.csv'
df_csv = pd.read_csv('products.csv')
# Perform operations
df_csv['New Column'] = df_csv['Price'] * 1.1
# Write to 'updated_products.csv'
df_csv.to_csv('updated_products.csv', index=False)

# Exercise 16
rename_df = pd.DataFrame({
    'Prod': ['Laptop', 'Mouse'],
    'Cat': ['Electronics', 'Accessories'],
    'Price': [80000, 1500],
    'Qty': [10, 100]
})
rename_df.columns = ['Product', 'Category', 'Price', 'Quantity']
print(rename_df)

# Exercise 17
multiindex_df = pd.DataFrame({
    'Store': ['Store A', 'Store A', 'Store B', 'Store B'],
    'Product': ['Laptop', 'Mouse', 'Monitor', 'Keyboard'],
    'Price': [80000, 1500, 20000, 3000],
    'Quantity': [10, 100, 50, 75]
})
multiindex_df.set_index(['Store', 'Product'], inplace=True)
print(multiindex_df)

# Exercise 18
dates = pd.date_range(start=datetime.now() - timedelta(days=29), periods=30)
sales = np.random.randint(1000, 5000, size=30)
time_series_df = pd.DataFrame({'Date': dates, 'Sales': sales})
time_series_df.set_index('Date', inplace=True)
weekly_sales = time_series_df.resample('W').sum()
print(weekly_sales)

# Exercise 19
duplicate_df = pd.DataFrame({
    'Product': ['Laptop', 'Mouse', 'Laptop', 'Mouse'],
    'Price': [80000, 1500, 80000, 1500],
    'Quantity': [10, 100, 10, 100]
})
print(duplicate_df.duplicated())
cleaned_df = duplicate_df.drop_duplicates()
print(cleaned_df)

# Exercise 20
features_df = pd.DataFrame({
    'Height': [170, 160, 180],
    'Weight': [65, 55, 75],
    'Age': [30, 25, 40],
    'Income': [50000, 60000, 70000]
})
correlation_matrix = features_df.corr()
print(correlation_matrix)

# Exercise 21
sales_df = pd.DataFrame({
    'Date': pd.date_range(start=datetime.now() - timedelta(days=29), periods=30),
    'Sales': np.random.randint(1000, 5000, size=30)
})
sales_df['Cumulative Sales'] = sales_df['Sales'].cumsum()
sales_df['Rolling Avg'] = sales_df['Sales'].rolling(window=7).mean()
print(sales_df)

# Exercise 22
names_df = pd.DataFrame({
    'Names': ['John Doe', 'Jane Smith', 'Sam Brown']
})
names_df[['First Name', 'Last Name']] = names_df['Names'].str.split(expand=True)
names_df['First Name'] = names_df['First Name'].str.upper()
print(names_df)

# Exercise 23
employee_df = pd.DataFrame({
    'Employee': ['Alice', 'Bob', 'Charlie'],
    'Age': [45, 35, 50],
    'Department': ['HR', 'IT', 'Finance']
})
employee_df['Status'] = np.where(employee_df['Age'] >= 40, 'Senior', 'Junior')
print(employee_df)

# Exercise 24
product_df = pd.DataFrame({
    'Product': ['Laptop', 'Mouse', 'Monitor', 'Keyboard'],
    'Category': ['Electronics', 'Accessories', 'Electronics', 'Accessories'],
    'Sales': [70000, 1500, 25000, 3500],
    'Profit': [15000, 500, 6000, 800]
})
print(product_df.head(10))
print(product_df[product_df['Category'] == 'Electronics'])
print(product_df[product_df['Sales'] > 50000][['Sales', 'Profit']])

# Exercise 25
store_a_df = pd.DataFrame({
    'Employee': ['Alice', 'Bob'],
    'Age': [25, 30],
    'Salary': [50000, 60000]
})
store_b_df = pd.DataFrame({
    'Employee': ['Charlie', 'David'],
    'Age': [35, 40],
    'Salary': [70000, 80000]
})
combined_df = pd.concat([store_a_df, store_b_df], axis=0)
print(combined_df)

dept_df = pd.DataFrame({
    'Employee': ['Alice', 'Bob'],
    'Department': ['HR', 'IT']
})
salary_df = pd.DataFrame({
    'Employee': ['Alice', 'Bob'],
    'Salary': [50000, 60000]
})
merged_df = pd.merge(dept_df, salary_df, on='Employee')
print(merged_df)

# Exercise 26
features_df = pd.DataFrame({
    'Product': ['Laptop', 'Mouse'],
    'Features': [['Feature1', 'Feature2'], ['Feature3']]
})
exploded_df = features_df.explode('Features')
print(exploded_df)

# Exercise 27
df = pd.DataFrame({
    'Product': ['Laptop', 'Mouse'],
    'Price': [80000, 1500],
    'Quantity': [10, 100]
})
df['Price'] = df['Price'].map(lambda x: x * 1.10)
df = df.applymap(lambda x: f'{x:.2f}' if isinstance(x, (int, float)) else x)
print(df)

# Exercise 28
city_df = pd.DataFrame({
    'City': ['New York', 'Los Angeles', 'New York', 'Los Angeles'],
    'Product': ['Laptop', 'Mouse', 'Monitor', 'Keyboard'],
    'Sales': [700
