import pandas as pd
import sqlite3

# Function to read data from CSV files
def read_data(file_path):
    return pd.read_csv(file_path)

# Function to transform data according to business rules
def transform_data(region_a_df, region_b_df):
    # Combine data from both regions
    combined_df = pd.concat([region_a_df, region_b_df], ignore_index=True)

    # Add region column
    combined_df['region'] = ['A'] * len(region_a_df) + ['B'] * len(region_b_df)

    # Calculate total_sales and net_sale
    combined_df['total_sales'] = combined_df['QuantityOrdered'] * combined_df['ItemPrice']
    combined_df['net_sale'] = combined_df['total_sales'] - combined_df['PromotionDiscount']

    # Remove duplicates based on OrderId
    combined_df.drop_duplicates(subset='OrderId', keep='first', inplace=True)

    # Exclude orders with negative or zero net_sales
    combined_df = combined_df[combined_df['net_sale'] > 0]

    return combined_df

# Function to load data into SQLite database
def load_data_to_db(df, db_name='sales_data.db'):
    conn = sqlite3.connect(db_name)
    df.to_sql('sales_data', conn, if_exists='replace', index=False)
    conn.commit()
    conn.close()

# Function to validate data with SQL queries
def validate_data(db_name='sales_data.db'):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # a. Count the total number of records
    cursor.execute("SELECT COUNT(*) FROM sales_data")
    total_records = cursor.fetchone()[0]
    print(f"Total number of records: {total_records}")

    # b. Find the total sales amount by region
    cursor.execute("SELECT region, SUM(total_sales) FROM sales_data GROUP BY region")
    total_sales_by_region = cursor.fetchall()
    print("Total sales amount by region:")
    for row in total_sales_by_region:
        print(f"Region {row[0]}: {row[1]}")

    # c. Find the average sales amount per transaction
    cursor.execute("SELECT AVG(net_sale) FROM sales_data")
    average_sales = cursor.fetchone()[0]
    print(f"Average sales amount per transaction: {average_sales}")

    # d. Ensure there are no duplicate OrderId values
    cursor.execute("SELECT COUNT(DISTINCT OrderId), COUNT(OrderId) FROM sales_data")
    distinct_order_count, total_order_count = cursor.fetchone()
    if distinct_order_count == total_order_count:
        print("No duplicate OrderId values found.")
    else:
        print("Duplicate OrderId values found.")

    conn.close()

# Main function to execute the ETL process
def main():
    # Read data from CSV files
    region_a_df = read_data('order_region_a.csv')
    region_b_df = read_data('order_region_b.csv')

    # Transform data
    transformed_data = transform_data(region_a_df, region_b_df)

    # Load data into the database
    load_data_to_db(transformed_data)

    # Validate data
    validate_data()

if __name__ == "__main__":
    main()