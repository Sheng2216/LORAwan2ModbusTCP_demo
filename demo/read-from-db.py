import sqlite3
DATA_BASE_PATH = "databases/lorawan_sensor_data.db"

# Connect to the database
conn = sqlite3.connect(DATA_BASE_PATH)

# Create a cursor object
cur = conn.cursor()

# Execute a SELECT statement to retrieve the unit_id column
cur.execute('SELECT unit_id FROM sensor_data_table')

# Fetch all the rows and store the result in a list
unit_ids = cur.fetchall()
print(unit_ids)

# Loop through the unit_ids list and retrieve the corresponding columns
for unit_id in unit_ids:
    # print(unit_id[0])
    cur.execute("SELECT raw_data FROM sensor_data_table WHERE unit_id=?", (unit_id[0],))
    rows = cur.fetchall()
    # print(rows)
    for row in rows:
        print(row)

# Close the cursor and database connection
cur.close()
conn.close()
