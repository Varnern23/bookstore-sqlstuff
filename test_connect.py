# test_connect.py

# We will use mysql.connector as our API to interact with the MySQL database
import mysql.connector as mc

# Note: The pattern of interacting with a database in Python is
# similar to the pattern of interacting with files: open, process data
# (with the help of a buffer that mostly operates in the background),
# close. With the database, the *connector* is analagous to Python's
# *file handle*. The *cursor* is analagous to the buffer. (You may
# have encountered a buffer through the .flush() method.) The cursor
# acts as a type of buffer, storing results until they are read with a
# fetch operation or written to the database with a commit operation.

# Start by establishing a connection
conn = mc.connect(
    host="cscdata.centre.edu",
    user="db_agent_a1",        # change per team
    password="DataBron67!",  # your team's password
    database="gravity_books"
)

# Initialize the cursor
cur = conn.cursor()

# Retrieve the result set as a list of tuples
# We can then treat the resulting table much like 2D list
print("Sample titles from v_books:")
cur.execute("SELECT title FROM v_books LIMIT 3;")
table = cur.fetchall()
for i in range(len(table)): # iterate over rows
    for j in range(len(table[i])): # iterate over fields in this row
        print(table[i][j], end='\t')
    print() # print line break

# Alternatively, we can process one line at a time
# Each row is a Python tuple
print("\nBooks with 'gravity' in the title:")
cur.execute("SELECT DISTINCT title FROM book WHERE title LIKE '%gravity%';")
row = cur.fetchone()
while row:
    print(" -", row[0])
    row = cur.fetchone()
print()

# Always end by closing the cursor and the connector in reverse order
cur.close()
conn.close()

