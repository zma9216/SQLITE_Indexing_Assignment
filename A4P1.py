import sqlite3
import os
from time import time


def q1(cursor, np):
    """
    Finds the price of part that has partNumber equal to
    a randomly selected UPC code that exist in Parts
    :param Cursor cursor: Cursor to the database
    :param String np: Number of runs
    """
    # Set initial number of runs per assignment requirement
    num = 50
    # Set number of runs to 10 if db has NP = 100k
    if int(np) == 100000:
        num = 10
    # Set number of runs to 10 if db has NP = 1M
    elif int(np) == 1000000:
        num = 5
    # Set counter for total query time
    query_time = 0
    for i in range(num):
        # Get time before executing query
        start = time()
        cursor.execute("""
        SELECT partPrice
        FROM Parts  
        WHERE partNumber in
        (SELECT partNumber
        FROM Parts
        ORDER BY RANDOM()
        LIMIT 1);
        """)
        # Get time after executing query
        end = time()
        # Add difference to total and convert to ms
        query_time += (end - start) * 1000
    # Get average by dividing total with number of runs
    avg = query_time / num
    print("Average Q1 time in %s runs for NP = %s: %.5f ms" % (num, np, avg))


def q2(cursor, np):
    """
    Finds the price of part that has needsPart equal to
    a randomly selected UPC code that exist in Parts
    :param Cursor cursor: Cursor to the database
    :param String np: Number of runs
    """
    # Set initial number of runs per assignment requirement
    num = 50
    # Set number of runs to 10 if db has NP = 100k
    if int(np) == 100000:
        num = 10
    # Set number of runs to 10 if db has NP = 1M
    elif int(np) == 1000000:
        num = 5
    # Set counter for total query time
    query_time = 0
    for i in range(num):
        # Get time before executing query
        start = time()
        cursor.execute("""
        SELECT partPrice
        FROM Parts
        WHERE needsPart in
        (SELECT partNumber
        FROM Parts
        ORDER BY RANDOM()
        LIMIT 1);
        """)
        # Get time after executing query
        end = time()
        # Add difference to total and convert to ms
        query_time += (end - start) * 1000
    # Get average by dividing total with number of runs
    avg = query_time / num
    print("Average Q2 time in %s runs for NP = %s: %.5f ms" % (num, np, avg))


def query_db(file):
    """
    Function for connecting to database and executing queries without indexing
    :param Tuple file: Tuple indicates which database is to be connected and what NP it is
    """
    # Message for opening database
    print("Opening %s" % file[0].strip("./"))
    # Connect to database
    conn = sqlite3.connect(file[0])
    # Get Cursor object
    cur = conn.cursor()

    # Drop index if it exists
    drop_index(cur)
    # Get size of database for analysis
    # size = get_size(file[0])
    # print("File size before indexing: %d%s" % (size[0], size[1]))
    # Execute both Q1 and Q2, then display their average query times
    q1(cur, file[1])
    q2(cur, file[1])
    # Close connection to database
    conn.close()
    # Message for closing database
    print("Closing %s\n" % file[0].strip("./"))


def query_db_idx(file):
    """
    Function for connecting to database and executing queries with indexing
    :param Tuple file: Tuple indicates which database is to be connected and what NP it is
    """
    # Message for opening database
    print("Opening %s" % file[0].strip("./"))
    # Connect to database
    conn = sqlite3.connect(file[0])
    # Get Cursor object
    cur = conn.cursor()

    # Create index if it does not exists
    create_index(cur)
    # Get size of database for analysis
    size = get_size(file[0])
    # print("File size before indexing: %d%s" % (size[0], size[1]))
    # Execute both Q1 and Q2, then display their average query times
    q1(cur, file[1])
    q2(cur, file[1])
    # Drop index for part 2 of assignment
    drop_index(cur)
    # Close connection to database
    conn.close()
    # Message for closing database
    print("Closing %s\n" % file[0].strip("./"))


def create_index(db_cursor):
    # Create index for lookups
    db_cursor.execute("""CREATE INDEX IF NOT EXISTS idxNeedsPart ON Parts (needsPart);""")


def drop_index(db_cursor):
    # Delete index and clean/optimize database
    db_cursor.execute("""DROP INDEX IF EXISTS idxNeedsPart;""")
    db_cursor.execute("""VACUUM;""")


def get_size(filename):
    """
    Function for getting size of database,
    then converts its size to appropriate units, e.g. KB or MB
    :param String filename: Filename of the database
    :return: The size of the database in either KB or MB
    :rtype: int
    """
    # Get size of database in bytes
    size = os.path.getsize(filename)
    # Convert to MB
    if size >= 1024 ** 2:
        return size / 1024 ** 2, "MB"
    # Convert to KB
    else:
        return size / 1024, "KB"


if __name__ == "__main__":
    # Set databases and their corresponding NP into tuples
    db_100 = ("./A4v100.db", "100")
    db_1k = ("./A4v1k.db", "1000")
    db_10k = ("./A4v10k.db", "10000")
    db_100k = ("./A4v100k.db", "100000")
    db_1m = ("./A4v1M.db", "1000000")

    print("Executing Part 1\n")
    # Execute Q1 and Q2 without indexing
    print("Executing Task A\n")
    query_db(db_100)
    query_db(db_1k)
    query_db(db_10k)
    query_db(db_100k)
    query_db(db_1m)

    # Execute Q1 and Q2 with indexing
    print("Executing Task C (using index)\n")
    query_db_idx(db_100)
    query_db_idx(db_1k)
    query_db_idx(db_10k)
    query_db_idx(db_100k)
    query_db_idx(db_1m)
