import sqlite3
import os
from time import time

# Lack of comments for some functionalities due to duplicate code
# Comments are added if there are significant changes
# Please refer to Part 1 for missing comments


def q5(cursor, np):
    """
    Find the quantity of parts that are not used
    in any other part with NOT EXISTS operator
    :param Cursor cursor: Cursor to the database
    :param String np: Number of runs
    """
    # Set number of runs per assignment requirement
    num = 50
    # Set counter for total query time
    query_time = 0
    for i in range(num):
        # Get time before executing query
        start = time()
        cursor.execute("""
        SELECT COUNT(*)
        FROM Parts p1
        WHERE NOT EXISTS (
        SELECT p2.partNumber
        FROM Parts p2
        WHERE p1.partNumber = p2.needsPart);
        """)
        # Get time after executing query
        end = time()
        # Add difference to total and convert to ms
        query_time += (end - start) * 1000
    # Get average by dividing total with number of runs
    avg = query_time / num
    print("Average Q5 time in %s runs for NP = %s: %.5f ms" % (num, np, avg))


def q6(cursor, np):
    """
    Find the quantity of parts that are not used
    in any other part with NOT IN operator
    :param Cursor cursor: Cursor to the database
    :param String np: Number of runs
    """
    num = 50
    if int(np) == 100000:
        num = 10
    elif int(np) == 1000000:
        num = 5
    query_time = 0
    for i in range(num):
        start = time()
        cursor.execute("""
        SELECT COUNT(*)
        FROM Parts p1
        WHERE p1.partNumber NOT IN (
        SELECT p2.needsPart
        FROM Parts p2);
        """)
        end = time()
        query_time += (end - start) * 1000
    avg = query_time / num
    print("Average Q6 time in %s runs for NP = %s: %.5f ms" % (num, np, avg))


def query_db(file):
    print("Opening %s" % file[0].strip("./"))
    conn = sqlite3.connect(file[0])
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    drop_index(cur)
    # Get size of database for analysis
    size = get_size(file[0])
    print("File size before indexing: %d%s" % (size[0], size[1]))
    # Execute both Q5 and Q6, then display their average query times
    # Execute Q5 and Q6 when NP is less than 100k
    if int(file[1]) < 100000:
        q5(cur, file[1])
        q6(cur, file[1])
    # Execute only Q6 when NP is 100k and higher
    if int(file[1]) >= 100000:
        q6(cur, file[1])
    conn.close()
    print("Closing %s\n" % file[0].strip("./"))


def query_db_idx(file):
    print("Opening %s" % file[0].strip("./"))
    conn = sqlite3.connect(file[0])
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    create_index(cur)
    # Get size of database for analysis
    size = get_size(file[0])
    print("File size before indexing: %d%s" % (size[0], size[1]))
    q6(cur, file[1])
    drop_index(cur)
    conn.close()
    print("Closing %s\n" % file[0].strip("./"))


def create_index(db_cursor):
    db_cursor.execute("""CREATE INDEX idxNeedsPart ON Parts (needsPart);""")


def drop_index(db_cursor):
    db_cursor.execute("""DROP INDEX IF EXISTS idxNeedsPart;""")
    db_cursor.execute("""VACUUM;""")


def get_size(filename):
    size = os.path.getsize(filename)
    if size >= 1024 ** 2:
        return size / 1024 ** 2, "MB"
    else:
        return size / 1024, "KB"


if __name__ == "__main__":
    db_100 = ("./A4v100.db", "100")
    db_1k = ("./A4v1k.db", "1000")
    db_10k = ("./A4v10k.db", "10000")
    db_100k = ("./A4v100k.db", "100000")
    db_1m = ("./A4v1M.db", "1000000")

    print("Executing Part 4\n")
    # Execute Q5 and Q6 without indexing
    print("Executing Tasks M and N\n")
    query_db(db_100)
    query_db(db_1k)
    query_db(db_10k)
    query_db(db_100k)
    query_db(db_1m)

    # Execute Q6 with indexing
    print("Executing Task Q (using index)\n")
    query_db_idx(db_100)
    query_db_idx(db_1k)
    query_db_idx(db_10k)
    query_db_idx(db_100k)
    query_db_idx(db_1m)
