import sqlite3
import random
import csv


def read_upc(file):
    with open(file, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)
        data = [tuple(row)[0] for row in reader]
    return data


def read_codes(file):
    with open(file, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)
        data = [tuple(row)[1] for row in reader]
    return data


def populate_table(cursor, num_samples):
    upc_file = './upc_corpus.csv'
    codes_file = './country_codes.csv'
    max_sql_int = 2 ** 63 - 1
    upc = [int(i) for i in read_upc(upc_file) if i.isdigit() and int(i) < max_sql_int]
    codes = read_codes(codes_file)
    random.shuffle(upc)
    random.shuffle(codes)

    part_number = random.choices(upc, k=num_samples)
    part_price = [random.randrange(1, 100) for i in range(num_samples + 1)]
    need_parts = random.choices(upc, k=num_samples)
    made_in = random.choices(codes, k=num_samples)

    insertions = list(zip(part_number, part_price, need_parts, made_in))

    cursor.executemany("""
    INSERT OR REPLACE INTO Parts VALUES (?, ?, ?, ?);
    """, insertions)


if __name__ == "__main__":
    db_100 = "./A4v100.db"
    db_1k = "./A4v1k.db"
    db_10k = "./A4v10k.db"
    db_100k = "./A4v100k.db"
    db_1m = "./A4v1M.db"

    filename = db_1m
    conn = sqlite3.connect(filename)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    np = 10000
    populate_table(cur, np)

    conn.commit()
    conn.close()
