#! /usr/bin/python3

import argparse
import logging
import pymysql.cursors
import getpass

def get_args():
    parser = argparse.ArgumentParser(description='Run the importer')
    requiredNamed = parser.add_argument_group('required arguments')
    requiredNamed.add_argument("--host",
                               type=str, help='Destionation database name',
                               required=True)
    requiredNamed.add_argument("--origin",
                               type=str,
                               help='Origin database name',
                               required=True)
    requiredNamed.add_argument("--dest",
                               type=str, help='Destionation database name',
                               required=True)
    parser.add_argument("-u", "--user",
                        type=str, default="root",
                        help='Specify username')
    parser.add_argument("-p", "--password",
                        action="store_true",
                        help="Importer will prompt for password")
    return parser.parse_args()

def get_db_conns(args):
    password=None
    if args.password:
        password = getpass.getpass(prompt='Enter you SQL password: ')

    sdb = pymysql.connect(host=args.host, user=args.user,
                                  password=password, db=args.origin)
    ddb = pymysql.connect(host=args.host, user=args.user,
                                  password=password, db=args.dest)
    return sdb, ddb

def get_xf_andy_rating(rating):
    if rating == -1:
        return 2
    elif rating == 0:
        return 1
    return 0

def import_row(row, cursor):
    if row[4] == "trade":
        print ("-- Trade require manual handling")
        answer = ""
        while answer != "s" or answer != "d" or answer != "b":
            answer = input("Enter transaction type (s=sell, b=buy, d=drop): ")
        if answer == "s":
            row[4] = "sell"
            import_row(row, cursor)
        if answer == "b":
            row[4] = "buy"
            import_row(row, cursor)
    else:
        rating = get_xf_andy_rating(row[3])
        if row[4] == "sell":
            cursor.execute(sql_insert,(row[0], row[6], rating, row[2], row[1], row[5], "",))
        else:
            cursor.execute(sql_insert, (row[0], row[6], rating, row[1], row[2], "", row[5],))

args        = get_args()
sdb, ddb    = get_db_conns(args)

sql = "SELECT \
       `fb_id`, `foruserid`, `fromuserid`, `amount`, \
       `type`, `review`, `dateline` \
       FROM xc_trade_feedback"

sql_insert = "INSERT INTO xf_andy_trader(fb_id, timestamp, rating, seller_id, \
                                         buyer_id, seller_comment, \
                                         buyer_comment) \
              VALUES (%s, %s, %s, %s, %s, %s, %s)"

sql_alter = "ALTER TABLE xf_andy_trader AUTO_INCREMENT = %s"

try:
    cursor          = sdb.cursor()
    insert_cursor   = ddb.cursor()
    auto_increment  = 1
    cursor.execute(sql, ())
    for row in cursor:
        print ("Importing transaction", row[0])
        auto_increment = row[0]
        import_row(row, insert_cursor)

    insert_cursor.execute(sql_alter, (auto_increment + 1,))
finally:
    sdb.close()
    ddb.close()
