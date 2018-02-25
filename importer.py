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

args = get_args()
sdb, ddb = get_db_conns(args)

sql = "SELECT \
       `fb_id`, `foruserid`, `fromuserid`, `amount`, \
       `type`, `review`, `dateline` \
       FROM xc_trade_feedback"

sql_insert = "INSERT INTO xf_andy_trader(fb_id, timestamp, rating, seller_id, \
                                         buyer_id, seller_comment, \
                                         buyer_comment) \
              VALUES (%s, %s, %s, %s, %s, %s, %s)"

try:
    cursor.execute(sql, ())
    for row in cursor:
        print(row)

    result = cursor.fetchall()
finally:
    sdb.close()
    ddb.close()
