import argparse
import sys
from datetime import datetime

from ConfigObject import ConfigObject

from util import open_database_connection


def check_arg(args=None):
    parser = argparse.ArgumentParser(description='Script to learn basic argparse')
    parser.add_argument('-env', '--environment',
                        help='--Snowflake_environment',
                        required=True)

    results = parser.parse_args(args)
    return results.environment.upper()


def drop_share(cur):
    cur.execute("use role accountadmin")
    cur.execute("show shares")
    shares = cur.fetchall()
    for sh in shares:
        if sh[1] == 'OUTBOUND' and sh[2].endswith('_Ê_SHARE'): #and not 'EDW_AUDIT_DB_DEV_PRD_AUDIT_SHARE' in sh[2] and not 'EDW_AUDIT_DB_STG_PRD_AUDIT_SHARE' in sh[2]
            share_creation_date = sh[0]
            delta = datetime.now() - datetime.strptime(share_creation_date.strftime("%Y-%m-%d"), '%Y-%m-%d')
            if delta.days >= 14:
                print(sh)
                destination_environment = sh[4].split(',')
                for i in range(len(destination_environment)):
                    if destination_environment[0] == 'CISCODEV':
                        drop_database_from_share('DEV', sh[2].split(".")[1])
                    elif destination_environment[0] == 'CISCOSTAGE':
                        drop_database_from_share('STG', sh[2].split(".")[1])
                    elif destination_environment[0] == 'CISCO':
                        drop_database_from_share('PRD', sh[2].split(".")[1])

                cur.execute("drop share %s" % sh[2].split(".")[1])


def drop_database_from_share(environment, share):
    snowflake_account = ConfigObject(filename=choose_environment(environment))
    connection = open_database_connection(snowflake_account)
    ctx = connection.cursor()

    ctx.execute("use role accountadmin")
    ctx.execute("show shares like '%s'" % share)
    for sh in ctx.fetchall():
        if sh[1] == 'INBOUND':
            share_db = sh[3]
            ctx.execute("drop database %s" % share_db)


def choose_environment(environment):
    if environment == 'DEV':
        return 'config_dev.ini'
    elif environment == 'STG':
        return 'config_stg.ini'
    elif environment == 'PRD':
        return 'config_prd.ini'
    else:
        print("wrong Environment")
        sys.exit(1)


def main():
    environment = check_arg(sys.argv[1:])

    snowflake_account = ConfigObject(filename=choose_environment(environment))
    connection = open_database_connection(snowflake_account)
    ctx = connection.cursor()

    try:
        drop_share(ctx)
    except Exception as e:
        str(e)
    finally:
        ctx.close()


main()
