import argparse
import sys
import pdb
from util import open_database_connection
from ConfigObject import ConfigObject
from datetime import datetime


def check_arg(args=None):
    parser = argparse.ArgumentParser(description='Script to learn basic argparse')
    parser.add_argument('-f', '-F', '--file',
                        help='share_file',
                        required=True)
    parser.add_argument('-r', '-R', '--role',
                        help='role_name',
                        default='EDW_DATALAKE_ROLE')
    parser.add_argument('-d', '-D', '--destination',
                        help='Destination environment',
                        required=True)

    results = parser.parse_args(args)

    return results.file, list(map(lambda x: x.upper(), results.role.split(","))), list(
        map(lambda x: x.upper(), results.destination.split(",")))


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


def create_share(cs, database, schema, tables, share, src):
    cs.execute("use role accountadmin")
    cs.execute("show shares like '{}_{}%'".format(database, src))
    pdb.set_trace()
    shares = cs.fetchall()
    if shares:
        for sh in shares:
            if sh[1] == 'OUTBOUND' and sh[3] == database:
                share_name = sh[2].split('.')[1]
                delta = datetime.now() - datetime.strptime(sh[0].strftime("%Y-%m-%d"), '%Y-%m-%d')
                account = sh[4]
                cs.execute("alter share {} remove account={}".format(share_name, account))
                destination_environment = account.split(',')
                for i in range(len(destination_environment)):
                    if destination_environment[i] == 'CISCODEV':
                        drop_database_from_share('DEV', sh[2].split(".")[1])
                    elif destination_environment[i] == 'CISCOSTAGE':
                        drop_database_from_share('STG', sh[2].split(".")[1])
                    elif destination_environment[i] == 'CISCO':
                        drop_database_from_share('PRD', sh[2].split(".")[1])

                if delta.days < 10:
                    cs.execute("revoke usage on database {} from share {}".format(database, share_name))
                else:
                    cs.execute("drop share %s" % share_name)
                    cs.execute("create or replace share %s comment = 'Schema Level Share '" % share_name)

                share = share_name

    else:
        cs.execute("create or replace share %s comment = 'Schema Level Share '" % share)

    cs.execute("grant usage on database %s to share %s" % (database, share))
    cs.execute("grant usage on schema %s.%s to share %s" % (database, schema, share))
    for table in tables:
        cs.execute("grant select on %s.%s.%s to share %s" % (database, schema, table, share))


def create_database_from_share(cs, db, account, share, role_name):
    database = db + '_DB'
    # cs.execute("use role accountadmin")
    sql = "create or replace database %s from share %s.%s" % (database, account, share)
    cs.execute(sql)
    for role in role_name:
        cs.execute("grant imported privileges on database %s to %s" % (database, role))


if __name__ == '__main__':
    file, role_list, dest_env = check_arg(sys.argv[1:])
    db_schema = file.split(".")
    db, schema = db_schema[0].split("@")[0].upper(), db_schema[0].split("@")[1].upper()
    dev_choices = ['_DV1', '_DV2', '_DV3']
    stg_choices = ['_TS1', '_TS2', '_TS3']

    with open(file, 'r') as f:
        # [myNames.replace(",", "") for myNames in [line.strip() for line in f]
        tables = [tables.replace(",", "") for tables in [line.strip() for line in f]]
    f.close()

    if db.endswith(tuple(dev_choices)):
        src_env = 'DEV'
    elif db.endswith(tuple(stg_choices)):
        src_env = 'STG'
    else:
        src_env = 'PRD'

    config_provider = ConfigObject(filename=choose_environment(src_env))
    ctx_provider = open_database_connection(config_provider)
    src_cur = ctx_provider.cursor()

    pdb.set_trace()
    share = "%s_%s_%s_SHARE" % (db, src_env, '_'.join(dest_env))

    create_share(src_cur, db, schema, tables, share, src_env)
    print(role_list)
    for dst in dest_env:
        config_customer = ConfigObject(filename=choose_environment(dst))
        ctx_customer = open_database_connection(config_customer)
        dst_cur = ctx_customer.cursor()

        customer_account = str(config_customer.config_properties.account).split(".")[0]
        provider_account = str(config_provider.config_properties.account).split(".")[0]

        src_cur.execute("alter share %s add accounts=%s" % (share, customer_account))
        create_database_from_share(dst_cur, share, provider_account, share, role_list)
        ctx_customer.close()
    ctx_provider.close()

# list(map(lambda x: x.upper(), results.role.split(",")))
