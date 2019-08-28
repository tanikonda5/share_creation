from util import open_database_connection
from ConfigObject import ConfigObject
import argparse
import sys
from datetime import datetime


def check_arg(args=None):
    parser = argparse.ArgumentParser(description='Script to learn basic argparse')
    parser.add_argument('-p', '--p_database',
                        help='provider_database',
                        required=True)
    parser.add_argument('-c', '--share_name',
                        help='share_name',
                        required=True)
    parser.add_argument('-s', '--schemas',
                        help='schemas',
                        default=None)
    parser.add_argument('-t', '--tables',
                        help='tables',
                        default=None)
    parser.add_argument('-src', '--provider_conf',
                        help='--provider_file',
                        required=True)
    parser.add_argument('-dest', '--customer_conf',
                        help='--customer_file',
                        required=True)

    results = parser.parse_args(args)
    return results.p_database.upper(), results.share_name.upper(), results.schemas.upper(), results.tables.upper(), \
           results.provider_conf.upper(), results.customer_conf.upper()


def create_share(cs, database, schemas, tables, account, share):
    if schemas is not None:
        schema_list = set(schemas.split(","))

    if tables is not None:
        table_list = set(tables.split(","))

    cs.execute("use role accountadmin")
    cs.execute("show shares")
    shares = cs.fetchall()
    for sh in shares:
        if sh[1] == 'OUTBOUND' and sh[2].split(".")[1] == share:
            share_creation_date = sh[0]
            delta = datetime.now() - datetime.strptime(share_creation_date.strftime("%Y-%m-%d"), '%Y-%m-%d')
            if delta.days >= 0:
                destination_environment = sh[4]
                if destination_environment == 'CISCODEV':
                    drop_database_from_share('DEV', share)
                elif destination_environment == 'CISCOSTAGE':
                    drop_database_from_share('STG', share)
                elif destination_environment == 'CISCO':
                    drop_database_from_share('PRD', share)

                cs.execute("drop share %s" % share)
    sql = "use %s" % database
    cs.execute(sql)
    sql = "create share %s" % share
    cs.execute(sql)
    sql = "grant usage on database %s to share %s" % (database, share)
    cs.execute(sql)
    cs.execute("show schemas")
    for schema in cs.fetchall():
        if schemas is not None:
            if not schema[1] in schema_list:
                continue
        try:
            sql = "grant usage on schema %s.%s to share %s" % (database, schema[1], share)
            cs.execute(sql)
            sql = "use schema %s" % (schema[1])
            cs.execute(sql)
        except:
            continue

        cs.execute("show tables")
        for table in cs.fetchall():
            if tables is not None:
                if not table[1] in table_list:
                    continue

            sql = "grant select on %s.%s.%s to share %s" % (database, schema[1], table[1], share)
            cs.execute(sql)

    sql = "alter share %s add accounts=%s" % (share, account)
    cs.execute(sql)


def drop_database_from_share(environment, share):
    snowflake_account = ConfigObject(filename=choose_environment(environment))
    connection = open_database_connection(snowflake_account)
    ctx = connection.cursor()

    ctx.execute("use role accountadmin")
    ctx.execute("show shares like '%s'" % share)
    for s in ctx.fetchall():
        if s[1] == 'INBOUND':
            share_db = s[3]
            ctx.execute("drop database %s" % share_db)


def display_share(cs, share):
    print("Grants to share %s" % share)
    sql = "show grants to share %s" % share
    cs.execute(sql)
    for grant in cs.fetchall():
        print(grant)

    print("%s share:" % share)
    cs.execute("show shares like '%s'" % share)
    for s in cs.fetchall():
        print(s)


def display_database_from_share(cs, database, warehouse):
    sql = "use warehouse %s" % warehouse
    cs.execute(sql)
    sql = "use %s" % database
    cs.execute(sql)
    sql = "select current_warehouse(), current_database(), current_schema()"
    cs.execute(sql)
    print(cs.fetchone())

    cs.execute("show schemas")
    for schema in cs.fetchall():
        print(schema[1])
        sql = "use schema %s" % (schema[1])
        cs.execute(sql)
        cs.execute("show tables")
        for table in cs.fetchall():
            print("    %s" % table[1])
            cs.execute("grant ownership on %s to sysadmin" % table[1])


def create_database_from_share(cs, db, account, share):
    database = db + '_DB'
    cs.execute("use role accountadmin")
    sql = "create or replace database %s from share %s.%s" % (database, account, share)
    cs.execute(sql)
    cs.execute("grant imported privileges on database %s to role edw_datalake_role" % database)
    cs.execute("grant imported privileges on database %s to role edw_bm_role" % database)


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
    p_database, share_name, schemas, tables, provider_conf, customer_conf = check_arg(sys.argv[1:])
    if provider_conf != customer_conf:

        if schemas is None and tables is not None:
            print("Schema is required for table")
            sys.exit(1)

        share = "%s_SHARE" % share_name

        config_provider = ConfigObject(filename=choose_environment(provider_conf))
        ctx_provider = open_database_connection(config_provider)
        cs_provider = ctx_provider.cursor()
        config_customer = ConfigObject(filename=choose_environment(customer_conf))

        ctx_customer = open_database_connection(config_customer)
        cs_customer = ctx_customer.cursor()
        warehouse = str(config_customer.config_properties.warehouse)

        customer_account = str(config_customer.config_properties.account).split(".")[0]
        provider_account = str(config_provider.config_properties.account).split(".")[0]

        def share_function():

            create_share(cs_provider, p_database, schemas, tables, customer_account, share)
            display_share(cs_provider, share)
            create_database_from_share(cs_customer, share, provider_account, share)
            display_database_from_share(cs_customer, share, warehouse)

        try:

            share_function()

        except Exception as e:
            return str(e)

        finally:
            cs_provider.close()
            cs_customer.close()
        ctx_provider.close()
        ctx_customer.close()

    else:
        print("Source Environment and Destination Environmet should not be the same")
        sys.exit(1)


if __name__ == '__main__':
    main()
