#!/apps/tools/anaconda3/bin/python
import argparse
import sys

from ConfigObject import ConfigObject

from util import open_database_connection


def check_arg(args=None):
    parser = argparse.ArgumentParser(description='Parser for creating the snowflake shares')
    parser.add_argument('-p', '-P', '--p_database',
                        help='provider_database',
                        required=True)
    parser.add_argument('-s', '-S', '--schemas',
                        help='schemas')
    parser.add_argument('-r', '-R', '--role_name',
                        help='role_name')
    parser.add_argument('-src', '-SRC', '--provider_conf',
                        help='--source account')
    parser.add_argument('-dest', '-DEST', '--customer_conf',
                        help='--destination account',
                        required=True)

    results = parser.parse_args(args)
    return results.p_database.upper(), results.schemas, results.role_name, results.provider_conf, list(
        map(lambda x: x.upper(), results.customer_conf.split(",")))


def create_share(cs, database, schemas, share):
    cs.execute("use role accountadmin")
    sql = "use %s" % database
    cs.execute(sql)
    sql = 'create or replace share "{}" comment = "Schema Level Share"'.format(share)
    cs.execute(sql)
    sql = 'grant usage on database "{}" to share "{}"'.format(database, share)
    cs.execute(sql)
    cs.execute("show schemas")
    for schema in cs.fetchall():
        if not schema[1] in schemas:
            continue
        try:
            sql = 'grant usage on schema "{}"."{}" to share "{}"'.format(database, schema[1], share)
            cs.execute(sql)
        except Exception as e:
            str(e)

        sql = 'grant select on all tables in schema "{}"."{}" to share "{}"'.format(database, schema[1], share)
        cs.execute(sql)


def create_database_from_share(cs, db, account, share, role_name):
    database = db + '_DB'
    cs.execute("use role accountadmin")
    sql = 'create or replace database "{}" from share {}."{}"'.format(database, account, share)
    cs.execute(sql)
    cs.execute('grant imported privileges on database "{}" to "{}"'.format(database, role_name))


def choose_environment(environment):
    if not isinstance(environment, list):
        if environment == 'DEV':
            return 'config_dev.ini'
        elif environment == 'STG':
            return 'config_stg.ini'
        elif environment == 'PRD':
            return 'config_prd.ini'
        else:
            print("wrong Environment")
            sys.exit(1)

    else:
        return list(map(lambda x: 'config_' + x.lower() + '.ini', environment))


def main():
    p_database, schemas, role_name, provider_conf, customer_conf = check_arg(sys.argv[1:])
    dev_list = ['_DV1', '_DV2', '_DV3', '_DV4']
    stg_list = ['_TS1', '_TS2', '_TS3', '_TS4']
    li = ['_ETL_', '_BR_']

    if role_name is None:
        role_name = 'EDW_DATALAKE_ROLE'
    else:
        role_name = role_name.upper()

    if provider_conf is None:
        if any(p_database.endswith(s) for s in dev_list):
            provider_conf = 'DEV'
        elif any(p_database.endswith(s) for s in stg_list):
            provider_conf = 'STG'
        elif p_database.endswith('_DB'):
            if any(l in p_database for l in li):
                provider_conf = 'PRD'
            else:
                print(
                    '\nUNABLE TO IDENTIFY THE SOURCE ACCOUNT PLEASE PROVIDE THE SOURCE ACCOUNT\n\nLIKE -src PRD OR -src DEV')
                sys.exit(1)

    else:
        provider_conf = provider_conf.upper()

    if provider_conf not in customer_conf:

        config_provider = ConfigObject(filename=choose_environment(provider_conf))
        ctx_provider = open_database_connection(config_provider)
        cs_provider = ctx_provider.cursor()

        if schemas is None:
            cs_provider.execute("use role accountadmin")
            cs_provider.execute("use %s" % p_database)
            cs_provider.execute("show schemas")
            import pdb
            pdb.set_trace()
            for schema in cs_provider.fetchall():
                if 'SS' in schema:
                    schemas = ['SS']
                    break
                elif 'BR' in schema:
                    schemas = ['BR']
                    break
                else:
                    continue
            if schemas is None:
                print('\nUNABLE TO IDENTIFY THE SCHEMA PLEASE PROVIDE THE SCHEMA\n\nLIKE -S SS OR -S BR')
                sys.exit(1)

        else:
            schemas = list(map(lambda x: x.upper(), schemas.split(",")))

        share = "%s_%s_%s_Ê_%s_SHARE" % (p_database, provider_conf, '_'.join(customer_conf), '_'.join(schemas))
        create_share(cs_provider, p_database, schemas, share)

        try:
            for i in range(len(customer_conf)):
                config_customer = ConfigObject(filename=choose_environment(customer_conf[i]))

                ctx_customer = open_database_connection(config_customer)
                cs_customer = ctx_customer.cursor()

                customer_account = str(config_customer.config_properties.account).split(".")[0]
                provider_account = str(config_provider.config_properties.account).split(".")[0]

                def share_function():
                    cs_provider.execute("use role accountadmin")
                    sql = 'alter share "{}" add accounts = {}'.format(share, customer_account)
                    cs_provider.execute(sql)

                    create_database_from_share(cs_customer, share, provider_account, share, role_name)

                share_function()

                cs_customer.close()
                ctx_customer.close()

        except Exception as e:
            return str(e)

        finally:
            cs_provider.close()

        ctx_provider.close()
        print('\nShare Database is "{}"\n'.format(share + '_DB'))

    else:
        print("Source Environment and Destination Environment should not be the same")
        sys.exit(1)


if __name__ == '__main__':
    main()
