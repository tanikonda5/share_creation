import snowflake.connector
import hvac
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


def open_database_connection(config):
    # Connect to Keeper to collect secrets
    client = hvac.Client(
        url=config.config_properties.keeper_uri,
        namespace=config.config_properties.keeper_namespace,
        token=config.config_properties.keeper_token
    )
    # Secrets are stored within the key entitled 'data'
    keeper_secrets = client.read(config.config_properties.secret_path)['data']
    passphrase = keeper_secrets['SNOWSQL_PRIVATE_KEY_PASSPHRASE']
    private_key = keeper_secrets['private_key']

    # PEM key must be byte encoded
    key = bytes(private_key, 'utf-8')

    p_key = serialization.load_pem_private_key(
        key
        , password=passphrase.encode()
        , backend=default_backend()
    )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER
        , format=serialization.PrivateFormat.PKCS8
        , encryption_algorithm=serialization.NoEncryption())

    conn = snowflake.connector.connect(
        user=config.config_properties.user
        , account=config.config_properties.account
        , warehouse=config.config_properties.warehouse
        , role=config.config_properties.role
        , database=config.config_properties.database
        , schema=config.config_properties.schema
        , timezone=config.config_properties.timezone
        # , password=config.config_properties.password
        , private_key=pkb
    )
    return conn
