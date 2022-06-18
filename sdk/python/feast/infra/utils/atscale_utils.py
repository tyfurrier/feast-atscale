import configparser
from getpass import getpass

from atscale.atscale import AtScale
from atscale import BigQuery, Databricks, Iris, Redshift, Snowflake, Synapse
from atscale.errors import UserError

from feast.errors import FeastProviderLoginError


def get_atscale_api(config):
    # todo, atscale.check_single_connection possible downfall and figure out if it can be the same for offline online
    # i.e. get rid of this complicated file reading stuff and config_header and just read the config as a dict
    from feast.infra.offline_stores.atscale_offline_store import AtScaleOfflineStoreConfig
    assert isinstance(config, AtScaleOfflineStoreConfig)
    if config.type == "atscale.offline":
        config_header = (
            "connection.feast_offline_store"  # honestly idk what this does - Ty
        )

    config = dict(config)  # turn into dict

    if config_header in config:
        kwargs = dict(config[config_header])
    else:
        kwargs = config

    print("printing", str(kwargs))

    try:
        atproject = AtScale(
            server=kwargs["server"],
            organization=kwargs["organization"],
            project_id=kwargs["project_id"],
            model_id=kwargs["model_id"],
            username=kwargs["username"],
            password=kwargs["password"],
            design_center_server_port=kwargs["design_center_server_port"],
            engine_port=kwargs["engine_port"],
        )
    except UserError as e:
        raise FeastProviderLoginError(str(e))

    connection_reqs: dict[str, list[str]] = {
        "redshift": [
            "db_username",
            "db_password",
            "db_host",
            "db_port",
            "db_database",
            "db_schema",
        ],
        "iris": [
            "db_username",
            "db_host",
            "db_namespace",
            "db_driver",
            "db_port",
            "db_schema",
            "db_password",
        ],
        "databricks": ["db_token", "db_host"],
        "bigquery": ["db_credentials_path", "db_project", "db_dataset"],
        "snowflake": [
            "db_username",
            "db_account",
            "db_warehouse",
            "db_database",
            "db_schema",
            "db_password",
        ],
        "synapse": [
            "db_username",
            "db_host",
            "db_database",
            "db_schema",
            "db_driver",
            "db_port",
            "db_password",
        ],
    }

    def password():
        if config["db_password"] is None:
            return getpass(
                f'{config["db_type"]} password for user: {config["db_username"]}'
            )
        else:
            return config["db_password"]
        # doesn't write newfound password to file

    db_type = config["db_type"]
    if db_type is None:
        raise UserError(
            "You must create a db connection,"
            " or pass its credentials to the feast config,"
            " to use an AtScale project as a feast project."
        )
    else:
        for param in connection_reqs[db_type]:
            if config[param] is None and param is not "db_password":
                raise UserError(
                    f"Incomplete config for {db_type}" f" connection, missing {param}"
                )
        # todo: put password in constructors in db.Iris and others

        if kwargs["db_type"] == "redshift":
            atproject.create_db_connection(
                db=Redshift(
                    atscale_connection_id=kwargs["db_connection_id"],
                    username=kwargs["db_username"],
                    password=password(),
                    host=kwargs["db_host"],
                    database_name=kwargs["db_database"],
                    schema=kwargs["db_schema"],
                    port=kwargs["db_port"],
                )
            )
        elif kwargs["db_type"] == "iris":
            atproject.create_db_connection(
                db=Iris(
                    atscale_connection_id=kwargs["db_connection_id"],
                    username=kwargs["db_username"],
                    host=kwargs["db_host"],
                    namespace=kwargs["db_namespace"],
                    driver=kwargs["db_driver"],
                    schema=kwargs["db_schema"],
                    port=kwargs["db_port"],
                )
            )
        elif kwargs["db_type"] == "databricks":
            atproject.create_db_connection(
                db=Databricks(
                    atscale_connection_id=kwargs["db_connection_id"],
                    token=kwargs["db_token"],
                    host=kwargs["db_host"],
                    database=kwargs["db_database"],
                    http_path=kwargs["db_http_path"],
                    driver=kwargs["db_driver"],
                    port=kwargs["db_port"],
                )
            )
        elif kwargs["db_type"] == "synapse":
            atproject.create_db_connection(
                db=Synapse(
                    atscale_connection_id=kwargs["db_connection_id"],
                    username=kwargs["db_username"],
                    host=kwargs["db_host"],
                    database=kwargs["db_database"],
                    driver=kwargs["db_driver"],
                    schema=kwargs["db_schema"],
                    port=kwargs["db_port"],
                )
            )
        elif kwargs["db_type"] == "bigquery":
            atproject.create_db_connection(
                db=BigQuery(
                    atscale_connection_id=kwargs["db_connection_id"],
                    credentials_path=kwargs["db_credentials_path"],
                    project=kwargs["db_project"],
                    dataset=kwargs["db_dataset"],
                )
            )
        elif kwargs["db_type"] == "snowflake":
            atproject.create_db_connection(
                db=Snowflake(
                    atscale_connection_id=kwargs["db_connection_id"],
                    username=kwargs["db_username"],
                    password=password(),
                    account=kwargs["db_account"],
                    warehouse=kwargs["db_warehouse"],
                    database=kwargs["db_database"],
                    schema=kwargs["db_schema"],
                )
            )
        else:
            raise Exception(f'Uknown db_type: {kwargs["db_type"]}')
    return atproject
