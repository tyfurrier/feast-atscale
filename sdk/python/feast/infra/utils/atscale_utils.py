from atscale.atscale import AtScale

from sdk.python.feast import RepoConfig


def get_atscale_api(config: RepoConfig):
    # todo, atscale.check_single_connection possible downfall and figure out if it can be the same for offline online
    # i.e. get rid of this complicated file reading stuff and config_header and just read the config as a dict
    if config.type == "atscale.offline":
        config_header = "connection.feast_offline_store"  # honestly idk what this does - Ty

    config = dict(config)  # turn into dict
    project_name = config['project']

    # read config file
    config_reader = configparser.ConfigParser()
    config_reader.read([config["config_path"]])  # read actual config file
    if config_reader.has_section(config_header):
        kwargs = dict(config_reader[config_header])
    else:
        kwargs = {}

    kwargs.update((k, v) for k, v in config.items())

    try:
        atproject = atscale.atscale.AtScale(
            server=kwargs["server"],
            organization=kwargs["organization"],
            project_id=kwargs["project_id"],
            model_id=kwargs["model_id"],
            token=kwargs["token"],
            username=kwargs["username"],
            password=kwargs["password"],
            design_center_server_port=kwargs["design_center_server_port"],
            engine_port=kwargs["engine_port"],
        )
    except UserError as e:
        raise FeastProviderLoginError(str(e))

    connection_reqs: dict[str, list[str]] = {
        'redshift': ['username', 'password',
                     'host', 'port', 'database',
                     'schema'],
        'iris': ['user', 'host', 'namespace', 'driver',
                 'port', 'schema', 'password'],
        'databricks': ['token', 'host'],
        'bigquery': ['credentials_path', 'project', 'dataset'],
        'snowflake': ['username', 'account', 'warehouse',
                      'database', 'schema', 'password'],
        'synapse': ['username', 'host', 'database', 'schema',
                    'driver', 'port', 'password']
    }

    def password():
        if config['password'] is None:
            return getpass.getpass(f'{conn_id} password for user: {config["username"]}')
        else:
            return config['password']
        # doesn't write newfound password to file

    conn_id = config['connection_id']
    if conn_id is None:
        raise UserError("You must create a db connection,"
                        " or pass its credentials to the feast config,"
                        " to use an AtScale project as a feast project.")
    else:
        for param in connection_reqs[conn_id]:
            if config[param] is None:
                raise UserError(f'Incomplete config for {conn_id}'
                                f' connection, missing {param}')
        # todo: put password in constructors in db.Iris and others
        constructors = {
            'redshift': atscale.db.Redshift(atscale_connection_id=f'feast-{project_name}',
                                            username=kwargs['username'],
                                            password=password(),
                                            host=kwargs['host'],
                                            database_name=kwargs['database'],
                                            schema=kwargs['schema'],
                                            port=kwargs['port']),
            'iris': atscale.db.Iris(atscale_connection_id=f'feast-{project_name}',
                                    username=kwargs['username'],
                                    host=kwargs['host'],
                                    namespace=kwargs['namespace'],
                                    driver=kwargs['driver'],
                                    schema=kwargs['schema'],
                                    port=kwargs['port']
                                    ),
            'databricks': atscale.db.Databricks(atscale_connection_id=f'feast-{project_name}',
                                                token=kwargs['token'],
                                                host=kwargs['host'],
                                                database=kwargs['database'],
                                                http_path=kwargs['http_path'],
                                                driver=kwargs['driver'],
                                                port=kwargs['port']),
            'synapse': atscale.db.Synapse(atscale_connection_id=f'feast-{project_name}',
                                          username=kwargs['username'],
                                          host=kwargs['host'],
                                          database=kwargs['database'],
                                          driver=kwargs['driver'],
                                          schema=kwargs['schema'],
                                          port=kwargs['port']),
            'bigquery': atscale.db.BigQuery(atscale_connection_id=f'feast-{project_name}',
                                            credentials_path=kwargs['credentials_path'],
                                            project=kwargs['project'],
                                            dataset=kwargs['dataset']),
            'snowflake': atscale.db.Snowflake(atscale_connection_id=f'feast-{project_name}',
                                              username=kwargs['username'],
                                              password=password(),
                                              account=kwargs['account'],
                                              warehouse=kwargs['warehouse'],
                                              database=kwargs['database'],
                                              schema=kwargs['schema'])
        }
        print('Does not instantiate in dict')
        # todo check for redshift params
        atproject.create_db_connection(constructors[kwargs['connection_id']])
    return atproject