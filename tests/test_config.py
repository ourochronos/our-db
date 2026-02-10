"""Tests for our_db.config."""

import os
from unittest.mock import patch

from our_db.config import CoreSettings, clear_config_cache, get_config, set_config


class TestCoreSettings:
    def test_default_values(self):
        settings = CoreSettings()
        assert settings.db_host == "localhost"
        assert settings.db_port == 5432
        assert settings.db_name == "postgres"
        assert settings.db_user == "postgres"
        assert settings.db_password == ""
        assert settings.db_pool_min == 5
        assert settings.db_pool_max == 20
        assert settings.log_level == "INFO"
        assert settings.log_format == ""
        assert settings.log_file is None

    def test_env_vars(self):
        env = {
            "ORO_DB_HOST": "myhost",
            "ORO_DB_PORT": "5433",
            "ORO_DB_NAME": "mydb",
            "ORO_DB_USER": "myuser",
            "ORO_DB_PASSWORD": "secret",
            "ORO_DB_POOL_MIN": "2",
            "ORO_DB_POOL_MAX": "10",
            "ORO_LOG_LEVEL": "DEBUG",
        }
        with patch.dict(os.environ, env, clear=False):
            settings = CoreSettings()
            assert settings.db_host == "myhost"
            assert settings.db_port == 5433
            assert settings.db_name == "mydb"
            assert settings.db_user == "myuser"
            assert settings.db_password == "secret"
            assert settings.db_pool_min == 2
            assert settings.db_pool_max == 10
            assert settings.log_level == "DEBUG"

    def test_database_url(self):
        settings = CoreSettings()
        assert settings.database_url == "postgresql://postgres:@localhost:5432/postgres"

    def test_connection_params(self):
        settings = CoreSettings()
        params = settings.connection_params
        assert params["host"] == "localhost"
        assert params["port"] == 5432
        assert params["dbname"] == "postgres"
        assert params["user"] == "postgres"
        assert params["password"] == ""

    def test_pool_config(self):
        settings = CoreSettings()
        pool = settings.pool_config
        assert pool == {"minconn": 5, "maxconn": 20}


class TestGlobalConfig:
    def test_get_config_returns_singleton(self):
        config1 = get_config()
        config2 = get_config()
        assert config1 is config2

    def test_clear_config_cache(self):
        config1 = get_config()
        clear_config_cache()
        config2 = get_config()
        assert config1 is not config2

    def test_set_config(self):
        custom = CoreSettings(db_host="custom-host")
        set_config(custom)
        assert get_config() is custom
        assert get_config().db_host == "custom-host"
