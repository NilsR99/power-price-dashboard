from unittest.mock import patch, MagicMock

import pytest

from dashboard import data_loader


@patch("dashboard.data_loader.create_engine")
@patch("dashboard.data_loader.st.secrets", {
    "POSTGRES_USER": "cloud_user",
    "POSTGRES_PASSWORD": "p@ss/word",
    "POSTGRES_HOST": "cloud-host",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": "cloud_db",
})
def test_get_engine_uses_streamlit_secrets_and_pooling(mock_create_engine):
    data_loader.get_engine.clear()
    mock_create_engine.return_value = MagicMock()

    data_loader.get_engine()

    args, kwargs = mock_create_engine.call_args
    assert "p%40ss%2Fword" in args[0]
    assert args[0].endswith("?sslmode=require")
    assert kwargs["pool_size"] == 2
    assert kwargs["max_overflow"] == 1
    assert kwargs["pool_pre_ping"] is True


@patch("dashboard.data_loader.create_engine")
@patch("dashboard.data_loader.st.secrets", {})
@patch("dashboard.data_loader.os.getenv")
def test_get_engine_falls_back_to_env(mock_getenv, mock_create_engine):
    data_loader.get_engine.clear()

    env = {
        "POSTGRES_USER": "local_user",
        "POSTGRES_PASSWORD": "local_pw",
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DB": "local_db",
    }
    mock_getenv.side_effect = lambda key, default="": env.get(key, default)
    mock_create_engine.return_value = MagicMock()

    data_loader.get_engine()

    connection_string = mock_create_engine.call_args[0][0]
    assert "local_user:local_pw@localhost:5432/local_db" in connection_string


@patch("dashboard.data_loader.st.secrets", {})
@patch("dashboard.data_loader.os.getenv", return_value="")
def test_get_engine_raises_on_missing_credentials(_mock_getenv):
    data_loader.get_engine.clear()

    with pytest.raises(ValueError, match="Unvollständige DB-Konfiguration"):
        data_loader.get_engine()
