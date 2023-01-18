import pytest
from nremc_database_connector import NREMCDatabaseConnector
from pyodbc import Row

@pytest.fixture
def database() -> NREMCDatabaseConnector:
    return NREMCDatabaseConnector.from_toml_config("./commands.toml")

def test_call_one_person(database: NREMCDatabaseConnector) -> None:
    database.call("SELECT_PERSON_BY_ID", 1)
    ret = database.fetch()
    assert isinstance(ret, Row)
    
def test_call_all_people(database: NREMCDatabaseConnector) -> None:
    database.call("SELECT_ALL_PEOPLE")
    ret = database.fetch_all()
    assert isinstance(ret, list) and len(ret) >= 1
    
def test_execute_many_cmd(database: NREMCDatabaseConnector) -> None:
    new_names_and_ages = (("Larsten Courtney", 8, 1), 
                        ("Adam", 7, 2), 
                        ("Eve", 90, 3))
    try:
        database.call_many("UPDATE_AGE_BY_ID", new_names_and_ages)
    except:
        pytest.fail("Database failed to execute call_many method...")
    finally:
        database.rollback()
    
    