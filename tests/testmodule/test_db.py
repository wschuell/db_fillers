import pytest
import os
import glob

import syri_db as sdb
from syri_db import Database
from syri_db.fillers import nodes,systemic_risk,zones,generic
# from syri_db.fillers import jrclive
from syri_db.views import views_script

conninfo = {
	'host':'localhost',
	'port':5432,
	'database':'test__db_fillers',
	'user':'postgres',
	'data_folder': os.path.dirname(os.path.dirname(__file__))
}


def test_connect():
	db = Database(**conninfo)

def test_init():
	db = Database(**conninfo)
	db.init_db()

def test_clean():
	db = Database(**conninfo)
	db.clean_db()
	db.init_db()

@pytest.fixture
def maindb():
	db = Database(**conninfo)
	db.init_db()
	yield db
	db.connection.close()

def test_filler(maindb,tmpdir):
	maindb.add_filler(fillers.TestFiller(data_folder=tmpdir))
	maindb.fill_db()

