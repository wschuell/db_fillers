import pytest
import os
import glob

import db_fillers as dbf
from db_fillers import fillers
from db_fillers import Database

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

def test_schema():
	db = Database(db_schema='test_schema',**conninfo)
	db.init_db()
	db.connection.close()

def test_searchpath():
	db = Database(db_schema='test_schema',additional_searchpath=['public'],**conninfo)
	db.init_db()
	db.connection.close()

def test_clone(tmpdir):
	f = fillers.Filler(data_folder=tmpdir)
	f.clone_repo(repo_url='https://github.com/wschuell/gis_fillers')
	f.clone_repo(repo_url='https://github.com/wschuell/gis_fillers',update=False)
	f.clone_repo(repo_url='https://github.com/wschuell/gis_fillers',update=True)
	f.clone_repo(repo_url='https://github.com/wschuell/gis_fillers',replace=True)

def test_download(tmpdir):
	f = fillers.Filler(data_folder=tmpdir)
	f.download(url='https://github.githubassets.com/images/modules/logos_page/GitHub-Logo.png',destination='githublogo.png')
	assert os.path.exists(os.path.join(f.data_folder,'githublogo.png'))
	