
import psycopg2
from psycopg2 import extras,sql
import os
import copy
import logging
import csv
import hashlib
import numpy as np
import inspect

logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.setLevel(logging.INFO)

try:
	import psycopg2
	from psycopg2 import extras
	from psycopg2.extensions import register_adapter, AsIs
	register_adapter(np.float64, AsIs)
	register_adapter(np.int64, AsIs)
except ImportError:
	logger.info('Psycopg2 not installed, pip install psycopg2 (or binary-psycopg2) if you want to use a PostgreSQL DB')



def split_sql_init(script):
	lines = script.split('\n')
	formatted = '\n'.join([l for l in lines if l[:2]!='--'])
	return formatted.split(';')[:-1]

class Database(object):
	"""
	This class creates a database object with the main structure, with a few methods  to manipulate it.
	To fill it, fillers are used (see Filler class).
	The object uses a specific data folder and a list of files used for the fillers, with name, keyword, and potential download link. (move to filler class?)
	"""
	tables_whitelist = ['spatial_ref_sys']
	def __init__(self,pre_initscript='',post_initscript='',data_folder='datafolder',register_exec=False,db_schema=None,additional_searchpath=['postgis'],DB_INIT=None,fallback_db='postgres',**db_conninfo):
		self.logger = logger
		self.db_conninfo = copy.deepcopy(db_conninfo) # db_conninfo can be partly defined in ~/.pgpass, especially for passwords. See postgres doc for more info.

		if DB_INIT is None:
			init_sql_file = os.path.join(os.path.dirname(inspect.getfile(self.__class__)),'initscript.sql')
			if not os.path.exists(init_sql_file):
				raise IOError(f'Missing file: {init_sql_file}')
			with open(init_sql_file,'r') as f:
				self.DB_INIT = f.read()
		else:
			self.DB_INIT = DB_INIT

		# Schemas order : [db_schema if not None] + [options if provided or orig_searchpath(default or DB specific) ]+ additional_searchpath
		if (db_schema is not None or additional_searchpath is not None):
			
			if db_schema is None:
				# db_schema = 'public'
				searchpath = []
			else:
				searchpath = [db_schema]
			
			other_options = None

			if additional_searchpath is None:
				additional_searchpath = []
			if 'options' in self.db_conninfo.keys():
				if not self.db_conninfo['options'].replace(' ','').startswith('-csearch_path'):
					raise SyntaxError(f'''postgres connection options with unsupported format (only search path implemented in db_fillers): {self.db_conninfo['options']}''') 
				self.logger.info(f'''Merging searchpath info from "options" parameter ({db_conninfo['options']}) and db_schema ({db_schema}) + additional_searchpath ({additional_searchpath})''')
				# raise SyntaxError('You provided a schema and/or a search_path while also providing the "options" argument in the connection info string, resolving potential conflicts there is not implemented.')
				orig_options = self.db_conninfo['options']
				other_options = None
				opt = orig_options
				l = len('-c search_path=')
				if opt.startswith('-c '):
					opt = opt[l:]
				else:
					opt = opt[l-1:]
				searchpath_options = opt.split(',') 
			else:
				searchpath_options = []

			if len(searchpath_options) == 0:
				temp_conninfo = copy.deepcopy(db_conninfo)
				temp_conninfo.update(dict(data_folder=data_folder,db_schema=None,additional_searchpath=None,))
				try:
					temp_db = self.__class__(**temp_conninfo)
					temp_db.cursor.execute('''SELECT UNNEST(STRING_TO_ARRAY(CURRENT_SETTING('search_path'),', '));''')
					orig_searchpath = [r[0] for r in temp_db.cursor.fetchall()]
					temp_db.connection.close()

				except psycopg2.OperationalError as e:
					temp_conninfo.update(dict(data_folder=data_folder,db_schema=None,additional_searchpath=None,database=fallback_db))
					temp_db = self.__class__(**temp_conninfo)
					
					temp_db.cursor.execute('''SELECT UNNEST(STRING_TO_ARRAY(boot_val,', ')) FROM pg_settings WHERE name='search_path';''')
					orig_searchpath = [r[0] for r in temp_db.cursor.fetchall()]
					
					temp_db.connection.close()
			
				searchpath += orig_searchpath
			else:
				searchpath += searchpath_options

			searchpath += copy.deepcopy(additional_searchpath)

			searchpath = [s.replace('"','') for s in searchpath]

			for s in searchpath:
				for e in ("'",';',','):
					if e in s:
						raise ValueError('db_schema {} contains illegal char: {}'.format(s,e))


			temp_searchpath = []
			for s in searchpath:
				if s not in temp_searchpath:
					temp_searchpath.append(s)
			searchpath = temp_searchpath
			
			self.db_conninfo['options'] = '-c search_path='+','.join(['"{}"'.format(s.replace('"','')) for s in searchpath])
			
			if other_options is not None:
				self.db_conninfo['options'] += ' '+other_options
			
			

		if 'password' in self.db_conninfo.keys():
			logger.warning('You are providing your password directly, this could be a security concern, consider using solutions like .pgpass file.')
		try:
			self.connection = psycopg2.connect(**self.db_conninfo)
		except psycopg2.OperationalError as e:
			if 'database "{}" does not exist\n'.format(db_conninfo['database']) in str(e):
				pgpass_env = 'PGPASSFILE'
				default_pgpass = os.path.join(os.environ['HOME'],'.pgpass')
				if pgpass_env not in os.environ.keys():
					os.environ[pgpass_env] = default_pgpass
				conninfo_nodb = copy.deepcopy(self.db_conninfo)
				conninfo_nodb.update(dict(database=fallback_db))
				# conninfo_nodb.update(dict(database=fallback_db,additional_searchpath=None,db_schema=None))
				self.logger.warning('Database {} does not exist: trying to create it via connecting primarily to database {}'.format(db_conninfo['database'],conninfo_nodb['database']))
				conn = psycopg2.connect(**conninfo_nodb)
				conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
				cur = conn.cursor()
				cur.execute(psycopg2.sql.SQL(
					"CREATE DATABASE {};"
					).format(psycopg2.sql.Identifier(self.db_conninfo['database'])))
				cur.close()
				conn.close()
				self.connection = psycopg2.connect(**self.db_conninfo)
			else:
				pgpass_env = 'PGPASSFILE'
				default_pgpass = os.path.join(os.environ['HOME'],'.pgpass')
				if pgpass_env not in os.environ.keys():
					os.environ[pgpass_env] = default_pgpass
					self.logger.info('Password authentication failed,trying to set .pgpass env variable')
					self.connection = psycopg2.connect(**self.db_conninfo)
				else:
					raise
		self.cursor = self.connection.cursor()
		if db_schema is not None:
			self.cursor.execute('SELECT schema_name FROM information_schema.schemata;')
			schemas = [s for s in self.cursor.fetchall()]
			if db_schema not in schemas:
				self.check_sqlname_safe(db_schema)
				self.cursor.execute('CREATE SCHEMA IF NOT EXISTS "{}";'.format(db_schema))
				self.connection.commit()

		self.register_exec = register_exec

		self.fillers = []
		self.data_folder = data_folder
		if not os.path.exists(self.data_folder):
			os.makedirs(self.data_folder)
		self.pre_initscript = pre_initscript
		self.post_initscript = post_initscript

	def clean_db(self,commit=True,extra_whitelist=[],**kwargs):
		self.logger.info('Cleaning DB')
		tables = [t for t in self.get_tables() if t not in self.tables_whitelist+extra_whitelist]

		for t in tables:
			self.check_sqlname_safe(t)
			self.cursor.execute(f'DROP TABLE IF EXISTS {t} CASCADE;')
		if commit:
			self.connection.commit()

	def get_tables(self):
		self.cursor.execute(
			'''SELECT table_name FROM information_schema.tables
			where table_schema=CURRENT_SCHEMA AND table_type='BASE TABLE'; ''')
		return [t[0] for t in self.cursor.fetchall()]

	def init_db(self):
		# for cmd in split_sql_init(self.DB_INIT)+split_sql_init(self.pre_initscript)+split_sql_init(self.post_initscript):
		for cmd in (self.pre_initscript,self.DB_INIT,self.post_initscript):
			if cmd != '' and cmd is not None:
				self.logger.debug(cmd)
				self.cursor.execute(cmd)
		if self.register_exec:
			self.register_exec_content()
		self.connection.commit()


	def fill_db(self):
		self.register_filler_content(filler_class='fill_db',filler_args=None,status='start_fill_db')
		for f in self.fillers:
			if not f.done:
				self.register_filler_content(filler_class=f.__class__.__name__,filler_args=f.get_relevant_attr_string(),status='init_prepare')
				f.prepare()
				self.logger.info('Prepared filler {}'.format(f.name))
				self.register_filler_content(filler_class=f.__class__.__name__,filler_args=f.get_relevant_attr_string(),status='end_prepare')
		# for f in self.fillers:
				if not f.done:
					if not f.check_requirements():
						raise Exception(f'Requirements not fulfilled for filler: {f.name}')
					else:
						self.register_filler_content(filler_class=f.__class__.__name__,filler_args=f.get_relevant_attr_string(),status='init_apply')
						f.apply()
						f.done = True
						self.register_filler_content(filler_class=f.__class__.__name__,filler_args=f.get_relevant_attr_string(),status='end_apply')
			self.logger.info('Filled with filler {}'.format(f.name))
		self.register_filler_content(filler_class='fill_db',filler_args=None,status='end_fill_db')

	def add_filler(self,f):
		if f.name in [ff.name for ff in self.fillers if ff.unique_name]:
			self.logger.warning('Filler {} already present'.format(f.name))
		else:
			f.db = self
			self.fillers.append(f)
			f.logger = self.logger
			f.after_insert()
			self.logger.info('Added filler {}'.format(f.name))

	def check_empty(self,table):
		self.cursor.execute('SELECT * FROM {table} LIMIT 1;'.format(table=table))
		ans = self.cursor.fetchone()
		return (ans is None)

	@classmethod
	def check_sqlname_safe(cls,s):
		assert s == ''.join( c for c in s if c.isalnum() or c in ('_',) ), '{} is not passing the check against SQL injection'.format(s)

########### files management
	def record_file(self,filename,filecode,folder=None):
		self.cursor.execute('''CREATE TABLE IF NOT EXISTS file_hash(
								filecode TEXT PRIMARY KEY,
								filename TEXT,
								hashtype TEXT DEFAULT 'SHA256',
								updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
								filehash TEXT NOT NULL
								);''')
		self.connection.commit()
		if folder is None:
			folder = self.data_folder
		with open(os.path.join(folder,filename),"rb") as f:
			filehash = hashlib.sha256(f.read()).hexdigest()
		self.cursor.execute('INSERT INTO file_hash(filecode,filename,filehash) VALUES(%s,%s,%s) ON CONFLICT (filecode) DO UPDATE SET filecode=EXCLUDED.filecode,filename=EXCLUDED.filename,filehash=EXCLUDED.filehash;',(filecode,filename,filehash))

	def register_exec_content(self):
		try:
			import __main__
		except ImportError:
			self.logger.warning('Trying to log exec script, but cannot import __main__, execution is not a typical python script execution, skipping')
		else:
			with open(__main__.__file__,'r') as f:
				exec_content = f.read()
			with open(__main__.__file__,"rb") as f:
				exec_hash = hashlib.sha256(f.read()).hexdigest()
			if 'password' in exec_content.lower():
				raise ValueError('Password should not be provided in exec file, especially if content is registered!')
			else:
				self.cursor.execute('''
					INSERT INTO _exec_info(content,content_hash)
					VALUES (%s,%s);
					''',(exec_content,exec_hash))
				self.connection.commit()

	def register_filler_content(self,filler_class,filler_args,status):
		self.cursor.execute('''
				INSERT INTO _fillers_info(class,args,status)
				VALUES (%s,%s,%s);
				''',(filler_class,filler_args,status))
		self.connection.commit()
