******
loadit
******

A blazing fast database for FEM loads.

.. image:: https://readthedocs.org/projects/loadit/badge/?version=latest
   :target: https://loadit.readthedocs.io/en/latest/?badge=latest
   :alt: Documentation Status

Requirements
============

* python 3.6 (or later)
* numpy
* pyarrow
* pandas
* cryptography
* pyjwt

Installation
============

Run the following command::

    pip install loadit


Using local databases
=====================

Create a new database::

    import loadit


    files = ['/Users/Alvaro/FEM_results/file01.pch', '/Users/Alvaro/FEM_results/file02.pch']
    database_path = '/Users/Alvaro/databases/FooDatabase'
    database_name = 'Foo database'
    database_version = '0.0.1'

    database = loadit.Database()
    database.create(files, database_path, database_name, database_version)

Load an existing database::

    database = loadit.DataBase(database_path)

Check database integrity::

    database.check()

Display database info::

    database.header.info()

Perform a query::

    dataframe = database.query_from_file(query_file)

Append new result files to an existing database (this action is reversible)::

    files = ['/Users/Alvaro/FEM_results/file03.pch', '/Users/Alvaro/FEM_results/file04.pch']
    batch_name = 'new_batch'
    database.append(files, batch_name)

Restore database to a previous state (this action is NOT reversible!)::

    database.restore('Initial batch')


Using remote databases
======================

Open a new client interfacing the cluster (you will be asked to login)::

    import loadit


    client = loadit.Client(('192.168.0.154', 8080))

Load a database::

    client.load('FooDatabase')

Display database info::

    client.database.header.info()

Check database integrity::

    client.database.check()

Perform a query::

    dataframe = client.database.query_from_file(query_file)

Append new result files to an existing database (this action is reversible)::

    files = ['/Users/Alvaro/FEM_results/file03.pch', '/Users/Alvaro/FEM_results/file04.pch']
    batch_name = 'new_batch'
    client.database.append(files, batch_name)

Restore database to a previous state (this action is NOT reversible!)::

    client.database.restore('Initial batch')

Display cluster info::

    client.info()

List cluster sessions::

    client.sessions()

Add a new session::

    client.add_session('jimmy_mcnulty', 'Im_the_boss', is_admin=True)

Remove a session::

    client.remove_session('jimmy_mcnulty')

Create a new database::

    files = ['/Users/Alvaro/FEM_results/file01.pch', '/Users/Alvaro/FEM_results/file02.pch']
    database_path = 'FooDatabase'
    database_name = 'Foo database'
    database_version = '0.0.1'

    client.create_database(files, database_path, database_name, database_version)

Remove a database::

    client.remove_database('FooDatabase')

Sync databases between cluster nodes::

    client.sync_databases()

Shutdown the cluster::

    client.shutdown()


Contact
=======
Álvaro Sanz Oriz – alvaro.sanz.oriz@gmail.com
