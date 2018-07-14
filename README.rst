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
* pandas
* numba
* pyarrow
* cryptography
* pyjwt
* wxpython


Installation
============

Run the following command::

    pip install loadit


Usage example
=============

Launch the database manager::

    client = loadit.Client()

Create a new database::

    database = client.create_database('example_local_database')

Load an existing database::

    database = client.load_database('example_local_database')

Connect to a remote server (you will be asked to login)::

    client.connect('192.168.0.154:8080')

Create a new remote database::

    database = client.create_remote_database('example_remote_database')

Load an existing remote database::

    database = client.load_remote_database('example_remote_database')

Database management
-------------------

Check database integrity::

    database.check()

Display database info::

    database.header.info()

Perform a query::

    dataframe = database.query_from_file(query_file)

Append new result files to an existing database (this action is reversible)::

    files = ['/Users/Alvaro/FEM_results/file03.pch', '/Users/Alvaro/FEM_results/file04.pch']
    batch_name = 'new_batch'
    database.new_batch(files, batch_name)

Restore database to a previous state (this action is NOT reversible!)::

    database.restore('Initial batch')

Add an attachment::

    database.add_attachment('/Users/Alvaro/Desktop/nastran_model_input/BulkData.zip')

Download an attachment::

    database.download_attachment('BulkData.zip', '/Users/Alvaro/Desktop/nastran_model_input/BulkData.zip'')

Remove an attachment::

    database.remove_attachment('BulkData.zip')

Cluster management
------------------

Show remote cluster info::

    client.info()

Sync databases between cluster nodes::

    client.sync_databases()

Shutdown the cluster::

    client.shutdown()


Contact
=======

Alvaro Sanz Oriz – alvaro.sanz.oriz@gmail.com
