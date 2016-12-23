# ReThink

## Setup steps:

### Clone this repository

### Install [Virtualenv](https://virtualenv.pypa.io/en/latest/), create a new environment, and activate it
1. The `.gitignore` file for this repository assumes that you call your virtual environment `env`.  You can change this behavior by editing the `.gitignore` file and adding the name you chose for your environment.

> Because we don't want to have all the binary files from the virtual environment in our git repository

### Use pip to install ReThink's dependencies
* `pip install flask` [Flask](http://flask.pocoo.org/) is the library we use for web back-end things that are hard to do by hand
* `pip install python-igraph` [Igraph](http://igraph.org/python/) is the library we use to do graph operations that would be really hard to do with lists or other standard Python objects
* 'pip install wikipedia' [Wikipedia](https://pypi.python.org/pypi/wikipedia)
is the library we use to make requests for data from wikipedia

### Source the file that sets up the environment variables needed to run the app properly

    source set-up

### Run the app on a local testing server

     flask run
