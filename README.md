# Wikimap - Visualize the connections between ideas
[![Sample Map](static/map.gif "Sample Wikimap")](https://dayofthepenguin.github.io/wikimap/interactive_demo.html)

## Examples:
[Interactive Map](https://dayofthepenguin.github.io/wikimap/interactive_demo.html)

[Orbiting Map](https://dayofthepenguin.github.io/wikimap/orbiting_demo.html)

## Installation:

### Clone this repository

`git clone https://github.com/DayOfThePenguin/wikimap wikimap`

### Install the Cairo 2d graphics library (if not already on your system)
igraph's native plotting depends on [pycairo](https://www.cairographics.org/pycairo/), and
in order to install pycairo, you need to have the cairo C library installed.  See
[Cairo's downloads page](https://www.cairographics.org/download/) for install information

### Install [Virtualenv](https://virtualenv.pypa.io/en/latest/), create a new environment, and activate it
This module supports python 3.8+
```shell
> pip install virtualenv
> cd wikimap
> virtualenv env
> source env/bin/activate
[env] > 
```

### Use pip to install wikimap's dependencies
```shell
[env] > pip install -r requirements.txt
```

### Add custom maps based on wikipedia pages of your choice - Open the `3d_json_generator` notebook
Run the first 2 cells, create a `WikipediaMap` object with your preferred wikipedia page, set how many
levels of links you want to go out, and pick a number of links to get per page (please be considerate of
Wikipedia's servers and don't set these to more than 3 levels and 20-30 links per page)


### Run uvicorn and open the server in a web browser
```shell
[env] > uvicorn main:app --reload
```
You should see a list of all the `.json` files in static/data. Select one to display its map. There are a few test files in there for you to experiment with.

## Create your local Wikipedia Database
Install Docker, Docker-compose

run
```shell
docker-compose up
```
in this directory

connect to the postgres database and create a `wikipedia` database
```shell
psql -h 127.0.0.1 -U postgres
postgres=\# CREATE DATABASE wikipedia;
```

## Notes
In order to run the tests, you need to have a copy of
`enwiki-20210420-pages-articles-multistream16.xml-p20460153p20570392.bz2` downloaded.
You can get this from https://ftp.acc.umu.se/mirror/wikimedia.org/dumps/enwiki/

Coding Style - [black](https://black.readthedocs.io/en/stable/)
Docstring Format - [numpydoc](https://numpydoc.readthedocs.io/en/latest/example.html#example)

## Common issues
1. Q: Module imports aren't working (some form of `ModuleNotFoundError: No module named 'wikimap'`)

A: The vscode workspace file in this repo implicitly sets the PYTHONPATH environment variable
(see the `"env"` dict in `.vscode/wikimap.code-workspace`). If you're not using vscode, set the
PYTHONPATH environment variable in your shell to the path to this repo.

## Future work
TODO: when adding functionality to work with a local wikipedia download (and adding parallel access to that...we
stick with a single thread/process when querying the actual wikipedia site to be respectful.), add a way (tf-idf for now?)
to calculate which pages we want to actually list as links (instead of arbitrarily picking the first x of the alphabet to make the visualizations tractable).
Goal: to apply some statistical comparisons to pages and if the similarity of the content is above a certain threshold, display a link
between the pages (filter all 600+ links of a page down to the smaller subset that we can actually display on screen at once)
Alternative: modify the 3d-graph-vis library so it doesn't calculate layouts in the browser. So you can calculate it locally
and then just plot the results in the browser. This would increase the total number of vertices you could display in the browser at
once

FEATURES: add a filter/legend that shows what color corresponds to which level and allow users to  so you can