# Wikimap

## First-time setup steps:

### Clone this repository

`git clone https://github.com/DayOfThePenguin/wikimap wikimap`

### Install [Virtualenv](https://virtualenv.pypa.io/en/latest/), create a new environment, and activate it

```shell
pip install virtualenv
cd wikimap
virtualenv env
```
`source env/bin/activate`

### Use pip to install wikimap's dependencies
`pip install requirements.txt`

### Open the `3d_json_generator` notebook
run the cells until you generate the 'hand_wrangled.json' file
if you change the name of the file you generate, make sure to change the line in
'interface.html' where the file is loaded.

### Open `interface.html` in a web browser
You should see the graph displayed and be able to interact with it after the force-directed layout finishes.

Below is old, TODO: refactor

### Set up the environment variable that flask needs to run a testing server
    `source setup`

### Run the app on a local testing server

         flask run

## Subsequent setups (if you've already done the first-time setup)

### Set up the environment variable that flask needs to run a testing server
    `source setup`

### Run the app on a local testing server

         flask run
