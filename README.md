# Wikimap
Visualize the connections between ideas
## Examples:
[Interactive Map](https://dayofthepenguin.github.io/wikimap/interactive_demo.html)

[Orbiting Map](https://dayofthepenguin.github.io/wikimap/orbiting_demo.html)

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

### Install the Cairo 2d graphics library (if not already installed)
igraph's native plotting depends on [pycairo](https://www.cairographics.org/pycairo/), and
in order to install pycairo, you need to have the cairo C library installed.  See
[Cairo's downloads page](https://www.cairographics.org/download/) for install information

### Use pip to install wikimap's dependencies
```shell
pip install -r requirements.txt
```

### Open the `3d_json_generator` notebook
run the cells until you generate the 'hand_wrangled.json' file
if you change the name of the file you generate, make sure to change the line in
'interface.html' where the file is loaded.

### Open `interface.html` in a web browser
You should see the graph displayed and be able to interact with it after the force-directed layout finishes.

TODO: when adding functionality to work with a local wikipedia download (and adding parallel access to that...we
stick with a single thread/process when querying the actual wikipedia site to be respectful.), add a way (tf-idf for now?)
to calculate which pages we want to actually list as links (instead of arbitrarily picking the first x of the alphabet to make the visualizations tractable).
Goal: to apply some statistical comparisons to pages and if the similarity of the content is above a certain threshold, display a link
between the pages (filter all 600+ links of a page down to the smaller subset that we can actually display on screen at once)
Alternative: modify the 3d-graph-vis library so it doesn't calculate layouts in the browser. So you can calculate it locally
and then just plot the results in the browser. This would increase the total number of vertices you could display in the browser at
once

FEATURES: add a filter/legend that shows what color corresponds to which level and allow users to  so you can

TODO: refactor


