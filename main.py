"""
WikiMap: Visualize the connections between ideas
Copyright (C) 2015  Colin Dablain

    WikiMap is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    WikiMap is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with WikiMap in COPYING.  If not, see <https://www.gnu.org/licenses/>.
"""

import urllib.parse
import wikipedia

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from pathlib import Path

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="static/templates")

def search_result(page_name: str) -> str:
    return(wikipedia.page(page_name))

def get_available_maps():
    data_path = Path("static/data")
    available_maps = []
    for child in data_path.iterdir():
        if child.suffix == ".json":
            available_maps.append(child.stem)
    return available_maps

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request,
                                                     "maps": get_available_maps()})

@app.get("/maps/{file_name}")
async def graph_json(file_name: str, request: Request, response_class=HTMLResponse):
    return templates.TemplateResponse("interface.html", {"request": request, "file_name": file_name})

# @app.get("/maps/{page_name}")
# async def read_item(page_name: str = "Quantum%20Mechanics", limit: int = 10):
#     print(urllib.parse.unquote(page_name))
#     return {"name": page_name.split("-"), "limit": limit,
#             "result": search_result(page_name)}


# fake_items_db = [{"item_name": "Foo"}, {"item_name": "Bar"}, {"item_name": "Baz"}]
# @app.get("/items/{id}", response_class=HTMLResponse)
# async def read_item(request: Request, id: str):
#     return templates.TemplateResponse("item.html", {"request": request, "id": id})

