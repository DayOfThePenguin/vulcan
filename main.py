import urllib.parse
import wikipedia

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="static/templates")

def search_result(page_name: str) -> str:
    return(wikipedia.page(page_name))

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})




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

