FROM python:3.8.12-slim-bullseye as base

WORKDIR /code

COPY requirements.txt .

RUN pip install -r requirements.txt

########## test ###########

FROM base as test

CMD [ "pytest" ]

########## dev ##########
FROM base as debug

CMD [ "python", "main.py" ]