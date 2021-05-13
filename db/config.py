# SQLAlchemy URI documentation: https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls
# Scheme: "postgresql://<USERNAME>:<PASSWORD>@<IP_ADDRESS>:<PORT>/<DATABASE_NAME>"

DATABASE_URI = "postgresql://postgres:postgres@localhost:5432/wikipedia"
# don't worry this is just the local database config. no secret info is in the database running on localhost
