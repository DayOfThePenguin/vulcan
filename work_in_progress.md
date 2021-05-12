1. finish test coverage for test_wikidump and test_wikixml

2. add setup and teardown to test_wikixml and turn the path into a regex that will match any .xml.bz2 file in `tests/`

3. Work on the database notebooks. The main database notebook has a decent starting schema in it...once the functions modularized, use those functions to test interactively adding data to the database and then automate that process.