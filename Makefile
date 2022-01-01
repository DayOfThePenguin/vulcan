coverage:
	coverage run -m pytest
	coverage html
	coverage report -m

setup: requirements.txt
	pip install -r requirements.txt

test:
	python -m pytest