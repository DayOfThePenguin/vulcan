coverage:
	coverage run -m pytest
	coverage html
	coverage report -m

requirements:
	python -m pip freeze > requirements.txt

setup: requirements.txt
	pip install -r requirements.txt
	git clone --depth=1 https://gerrit.wikimedia.org/r/mediawiki/core.git mediawiki
	docker-compose -d up
	echo "Sleeping for 20 sec to let services start..."
	sleep 20s
	docker-compose down
	docker-compose exec mediawiki composer update
	docker-compose exec mediawiki /bin/bash /docker/install.sh

test:
	python -m pytest