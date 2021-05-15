1. finish test coverage for test_wikidump and test_wikixml

2. add setup and teardown to test_wikixml and turn the path into a regex that will match any .xml.bz2 file in `tests/`

3. Work on the database notebooks. The main database notebook has a decent starting schema in it...once the functions modularized, use those functions to test interactively adding data to the database and then automate that process.

Profile memory using https://github.com/nvdv/vprof

Snakevis shows that ~80% of cpu time per WikipediaPage creation is from `parse_any` in `mwparserfromhell`. This is the cause of basically all the time it takes to get the headings, sections, and pagelinks. So no low-hanging optimization fruit unless I want to rewrite that method...

Most of the memory problems seem to be gone with the transition to multiprocessing and running outside vs code's debugger for long jobs

# Multiprocessing
### v1: single queue consumer, many xml file readers
Concept:
- multiple processes with xml file readers that feed WikipediaPage objects to a queue that is consumed by a process that takes WikipediaPages and loads them into database.
Notes:
- queue gets to 5000 item max and is bound by queue consumer in less than 6hrs/ 500,000 total items
- at that point, CPU usage basically goes to 0.
- didn't have any mechanism for checking database to see if item was already in database. assumed this was too slow to work practically. instead, just tried to commit the session after adding a single item and handled an error if orm/database raised uniqueness error because primary key (title) already existed. bad design decision, made really verbose logs.
- main thread loop is super complicated because need a shared conditional variable to indicate to consumer processes when readers are done reading files
- noticed that the unidecoding titles seems to lose some uniqueness (or there are just duplicate titles in the database dumps?) because during normal runs, getting 1-2 duplicate articles per every 2k added to db. think this is probably from words that are different when represented in unicode and not just ASCII.

### v1.5: add multiple queue consumers
Concept:
- solve the queue bottleneck by adding a second consumer process.
- not hard to implement, just fork a second consumer after the first.
Cons:
- can't tell what the right queue size is to optimize without studying. runs are hard to resume (have to restart every file because you can't just jump into the middle of an xml file with a line number or something...starting from the middle of a file, parser will hit end brackets that it never saw start brackets for because it started inside them), so studying optimal queue size is expensive.
- if guess queue size too big, consumers end up waiting. one of the process types is always waiting.
- main thread loop still complicated

### v2: xml reader and consumer combined in single process. multiple combined xml/consumer processes:
Concept:
- remove bottleneck of queue/having to optimize consumer/producer balance by combining consumer/processer code into single process
- ADDED query to database before staging each item to see if its already there. makes logs nicer, more elegant design overall. was worried would be too slow but even running queries on 2k items before committing them takes less than 10 seconds. BIG improvement.
- added statistics (files actually added to db, duplicates, errors on a per-commit basis, also each process logs duplicates, stats to files for analysis)
Stats:
- 14h 34m 7s complete db with 6 processes! (15May, 1007)
Notes:
- Great CPU usage! running 6 processes gives avg cpu utilization just over 6 (database has own processes/os/some background tasks).
- by committing files in chunks (2k files in v2.0)
- Very low RAM usage! Haven't seen over 10gb usage running 6 processes. Could probably add more, but would lose work in progress on current files (see above why can't resume spot in xml file) and it's (probably?) disk-bound if you just keep adding processes because all 6 processes are reading from and the database writes to a hdd.
- unlike dask/prefect, because I create a new process for each job, when a worker finishes reading a file, the process ends and memory is actaully returned to the system, so even with small memory leaks, don't get deadlocked like dask workers did.
- 10 minutes seems like an acceptable interval for checking if any processes have completed. when main thread sleeps, cpu usage on that process goes to 0. good design choice
- This is strange...code suggests `for` loop should catch both of the completed processes' exit signals the first time through, but it looks like it runs the main
`while` loop twice to pop the completed processes and add the new ones (the waiting to joins and next started logs are separate instead of together and worker count is 6 both times instead of being 5 & 6) 
```shell
[INFO/Xml_consumer_7] pid: 17830 committed. 0 errors
[INFO/MainProcess] main thread woke up, checking if there are any completed workers
[INFO/MainProcess] waiting to join completed worker 16709
[INFO/MainProcess] successfully joined completed worker 16709
[INFO/MainProcess] started process pid - 19074. worker count is 6

[INFO/MainProcess] waiting to join completed worker 16710
[INFO/MainProcess] successfully joined completed worker 16710
[INFO/Xml_consumer_10] child process calling self.run()
[INFO/MainProcess] started process pid - 19075. worker count is 6

[INFO/MainProcess] no completed workers. sleeping for 10 minutes
[INFO/Xml_consumer_11] child process calling self.run()
[INFO/Xml_consumer_8] pid: 17903 processed 16000 files
```
- it seems like there are a fair amount of files that are noted as duplicates where all that's returned is is `""`. Could this be because unidecode is failing to decode the title and is just returning `None`/a blank string? Or because there's just entries in the db that aren't readable?