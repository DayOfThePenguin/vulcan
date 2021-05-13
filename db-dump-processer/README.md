## Environment setup (run each in a separate terminal)
The following commands set up a [Prefect](https://www.prefect.io/core) server (expect this to take some time the first time you run it
to install the docker images) to submit workflows to, a Prefect agent to execute workflows, a
[Dask](https://distributed.dask.org/en/latest/) scheduler to receive and distribute work from the Prefect agent, and 6 Dask worker
processes, each running on a single thread with 5GB of memory.


```shell
prefect backend server # make prefect run a local server
prefect server start --use-volume
prefect agent start
dask-scheduler
dask-worker tcp://192.168.0.12:8786 --nprocs 6 --nthreads 1 --memory-limit=5e9
```
On a 32GB machine, this leaves me with 2GB for system tasks before I start swapping. My experience is that you need about 5GB per
worker to load the wikipeda xml files, so divide your RAM by 5 to get the correct `nprocs` (num processes) for you. `nthreads` is
the number of threads per process, limit this to one to avoid using all your system's memory.

## Flow execution
After setting up the environment, you should have a Prefect interface available at `localhost:8000`.
Now run `dump-flow.py` to register the workflow with Prefect server. 
```shell
[env] > python dump-flow.py
```
After registering the flow, go to `localhost:8000`, select a flow to run, and run it. Prefect execution status can be monitored in the
interface and logs are available real-time in the *Logs* tab. The status of your local Dask cluster (worker CPU & memory usage) is
available in the Dask UI at `localhost:8787`.