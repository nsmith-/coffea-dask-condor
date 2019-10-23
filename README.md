To run:
```
condor_submit scheduler.jdl
tail -f logs/dask-scheduler.log
# (wait for it to start, grab node IP)
export DASK_SCHEDULER=<IP>:8786
condor_submit workers.jdl
```
