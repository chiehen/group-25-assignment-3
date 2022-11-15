# Distributed Query Execution

In this assignment, we implement distributed query execution in a shared-nothing environment.
We start with a single-node execution (see: `coordinator.cpp`), that we want to scale to multiple workers (`worker.cpp`).

We store the data to process on external storage, and partitioned them into many small chunks, all of which will be accessible via HTTP.
Our worker processes should be an elastic compute resource, which can scale depending on the workload.
The coordinator should then be responsible to manage the workers, and additionally ensure a proper load balancing between the workers.

Before starting to write code, consider the following design questions.

## Design Questions:

* How does the leader process know how many workers there are?
* How to distribute work "fairly" among workers?
* With what messages do leader - worker communicate?
* How to detect failed / crashed workers?
* How to recover when a worker failed?

## Execution:

Install dependencies (Ubuntu):

```bash
sudo apt install cmake g++ libcurl4-openssl-dev
```

Build executables:

```bash
mkdir -p cmake-build-debug
cd cmake-build-debug
cmake ..
make
```

Run on test data:

```bash
./runTest.sh data/test.csv
# should print 5
```

Run a larger workload with:

```bash
./testWorkload.sh https://db.in.tum.de/teaching/ws2223/clouddataprocessing/data/filelist.csv
# should print 275625
```

## Submission:
You can submit everything via GitLab.
First fork this repository, and add all members of your group to a single repository.
Then, in this group repository, add:
* Names of all members of your group
* Code that implements the assignment
* A written report about your implementation
