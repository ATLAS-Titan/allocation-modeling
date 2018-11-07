# Modeling allocation utilization strategies on supercomputers

This project includes design and development of a quantitative model and a simulator (see repository's [wiki](https://github.com/ATLAS-Titan/allocation-modeling/wiki) for details). The description and the code of the designed simulator is presented in the current repository.

## Introduction

The focus of the research work is on the study of the load on a supercomputer (specifically, on [Titan](https://www.olcf.ornl.gov/olcf-resources/compute-systems/titan/) supercomputer ) and its modeling. The load on a resource is defined as a number of busy service nodes at a certain time; it is determined by the number and parameters of running computing jobs:
* real execution time per job (initially is requested the maximum required time for job to be executed, i.e., wall time) 
* number of required nodes per job
* jobs generation rate

The concept of an **execution strategy** is defined as the set of values of denoted parameters that uniquely define the group of jobs to be executed.

## Simulator

The designed analysis and modeling tool simulates the load on a supercomputer and produces job traces for a given workload. It is characterized by the following features:
* based on [queueing theory](https://en.wikipedia.org/wiki/Queueing_theory)
    * _arrival process_ is represented by streams that are responsible for job generation and is described either by a Poisson process or by a deterministic model
    * _service/server process_ is represented by a set of nodes that simulate job execution process and is described either by a Poisson process or by a deterministic model as well
    * _number of servers/nodes_ corresponds to the number of computing nodes (in terms of Titan supercomputer)
    * _capacity of the queue or system overall_ is defined and can be limited by the queue limit (either per stream or for the total number of jobs in the queue) and assumes that the queue buffer is not used, otherwise the capacity is unlimited
    * _queueing discipline_ is provided in two options: FIFO or Priority.
* includes the possibility to use _the schedule_ to boost the starting for the execution of "small" jobs (i.e., _backfill mode_ in terms of Titan supercomputer)
* provides the explicit job state model (state transitions)
    * _generated_ - _holding_ (i.e., Titan notation: blocked) in the buffer - _pending_ (i.e., Titan notation: eligible-to-run) in the queue - _starting_ - _executing_ - _finished_

Simulator is named as **Queueing System Simulator** (**QSS**)

## Usage

#### The main class object definition

```python
from qss import QSS
qss_obj = QSS(num_nodes=18688)
```
Required argument `num_nodes` defines the number of service/computing nodes. \
Optional arguments:
* `queue_limit` defines the total limit of the queue (default value is `None`)
* `use_queue_buffer` is a flag to use the queue buffer (default value is `False`)
* `use_scheduler` is a flag to use schedule, i.e., backfill mode (default value is `False`)
* `time_limit` provides a timestamp when the processing should be stopped; if not is defined then the processing continues while the job generators (streams) produce new jobs (default value is `None`)
* `output_file` file name, store the output information per job (each record includes: *arrival_timestamp*, *start_execution_timestamp*, *end_execution_timestamp*, *num_nodes*, *source*, *label*) to keep job records after its execution is done (default value is `None`)
* `trace_file` file name, store a real time information about the system state (*current_time*, *num_jobs_in_buffer*, *num_jobs_in_queue*, *num_jobs_executing*, *current_action*) that's also can be printed out at the screen if the corresponding argument `verbose` is set for the method `run()` (default value is `None`)

#### Jobs generation definition

Implementation of jobs generation is based on python function called `generator`, which is represented as a "stream" function inside the QSS package. There are several predefined such functions (see module `qss/stream.py`), but it is up to user to use one of them or to create a different one.

```python
from qss import stream_generator, stream_generator_by_file
stream_1 = stream_generator(arrival_rate=11./36,
                            execution_rate=1./3,
                            num_nodes=100,
                            source='main',
                            num_jobs=None,
                            time_limit=1000.)
stream_2 = stream_generator_by_file(file_name='qss_input.txt',
                                    source='external',
                                    time_limit=1000.)
```

It is important to note the following arguments in the predefined stream functions - `num_nodes` and `time_limit` - that represent restrictions imposed either on the number of generated jobs or on the processing time. Either of them **should** be set.

#### Run the processing

```python
qss_obj.run(streams=[stream_1, stream_2],
            verbose=True)
qss_obj.print_stats()
```
