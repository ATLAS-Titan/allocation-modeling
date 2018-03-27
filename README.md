# ATLAS Titan allocation modeling

## QSS
Advanced implementation of Queueing System Simulator

```
from qss import QSS, stream_generator, stream_generator_by_file
from qss.constants import StreamName
 
qs = QSS(
    num_nodes, 
    queue_limit,
    use_queue_buffer,
    time_limit,
    output_file,
    trace_file)
 
qs.run(
    streams=[
        stream_generator(
            arrival_rate,
            execution_rate,
            num_nodes_per_job,
            StreamName.Main,
            first_arrival_timestamp),
        stream_generator_by_file(
            file_name,
            StreamName.External)
    ],
    verbose=True
)
 
qs.print_stats()
```
