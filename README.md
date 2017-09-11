# ATLAS Titan allocation modeling

## QSS Base
Basic implementation of Queueing System Simulator

    from qssbase import QSS

    basicqueueingsystem = QSS(service_rate, num_nodes)
    basicqueueingsystem.run(arrival_rate, time_limit)

## QSS
Advanced implementation of Queueing System Simulator

    from qss import QSS, stream_generator
    
    queueingsystem = QSS(num_nodes)
    queueingsystem.run(streams=[
        stream_generator(arrival_rate, 
                         execution_rate, 
                         num_nodes_per_job, 
                         source_label, 
                         num_generated_jobs, 
                         time_limit)
    ])
