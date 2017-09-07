# ATLAS Titan allocation modeling

## QSS Base
Basic implementation of Queueing System Simulator

    from qssbase import QSS

    basicqueueingsystem = QSS(service_rate, num_nodes)
    basicqueueingsystem.run(arrival_rate, time_limit)

## QSS
Advanced implementation of Queueing System Simulator

    from qss import QSS
    from qss.core import stream_generator
    
    queueingsystem = QSS(num_nodes)
    queueingsystem.run(stream=stream_generator(arrival_rate, service_rate, num_jobs, time_limit))
