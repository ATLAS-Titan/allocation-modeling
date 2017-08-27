# ATLAS Titan allocation modeling

## QSS
Queueing System Simulator

    from qss import QSS

    queueingsystem = QSS(service_rate, num_nodes)
    queueingsystem.run(arrival_timestamps, time_limit)
