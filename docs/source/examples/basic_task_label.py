"""Basic task label example

Labels can be used for identifying or semantically grouping tasks. Using the $glob operator
relevant task records can be queried from the Task Database.

Authors
-------
* ottointhesky
"""

import ipyparallel as ipp

# start up ipp cluster with 2 engines
cluster = ipp.Cluster(n=2)
cluster.start_cluster_sync()

rc = cluster.connect_client_sync()
rc.wait_for_engines(n=2)


def wait(t):
    import time

    tic = time.time()
    time.sleep(t)
    return time.time() - tic


count = 5  # number of tasks per method

# use load balanced view
bview = rc.load_balanced_view()
ar_list_b1 = [
    bview.set_flags(label=f"bview_map_{i:02}").map_async(wait, [2])
    for i in range(count)
]
ar_list_b2 = [
    bview.set_flags(label=f"bview_apply_{i:02}").apply_async(wait, 2)
    for i in range(count)
]
bview.wait(ar_list_b1)
bview.wait(ar_list_b2)


# use direct view
dview = rc[:]
ar_list_d1 = [
    dview.set_flags(label=f"dview_map_{i + 10:02}").map_async(wait, [2])
    for i in range(count)
]
ar_list_d2 = [
    dview.set_flags(label=f"dview_apply_{i + 10:02}").apply_async(wait, 2)
    for i in range(count)
]
dview.wait(ar_list_d1)
dview.wait(ar_list_d2)


def print_records(titel, data):
    print(f"{titel} ({len(data)} records)")
    for d in data:
        print(
            f"\tmsg_id={d['msg_id']}; label={d['label']}; engine_uuid={d['engine_uuid']}"
        )


query_keys = ['msg_id', 'label', 'engine_uuid']

# query database
data = rc.db_query({'label': {"$nin": ""}}, keys=query_keys)
print_records("all entries with labels", data)

data = rc.db_query({'label': {"$glob": "dview_*"}}, keys=query_keys)
print_records("all dview label entries", data)

data = rc.db_query({'label': {"$glob": "*_map_*"}}, keys=query_keys)
print_records("all map label entries", data)

# stop cluster
cluster.stop_cluster_sync()
