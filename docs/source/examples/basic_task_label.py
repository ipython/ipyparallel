"""Basic task label example"""

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


# use load balanced view
bview = rc.load_balanced_view()
ar_list_b1 = [
    bview.set_flags(label=f"mylabel_map_{i:02}").map_async(wait, [2]) for i in range(10)
]
ar_list_b2 = [
    bview.set_flags(label=f"mylabel_map_{i:02}").apply_async(wait, 2) for i in range(10)
]
bview.wait(ar_list_b1)
bview.wait(ar_list_b2)


# use direct view
dview = rc[:]
ar_list_d1 = [
    dview.set_flags(label=f"mylabel_map_{i + 10:02}").apply_async(wait, 2)
    for i in range(10)
]
ar_list_d2 = [
    dview.set_flags(label=f"mylabel_map_{i + 10:02}").map_async(wait, [2])
    for i in range(10)
]
dview.wait(ar_list_d1)
dview.wait(ar_list_d2)

# query database
data = rc.db_query({'label': {"$nin": ""}}, keys=['msg_id', 'label', 'engine_uuid'])
for d in data:
    print(f"msg_id={d['msg_id']}; label={d['label']}; engine_uuid={d['engine_uuid']}")

cluster.stop_cluster_sync()
