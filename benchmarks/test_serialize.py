import numpy as np

import ipyparallel as ipp


client = ipp.Client(profile='asv')
view = client.spanning_tree_view()
# print(view.apply_sync(lambda x: x, np.array([0] * 10000, dtype=np.int8)))
print(client[:].apply_sync(lambda x: x, np.array([0] * 1000, dtype=np.int8)))
