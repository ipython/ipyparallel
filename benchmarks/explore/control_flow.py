import numpy as np

import ipyparallel as ipp


def main():
    client = ipp.Client(profile='asv')
    # client.debug = True
    direct_view = client[:]
    direct_view.targets = list(range(5))
    direct_view_result = direct_view.apply(lambda x: x * 2, 13)
    print(direct_view_result.get())
    # load_balanced_view = client.load_balanced_view()
    # result = load_balanced_view.apply(lambda x: x * 2, 13)
    # print(result.get())
    broadcast_view = client.broadcast_view(is_coalescing=True)
    broadcast_result = broadcast_view.apply_sync(
        lambda x: x, np.array([0] * 8, dtype=np.int8)
    )

    print(broadcast_result)
    print(len(broadcast_result))
    # broadcast_view2 = client.broadcast_view(is_coalescing=False)
    # broadcast_result = broadcast_view2.apply_sync(
    #     lambda x: x, np.array([0] * 8, dtype=np.int8)
    # )
    #
    # print(broadcast_result)
    # print(len(broadcast_result))


if __name__ == '__main__':
    main()
