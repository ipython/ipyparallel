import ipyparallel as ipp


def main():
    client = ipp.Client(profile='asv')
    # client.debug = True
    # direct_view = client[:]
    # direct_view_result = direct_view.apply(lambda x: x * 2, 13)
    # print(direct_view_result.get())
    # load_balanced_view = client.load_balanced_view()
    # result = load_balanced_view.apply(lambda x: x * 2, 13)
    # print(result.get())
    broadcast_view = client.broadcast_view(is_coalescing=True)
    broadcast_result = broadcast_view.apply_sync(
        lambda x: f'The answer to Life, the Universe and Everything: {x * 2}', 21
    )

    print(broadcast_result)
    spanning_tree_view = client.spanning_tree_view()
    spanning_result = spanning_tree_view.apply(
        lambda x: f'The answer to Life, the Universe and Everything: {x * 2}', 21
    ).get()
    # print(spanning_result)
    # print(f'{len(broadcast_result)} == {len(spanning_result)}')

if __name__ == '__main__':
    main()
