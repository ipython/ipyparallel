import ipyparallel as ipp


def main():
    client = ipp.Client(profile='asv')
    # direct_view = client[:]
    # direct_view_result = direct_view.apply(lambda x: x * 2, 13)
    # print(direct_view_result.get())
    load_balanced_view = client.load_balanced_view()
    result = load_balanced_view.apply(lambda x: x * 2, 13)
    print(result.get())
    broad_cast_view = client.broadcast_view()
    print(broad_cast_view.apply(
        lambda x: f'The answer to Life, the Universe and Everything: {x * 2}', 21
    ).get())



if __name__ == '__main__':
    main()
