import ipyparallel as ipp

def main():
    client = ipp.Client(profile='asv')
    # direct_view = client[:]
    # direct_view_result = direct_view.apply(lambda x: x * 2, 13)
    # load_balanced_view = client.load_balanced_view()
    # result = load_balanced_view.apply(lambda x: x * 2, 13)
    # print(result.get())
    # print(direct_view_result.get())
    broad_cast_view = client.broadcast_view()
    bc_result = broad_cast_view.apply(lambda x: x * 2, 13)
    print(bc_result.get())





if __name__ == '__main__':
    main()
