"""test BroadcastView objects"""

import pytest

from . import test_view

needs_map = pytest.mark.xfail(reason="map not yet implemented")


@pytest.mark.usefixtures('ipython')
class TestBroadcastView(test_view.TestView):
    is_coalescing = False

    def setup_method(self):
        super().setup_method()
        self._broadcast_view_used = False
        # use broadcast view for direct API
        real_direct_view = self.client.real_direct_view = self.client.direct_view

        def broadcast_or_direct(targets):
            if isinstance(targets, int):
                return real_direct_view(targets)
            else:
                self._broadcast_view_used = True
                return self.client.broadcast_view(
                    targets, is_coalescing=self.is_coalescing
                )

        self.client.direct_view = broadcast_or_direct

    def teardown_method(self):
        super().teardown_method()
        # note that a test didn't use a broadcast view
        if not self._broadcast_view_used:
            pytest.skip("No broadcast view used")

    @pytest.mark.xfail(reason="Tracking gets disconnected from original message")
    def test_scatter_tracked(self):
        pass


class TestBroadcastViewCoalescing(TestBroadcastView):
    is_coalescing = True

    @pytest.mark.xfail(reason="coalescing view doesn't preserve target order")
    def test_target_ordering(self):
        self.minimum_engines(4)
        ids_in_order = self.client.ids
        dv = self.client.real_direct_view(ids_in_order)

        dv.scatter('rank', ids_in_order, flatten=True, block=True)
        assert dv['rank'] == ids_in_order

        view = self.client.broadcast_view(ids_in_order, is_coalescing=True)
        assert view['rank'] == ids_in_order

        view = self.client.broadcast_view(ids_in_order[::-1], is_coalescing=True)
        assert view['rank'] == ids_in_order[::-1]

        view = self.client.broadcast_view(ids_in_order[::2], is_coalescing=True)
        assert view['rank'] == ids_in_order[::2]

        view = self.client.broadcast_view(ids_in_order[::-2], is_coalescing=True)
        assert view['rank'] == ids_in_order[::-2]

    def test_engine_metadata(self):
        self.minimum_engines(4)
        ids_in_order = sorted(self.client.ids)
        dv = self.client.real_direct_view(ids_in_order)
        dv.scatter('rank', ids_in_order, flatten=True, block=True)
        view = self.client.broadcast_view(ids_in_order, is_coalescing=True)
        ar = view.pull('rank', block=False)
        result = ar.get(timeout=10)
        assert isinstance(ar.engine_id, list)
        assert isinstance(ar.engine_uuid, list)
        assert result == ar.engine_id
        assert sorted(ar.engine_id) == ids_in_order

        even_ids = ids_in_order[::-2]
        view = self.client.broadcast_view(even_ids, is_coalescing=True)
        ar = view.pull('rank', block=False)
        result = ar.get(timeout=10)
        assert isinstance(ar.engine_id, list)
        assert isinstance(ar.engine_uuid, list)
        assert result == ar.engine_id
        assert sorted(ar.engine_id) == sorted(even_ids)

    @pytest.mark.xfail(reason="displaypub ordering not preserved")
    def test_apply_displaypub(self):
        pass


# FIXME
del TestBroadcastView
