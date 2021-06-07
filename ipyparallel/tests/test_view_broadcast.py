"""test BroadcastView objects"""
import pytest

from . import test_view


needs_map = pytest.mark.xfail(reason="map not yet implemented")


@pytest.mark.usefixtures('ipython')
class TestBroadcastView(test_view.TestView):
    def setUp(self):
        super().setUp()
        self._broadcast_view_used = False
        # use broadcast view for direct API
        real_direct_view = self.client.direct_view

        def broadcast_or_direct(targets):
            if isinstance(targets, int):
                return real_direct_view(targets)
            else:
                self._broadcast_view_used = True
                return self.client.broadcast_view(targets)

        self.client.direct_view = broadcast_or_direct

    def tearDown(self):
        super().tearDown()
        # note that a test didn't use a broadcast view
        if not self._broadcast_view_used:
            pytest.skip("No broadcast view used")

    @needs_map
    def test_map(self):
        pass

    @needs_map
    def test_map_ref(self):
        pass

    @needs_map
    def test_map_reference(self):
        pass

    @needs_map
    def test_map_iterable(self):
        pass

    @needs_map
    def test_map_empty_sequence(self):
        pass

    @needs_map
    def test_map_numpy(self):
        pass

    @pytest.mark.xfail(reason="Tracking gets disconnected from original message")
    def test_scatter_tracked(self):
        pass
