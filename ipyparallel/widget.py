from ipyparallel.nbextension import clustermanager
import ipyparallel


class IPyParallelCluster():
    def __init__(self):
        self.cluster_manager = clustermanager.ClusterManager()
        self._active_profile = 'default'

    @property
    def active_profile(self):
        return self._active_profile

    def _widget(self):
        try:
            return self._cached_widget
        except AttributeError:
            pass

        try:
            import ipywidgets
        except ImportError:
            self._cached_widget = None
            return None

        input_clusters_select = ipywidgets.RadioButtons(
            options=[],
            description='Select Profile:',
            disabled=False,
            style={'description_width': 'initial'}
        )
        input_number_workers = ipywidgets.IntText(
            value=1,
            description='Workers',
            disabled=False
        )
        button_action = ipywidgets.Button(
            description=''
        )

        def update_widgets(changed=None):
            profiles = {_['profile']: _ for _ in self.cluster_manager.list_profiles()}

            input_clusters_select.options = list(sorted(profiles))
            self._active_profile = input_clusters_select.value

            status = profiles[self.active_profile]['status']
            input_number_workers.layout.visibility = 'visible' if status == 'stopped' else 'hidden'
            button_action.description = 'start' if status == 'stopped' else 'stop'

        def on_button_clicked(b):
            active_profile = input_clusters_select.value
            if button_action.description == 'start':
                result = self.cluster_manager.start_cluster(active_profile, int(input_number_workers.value))
            else:
                result = self.cluster_manager.stop_cluster(active_profile)
            update_widgets()

        button_action.on_click(on_button_clicked)
        input_clusters_select.observe(update_widgets, names='value')
        update_widgets()

        box = ipywidgets.Box([
            ipywidgets.HTML('<h1>IPyParallel Cluster</h1>'),
            input_clusters_select,
            input_number_workers,
            button_action,
        ], layout=ipywidgets.Layout(
            display='flex',
            flex_flow='column',
            align_items='stretch',
        ))

        self._cached_widget = box
        return self._cached_widget

    def _ipython_display_(self, **kwargs):
        widget = self._widget()
        if widget is not None:
            return widget._ipython_display_(**kwargs)
        else:
            from IPython.display import display

            data = {"text/plain": repr(self), "text/html": self._repr_html_()}
            display(data, raw=True)

    def get_client(self):
        return ipyparallel.Client(profile=self.active_profile)
