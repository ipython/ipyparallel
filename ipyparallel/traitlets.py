"""Custom ipyparallel trait types"""
import entrypoints
from traitlets import Type


class Launcher(Type):
    """Entry point-extended Type

    classes can be registered via entry points
    in addition to standard 'mypackage.MyClass' strings
    """

    def __init__(self, *args, entry_point_group, **kwargs):
        self.entry_point_group = entry_point_group
        kwargs.setdefault('klass', 'ipyparallel.cluster.launcher.BaseLauncher')
        super().__init__(*args, **kwargs)

    _original_help = ''

    @property
    def help(self):
        """Extend help by listing currently installed choices"""
        chunks = [self._original_help]
        chunks.append("Currently installed: ")
        for key, entry_point in self.load_entry_points().items():
            chunks.append(
                "  - {}: {}.{}".format(
                    key, entry_point.module_name, entry_point.object_name
                )
            )
        return '\n'.join(chunks)

    @help.setter
    def help(self, value):
        self._original_help = value

    def load_entry_points(self):
        """Load my entry point group"""
        # load the group
        group = entrypoints.get_group_named(self.entry_point_group)
        # make it case-insensitive
        return {key.lower(): value for key, value in group.items()}

    def validate(self, obj, value):
        if isinstance(value, str):
            # first, look up in entry point registry
            registry = self.load_entry_points()
            key = value.lower()
            if key in registry:
                value = registry[key].load()
        return super().validate(obj, value)
