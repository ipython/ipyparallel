"""Custom ipyparallel trait types"""
import entrypoints
from traitlets import List
from traitlets import TraitError
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


class PortList(List):
    """List of ports

    For use configuring a list of ports to consume

    Ports will be a list of valid ports

    Can be specified as a port-range string for convenience
    (mainly for use on the command-line)
    e.g. '10101-10105,10108'
    """

    @staticmethod
    def parse_port_range(s):
        """Parse a port range string in the form '1,3-5,6' into [1,3,4,5,6]"""
        ports = []
        ranges = s.split(",")
        for r in ranges:
            start, _, end = r.partition("-")
            start = int(start)
            if end:
                end = int(end)
                ports.extend(range(start, end + 1))
            else:
                ports.append(start)
        return ports

    def from_string_list(self, s_list):
        ports = []
        for s in s_list:
            ports.extend(self.parse_port_range(s))
        return ports

    def validate(self, obj, value):
        if isinstance(value, str):
            value = self.parse_port_range(value)
        value = super().validate(obj, value)
        for item in value:
            if not isinstance(item, int):
                raise TraitError(
                    f"Ports must be integers in range 1-65536, not {item!r}"
                )
            if not 1 <= item <= 65536:
                raise TraitError(
                    f"Ports must be integers in range 1-65536, not {item!r}"
                )
        return value
