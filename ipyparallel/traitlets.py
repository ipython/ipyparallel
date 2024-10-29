"""Custom ipyparallel trait types"""

import sys

if sys.version_info < (3, 10):
    from importlib_metadata import entry_points
else:
    from importlib.metadata import entry_points

from traitlets import List, TraitError, Type


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
            chunks.append(f"  - {key}: {entry_point.value}")
        return '\n'.join(chunks)

    @help.setter
    def help(self, value):
        self._original_help = value

    def load_entry_points(self):
        """Load my entry point group"""
        return {
            entry_point.name.lower(): entry_point
            for entry_point in entry_points(group=self.entry_point_group)
        }

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
