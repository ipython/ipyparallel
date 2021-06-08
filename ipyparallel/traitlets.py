"""Custom ipyparallel trait types"""
from traitlets import Type


class Launcher(Type):
    """Extend Type to allow launcher prefix abbreviations"""

    def __init__(self, *args, kind, **kwargs):
        self.kind = kind
        kwargs.setdefault('klass', 'ipyparallel.cluster.launcher.BaseLauncher')
        super().__init__(*args, **kwargs)

    def validate(self, obj, value):
        if isinstance(value, str) and '.' not in value:
            value = f'ipyparallel.cluster.launcher.{value}{self.kind}Launcher'
        return super().validate(obj, value)
