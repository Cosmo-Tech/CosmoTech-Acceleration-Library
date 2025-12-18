from typing import Optional

from cosmotech.orchestrator.utils.translate import T

from cosmotech.coal.utils.configuration import Configuration, Dotdict


class ChannelInterface:
    required_keys = {}
    requirement_string: str = T("coal.store.output.data_interface.requirements")

    def __init__(self, dct: Dotdict = None):
        self.configuration = Configuration(dct)
        if not self.is_available():
            raise MissingChannelConfigError(self)

    def send(self, filter: Optional[list[str]] = None) -> bool:
        raise NotImplementedError()

    def delete(self) -> bool:
        raise NotImplementedError()

    def is_available(self) -> bool:
        try:
            return all(
                all(key in self.configuration[section] for key in self.required_keys[section])
                for section in self.required_keys.keys()
            )
        except KeyError:
            return False


class MissingChannelConfigError(Exception):
    def __init__(self, interface_class):
        self.message = T("coal.store.output.split.requirements").format(
            interface_name=interface_class.__class__.__name__, requirements=interface_class.requirement_string
        )
        super().__init__(self.message)
