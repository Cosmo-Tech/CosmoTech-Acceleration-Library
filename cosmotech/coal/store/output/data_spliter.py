from typing import Optional

from cosmotech.orchestrator.utils.translate import T

from cosmotech.coal.store.output.data_interface import DataInterface
from cosmotech.coal.utils.logger import LOGGER


class DataSpliter(DataInterface):
    requirement_string: str = "(Requires any working interface)"
    targets = []
    possible_interfaces = [DataInterface]

    def __init__(self):

        for InterfaceClass in self.possible_interfaces:

            try:
                _i = InterfaceClass()
                if _i.is_available():
                    self.targets.append(_i)
            except Exception as e:
                LOGGER.warning(
                    T("coal.store.output.split.requirements").format(
                        interface_name=InterfaceClass.__name__, requirements=InterfaceClass.requirement_string
                    )
                )
        if not self.targets:
            raise AttributeError(T("coal.store.output.split.no_targets"))

    def send(self, filter: Optional[list[str]] = None) -> bool:
        any_ok = False
        for i in self.targets:
            try:
                any_ok = any_ok or i.send(filter=filter)
            except Exception as e:
                LOGGER.error(T("coal.store.output.split.send.error").format(interface_name=i.__class__.__name__))
        return any_ok

    def delete(self, filter: Optional[list[str]] = None) -> bool:
        any_ok = False
        for i in self.targets:
            try:
                any_ok = any_ok or i.delete()
            except Exception as e:
                LOGGER.error(T("coal.store.output.split.delete.error").format(interface_name=i.__class__.__name__))
        return any_ok
