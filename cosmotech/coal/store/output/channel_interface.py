from typing import Optional

from cosmotech.orchestrator.utils.translate import T


class ChannelInterface:
    required_keys = {}
    requirement_string: str = T("coal.store.output.data_interface.requirements")

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
