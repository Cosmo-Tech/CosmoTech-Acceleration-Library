from typing import Optional

from cosmotech.orchestrator.utils.translate import T


class ChannelInterface:

    requirement_string: str = T("coal.store.output.data_interface.requirements")

    def send(self, filter: Optional[list[str]] = None) -> bool:
        raise NotImplementedError()

    def delete(self) -> bool:
        raise NotImplementedError()

    def is_available(self) -> bool:
        raise NotImplementedError()
