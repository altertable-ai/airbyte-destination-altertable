from typing import Any, Mapping, Iterable, Union
from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import (
    AirbyteConnectionStatus,
    ConfiguredAirbyteCatalog,
    AirbyteMessage,
    AirbyteStateMessage,
    Type,
    Status,
)
from .altertable_writer import AltertableWriter


class DestinationAltertable(Destination):
    def check(self, logger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        try:
            writer = AltertableWriter(config)
            writer.test_connection()
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message=str(e))

    def write(
        self,
        config: Mapping[str, Any],
        configured_catalog: ConfiguredAirbyteCatalog,
        input_messages: Iterable[AirbyteMessage],
    ) -> Iterable[Union[AirbyteMessage, AirbyteStateMessage]]:
        writer = AltertableWriter(config)
        writer.set_catalog(configured_catalog)

        for message in input_messages:
            if message.type == Type.RECORD:
                writer.buffer_record(message.record)
            elif message.type == Type.STATE:
                writer.flush()
                yield message

        writer.flush()
