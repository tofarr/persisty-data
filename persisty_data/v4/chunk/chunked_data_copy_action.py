from servey.action.action import action

from persisty_data.v4.chunk.chunked_data_copy_event import ChunkedDataCopyEvent


@action
def chunked_data_copy_action(event: ChunkedDataCopyEvent):
    event.copy_data_to_chunks()
