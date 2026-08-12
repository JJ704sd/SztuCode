from __future__ import annotations

import asyncio
import fnmatch
import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

from pydantic import BaseModel

from sztu_code.core.bus.envelope import EventPushEnvelope
from sztu_code.core.trace.record import TraceRecord
from sztu_code.core.trace.writer import TraceWriter

logger = logging.getLogger(__name__)

type _QueuedEvent = tuple[bytes, str, str, str | None]


def _now() -> str:
    return datetime.now(UTC).isoformat()


@dataclass
class _Subscription:
    sub_id: str
    writer: asyncio.StreamWriter
    topics: list[str]
    scope: str


@dataclass
class _WriterState:
    writer: asyncio.StreamWriter
    queue: asyncio.Queue[_QueuedEvent]
    task: asyncio.Task[None] | None = None


class IpcEventBroadcaster:
    def __init__(
        self,
        trace: TraceWriter | None = None,
        *,
        max_pending_events: int = 256,
    ) -> None:
        self._subscriptions: list[_Subscription] = []
        self._writer_states: dict[asyncio.StreamWriter, _WriterState] = {}
        self._trace = trace
        self._max_pending_events = max(1, max_pending_events)

    # 注册一个客户端订阅，返回 subscription_id
    def subscribe(
        self,
        writer: asyncio.StreamWriter,
        topics: list[str],
        scope: str = "global",
    ) -> str:
        sub_id = f"sub-{uuid.uuid4().hex[:8]}"
        sub = _Subscription(sub_id=sub_id, writer=writer, topics=topics, scope=scope)
        self._subscriptions.append(sub)
        if writer not in self._writer_states:
            self._writer_states[writer] = _WriterState(
                writer=writer,
                queue=asyncio.Queue(maxsize=self._max_pending_events),
            )
        return sub_id

    # 移除指定 writer 的所有订阅
    def unsubscribe(self, writer: asyncio.StreamWriter) -> None:
        self._remove_writer(writer, cancel_task=True)

    # 移除 writer 的订阅与发送状态，并按调用场景决定是否取消后台发送任务
    def _remove_writer(self, writer: asyncio.StreamWriter, *, cancel_task: bool) -> None:
        self._subscriptions = [s for s in self._subscriptions if s.writer is not writer]
        state = self._writer_states.pop(writer, None)
        if cancel_task and state is not None and state.task is not None:
            state.task.cancel()

    # 将事件推送到所有匹配的订阅客户端，写入失败时延迟清理死连接
    async def handle(self, event: BaseModel) -> None:
        event_dict = event.model_dump()
        event_type: str = event_dict.get("type", "")
        run_id: str | None = event_dict.get("run_id")
        payload: bytes | None = None
        queued = False
        overflowed: set[asyncio.StreamWriter] = set()

        for sub in list(self._subscriptions):
            if not self._matches_topic(event_type, sub.topics):
                continue
            if not self._matches_scope(run_id, sub.scope):
                continue
            state = self._writer_states.get(sub.writer)
            if state is None:
                continue
            try:
                if payload is None:
                    payload = (
                        EventPushEnvelope(event=event_dict).model_dump_json().encode()
                        + b"\n"
                    )
                state.queue.put_nowait((payload, sub.sub_id, event_type, run_id))
                self._ensure_writer_task(state)
                queued = True
            except asyncio.QueueFull:
                overflowed.add(sub.writer)

        for writer in overflowed:
            logger.warning("disconnecting slow IPC subscriber after send queue overflow")
            self.unsubscribe(writer)
            writer.close()

        if queued:
            await asyncio.sleep(0)

    # 确保指定 writer 只有一个有序发送任务在运行
    def _ensure_writer_task(self, state: _WriterState) -> None:
        if state.task is None or state.task.done():
            state.task = asyncio.create_task(self._drain_writer(state))

    # 按入队顺序发送单个客户端的事件，网络失败时只移除该客户端
    async def _drain_writer(self, state: _WriterState) -> None:
        try:
            while True:
                try:
                    payload, sub_id, event_type, run_id = state.queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    state.writer.write(payload)
                    await state.writer.drain()
                    if self._trace is not None:
                        client_id = str(
                            state.writer.get_extra_info("peername", "<unknown>")
                        )
                        self._trace.emit(
                            TraceRecord(
                                ts=_now(),
                                direction="CORE→CLIENT",
                                layer="ipc",
                                kind="push",
                                run_id=run_id,
                                client_id=client_id,
                                data={"sub_id": sub_id, "event_type": event_type},
                            )
                        )
                except (ConnectionResetError, BrokenPipeError, OSError):
                    logger.debug("dead connection for sub %s, scheduling cleanup", sub_id)
                    self._remove_writer(state.writer, cancel_task=False)
                    return
                finally:
                    state.queue.task_done()
        finally:
            if self._writer_states.get(state.writer) is state:
                state.task = None
                if not state.queue.empty():
                    self._ensure_writer_task(state)

    # 检查事件类型是否匹配订阅的 topic 列表（支持 fnmatch glob 模式）
    @staticmethod
    def _matches_topic(event_type: str, topics: list[str]) -> bool:
        return any(fnmatch.fnmatch(event_type, pattern) for pattern in topics)

    # 检查事件 run_id 是否匹配订阅的 scope（global 全通，run:<id> 精确匹配）
    @staticmethod
    def _matches_scope(run_id: str | None, scope: str) -> bool:
        if scope == "global":
            return True
        if scope.startswith("run:"):
            return run_id == scope[4:]
        return False
