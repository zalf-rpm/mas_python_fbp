from __future__ import annotations

import asyncio
import sys
from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any, cast

from zalfmas_fbp.run import channel_starter_service, channels, components, local_components_service, process


class DummyProcess:
    def __init__(self, stdout: Any = None):
        self.stdout = stdout

    def poll(self) -> None:
        return None

    def terminate(self) -> None:
        return None


class DummyStdout:
    def __init__(self, lines: list[str]):
        self._lines: Iterator[str] = iter(lines)

    def readline(self) -> str:
        return next(self._lines, "")


class ManagedDummyProcess:
    def __init__(self):
        self.returncode: int | None = None
        self.terminated = False
        self.killed = False
        self.wait_calls = 0

    def poll(self) -> int | None:
        return self.returncode

    def terminate(self) -> None:
        self.terminated = True
        self.returncode = 0

    def kill(self) -> None:
        self.killed = True
        self.returncode = -9

    def wait(self, timeout: float | None = None) -> int:
        self.wait_calls += 1
        if self.returncode is None:
            raise local_components_service.sp.TimeoutExpired("managed-dummy", timeout or 0)
        return self.returncode


class DummyProcessCap:
    def __init__(self):
        self.stop_called = False

    async def stop(self) -> SimpleNamespace:
        self.stop_called = True
        return SimpleNamespace(stopped=True)


def test_start_local_component_appends_log_level(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}

    def fake_popen(args: list[str], **kwargs: Any) -> DummyProcess:
        captured["args"] = args
        captured["kwargs"] = kwargs
        return DummyProcess()

    monkeypatch.setattr(components.sp, "Popen", fake_popen)

    components.start_local_component("python component.py", "reader-sr", name="demo", log_level="ERROR")

    assert captured["args"] == [
        sys.executable,
        "component.py",
        "reader-sr",
        '--name="demo"',
        "--log_level=ERROR",
    ]
    assert captured["kwargs"]["text"] is True


def test_start_local_process_component_appends_log_level(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}

    def fake_popen(args: list[str], **kwargs: Any) -> DummyProcess:
        captured["args"] = args
        captured["kwargs"] = kwargs
        return DummyProcess()

    monkeypatch.setattr(process.sp, "Popen", fake_popen)

    process.start_local_process_component("python process.py", "writer-sr", name="demo", log_level="DEBUG")

    assert captured["args"] == [
        sys.executable,
        "process.py",
        "writer-sr",
        '--name="demo"',
        "--log_level=DEBUG",
    ]
    assert captured["kwargs"]["text"] is True


def test_start_first_channel_appends_log_level(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}

    def fake_popen(args: list[str], **kwargs: Any) -> DummyProcess:
        captured["args"] = args
        captured["kwargs"] = kwargs
        return DummyProcess(stdout=DummyStdout(["readerSR=reader-sr\n", "writerSR=writer-sr\n"]))

    monkeypatch.setattr(channels.sp, "Popen", fake_popen)

    _, reader_sr, writer_sr = channels.start_first_channel("/tmp/channel", log_level="INFO")

    assert "--log_level=INFO" in captured["args"]
    assert captured["kwargs"]["text"] is True
    assert reader_sr == "reader-sr"
    assert writer_sr == "writer-sr"


def test_start_channel_appends_log_level(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}

    def fake_popen(args: list[str], **kwargs: Any) -> DummyProcess:
        captured["args"] = args
        captured["kwargs"] = kwargs
        return DummyProcess()

    monkeypatch.setattr(channels.sp, "Popen", fake_popen)

    channels.start_channel(
        "/tmp/channel",
        "startup-id",
        "writer-sr",
        name="demo channel",
        no_of_channels=2,
        log_level="WARNING",
    )

    assert "--log_level=WARNING" in captured["args"]


def test_runnable_passes_log_level_to_child_component(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}

    def fake_start_local_component(
        path_to_executable: str,
        port_infos_reader_sr: str,
        name: str | None = None,
        log_level: str | None = None,
    ) -> DummyProcess:
        captured["path_to_executable"] = path_to_executable
        captured["port_infos_reader_sr"] = port_infos_reader_sr
        captured["name"] = name
        captured["log_level"] = log_level
        return DummyProcess()

    monkeypatch.setattr(local_components_service.comp, "start_local_component", fake_start_local_component)
    monkeypatch.setattr(local_components_service.common, "sturdy_ref_str_from_sr", lambda sr: sr)

    runnable = local_components_service.Runnable("/tmp/component", log_level="CRITICAL")
    context = SimpleNamespace(
        params=SimpleNamespace(portInfosReaderSr="reader-sr", name="demo", _has=lambda name: False),
        results=SimpleNamespace(),
    )

    asyncio.run(runnable.start_context(cast("Any", context)))

    assert captured["path_to_executable"] == "/tmp/component"
    assert captured["port_infos_reader_sr"] == "reader-sr"
    assert captured["name"] == "demo"
    assert captured["log_level"] == "CRITICAL"
    assert context.results.success is True


def test_process_handle_close_stops_child_and_terminates_runtime() -> None:
    async def run_test() -> None:
        process_cap = DummyProcessCap()
        proc = ManagedDummyProcess()
        removed: list[ManagedDummyProcess] = []
        handle = local_components_service.ProcessHandle(
            cast("Any", process_cap),
            cast("Any", proc),
            cast("Any", removed.append),
        )

        assert await handle.process() is process_cap
        assert await handle.alive() is True

        assert await handle.close() is True

        assert process_cap.stop_called is True
        assert proc.terminated is True
        assert proc.killed is False
        assert removed == [proc]
        assert await handle.alive() is False

    asyncio.run(run_test())


def test_process_factory_create_returns_process_handle(monkeypatch: Any) -> None:
    async def run_test() -> None:
        process_cap = DummyProcessCap()
        proc = ManagedDummyProcess()

        class DummyWriter:
            def __init__(self):
                self.unregister_writer = None
                self.process_cap_received_future = asyncio.Future()
                self.process_cap_received_future.set_result(cast("Any", process_cap))

        class DummyRestorer:
            async def save_cap(self, _cap: object) -> tuple[str, str]:
                return "writer-token", "unsave-token"

            async def unsave(self, _token: str) -> None:
                return None

            def sturdy_ref_str(self, _token: str) -> str:
                return "writer-sr"

        monkeypatch.setattr(local_components_service, "ProcessWriter", DummyWriter)
        monkeypatch.setattr(
            local_components_service.process,
            "start_local_process_component",
            lambda *_args, **_kwargs: proc,
        )

        factory = local_components_service.ProcessFactory(
            "/tmp/process",
            restorer=cast("Any", DummyRestorer()),
        )

        handle = await factory.create(cast("Any", None))

        assert isinstance(handle, local_components_service.ProcessHandle)
        assert await handle.process() is process_cap
        assert factory.procs == [proc]

    asyncio.run(run_test())


def test_channel_service_does_not_pass_log_level_to_startup_channel(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}

    def fake_start_first_channel(
        path_to_channel: str,
        name: str | None = None,
        log_level: str | None = None,
    ) -> tuple[DummyProcess, None, None]:
        captured["path_to_channel"] = path_to_channel
        captured["name"] = name
        captured["log_level"] = log_level
        return DummyProcess(), None, None

    monkeypatch.setattr(channel_starter_service.channels, "start_first_channel", fake_start_first_channel)

    service = channel_starter_service.StartChannelsService(
        con_man=cast("Any", SimpleNamespace()),
        path_to_channel="/tmp/channel",
    )

    asyncio.run(service.create_startup_info_channel())

    assert captured["path_to_channel"] == "/tmp/channel"
    assert captured["name"] is None
    assert captured["log_level"] is None


def test_channel_service_does_not_pass_log_level_to_started_channel(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}

    def fake_start_channel(
        path_to_channel: str,
        startup_info_id: str | None,
        startup_info_writer_sr: str | None,
        name: str | None = None,
        verbose: bool = False,
        host: str | None = None,
        port: str | None = None,
        no_of_channels: int = 1,
        no_of_readers: int = 1,
        no_of_writers: int = 1,
        reader_srts: str | None = None,
        writer_srts: str | None = None,
        buffer_size: int = 1,
        log_level: str | None = None,
    ) -> DummyProcess:
        captured["path_to_channel"] = path_to_channel
        captured["startup_info_id"] = startup_info_id
        captured["startup_info_writer_sr"] = startup_info_writer_sr
        captured["name"] = name
        captured["log_level"] = log_level
        return DummyProcess()

    monkeypatch.setattr(channel_starter_service.channels, "start_channel", fake_start_channel)

    service = channel_starter_service.StartChannelsService(
        con_man=cast("Any", SimpleNamespace()),
        path_to_channel="/tmp/channel",
    )
    service.first_reader = cast("Any", SimpleNamespace())
    service.first_writer_sr = "writer-sr"

    async def fake_get_start_infos(_chan: DummyProcess, _chan_id: str, _no_of_chans: int) -> list[Any]:
        return []

    monkeypatch.setattr(service, "get_start_infos", fake_get_start_infos)

    context = SimpleNamespace(
        params=SimpleNamespace(
            name="demo",
            noOfChannels=1,
            noOfReaders=1,
            noOfWriters=1,
            readerSrts=[],
            writerSrts=[],
            bufferSize=1,
        ),
        results=SimpleNamespace(),
    )

    asyncio.run(service.start_context(cast("Any", context)))

    assert captured["path_to_channel"] == "/tmp/channel"
    assert captured["startup_info_writer_sr"] == "writer-sr"
    assert captured["name"] == "demo"
    assert captured["log_level"] is None
