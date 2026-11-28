import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from astrbot.core import initial_loader


@pytest.mark.asyncio
@pytest.mark.parametrize("failed_task", ["core", "dashboard"])
@pytest.mark.parametrize("cleanup", ["graceful", "dispose-fails", "peer-timeout"])
async def test_runtime_failure_shuts_down_sibling_and_disposes_database(
    monkeypatch: pytest.MonkeyPatch, failed_task: str, cleanup: str
):
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(
        loop, "add_signal_handler", MagicMock(side_effect=NotImplementedError)
    )
    peer_started = asyncio.Event()
    peer_done = asyncio.Event()
    shutdown = asyncio.Event()
    release_peer = asyncio.Event()
    order = []
    original_error = RuntimeError(f"{failed_task} failed")
    if cleanup == "peer-timeout":
        monkeypatch.setattr(initial_loader, "DASHBOARD_SHUTDOWN_TIMEOUT", 0)

    async def fail():
        await peer_started.wait()
        raise original_error

    async def peer():
        peer_started.set()
        try:
            await shutdown.wait()
            if cleanup == "peer-timeout":
                await release_peer.wait()
            await asyncio.sleep(0)
            order.append("peer closed")
        finally:
            peer_done.set()

    async def stop():
        order.append("stop")

    async def dispose():
        order.append("dispose")
        if cleanup == "dispose-fails":
            raise RuntimeError("database disposal failed")

    lifecycle = MagicMock()
    lifecycle.initialize = AsyncMock()
    lifecycle.start.side_effect = fail if failed_task == "core" else peer
    lifecycle.stop = AsyncMock(side_effect=stop)
    lifecycle.dashboard_shutdown_event = shutdown
    dashboard = MagicMock()
    dashboard.run.side_effect = fail if failed_task == "dashboard" else peer
    monkeypatch.setattr(
        initial_loader, "AstrBotCoreLifecycle", MagicMock(return_value=lifecycle)
    )
    monkeypatch.setattr(
        initial_loader, "AstrBotDashboard", MagicMock(return_value=dashboard)
    )
    db = MagicMock()
    db.engine.dispose = AsyncMock(side_effect=dispose)
    loader = initial_loader.InitialLoader(db, MagicMock())

    try:
        with pytest.raises(RuntimeError, match=f"{failed_task} failed") as caught:
            await loader.start()
        assert caught.value is original_error
        if cleanup == "peer-timeout":
            assert order == ["stop", "dispose"]
            await asyncio.wait_for(peer_done.wait(), timeout=1)
        else:
            assert order == ["stop", "peer closed", "dispose"]
    finally:
        shutdown.set()
        release_peer.set()
        await asyncio.wait_for(peer_done.wait(), timeout=1)
