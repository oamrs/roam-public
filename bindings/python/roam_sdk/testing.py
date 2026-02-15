from typing import Optional

from .client import RoamClient


class TestClient(RoamClient):
    """
    A test implementation of the RoamClient.
    This client simulates a connected state and can be used to test
    interactions without a running OAM system, similar to FastAPI's TestClient.
    """

    __test__ = False

    def __init__(self, agent_id: str = "mock-agent"):
        self.agent_id = agent_id
        self.connected = False
        self._events: list[dict] = []

    def connect(self) -> bool:
        """Simulates a successful connection."""
        self.connected = True
        return True

    def register_tool(self, tool_def: dict):
        """Mock tool registration."""
        print(f"Mock: Registered tool {tool_def.get('name')}")
        return True

    def emit_event(self, event_type: str, payload: dict):
        """Store emitted events for assertion."""
        self._events.append({"type": event_type, "payload": payload})
