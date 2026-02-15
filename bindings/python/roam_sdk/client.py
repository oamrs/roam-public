from typing import Any, Callable, Optional

import grpc
from pydantic import BaseModel

# Import generated gRPC code relative to this file
# Note: real implementation would need to handle import paths carefully
try:
    from .v1 import service_pb2, service_pb2_grpc  # type: ignore
except ImportError:
    # Use mocks/placeholders if protos are not generated yet
    service_pb2 = None
    service_pb2_grpc = None


class RoamClient:
    """
    Main client for interacting with the ROAM OAM (Object Agent Mapper).
    Wrapper around the generated gRPC client.
    """

    def __init__(self, address: str = "localhost:50051", api_key: Optional[str] = None):
        self.address = address
        self.api_key = api_key
        self.channel = None
        self.stub = None
        self.connected = False

    def connect(self):
        """Establishes gRPC channel."""
        self.channel = grpc.insecure_channel(self.address)
        if service_pb2_grpc:
            self.stub = service_pb2_grpc.AgentServiceStub(self.channel)
        self.connected = True
        return True

    def register_tool(self, tool_def: dict) -> bool:
        """
        Registers a tool definition with the OAM agent.
        """
        if not self.connected:
            self.connect()
        # In a real implementation, this would call a gRPC method
        # e.g. self.stub.RegisterTool(tool_def)
        return True

    def close(self):
        if self.channel:
            self.channel.close()
