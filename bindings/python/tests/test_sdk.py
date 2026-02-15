import pytest
from pydantic import BaseModel, Field

from roam_sdk.client import RoamClient
from roam_sdk.decorators import wrap_tool_call
from roam_sdk.testing import TestClient


# Define a Pydantic model for the agent's input
class SearchInput(BaseModel):
    query: str = Field(..., description="The search query")
    limit: int = Field(10, description="Max results")


# Define a Pydantic model for the output
class SearchResult(BaseModel):
    title: str
    url: str


class SearchResponse(BaseModel):
    results: list[SearchResult]


# Mock client for testing without a real server
# Use the official TestClient from roam_sdk.testing


@pytest.fixture
def client():
    c = TestClient()
    c.connect()
    return c


def test_client_init(client):
    assert client.connected


def test_pydantic_integration():
    """Test that models can be used with the SDK structure"""
    input_data = SearchInput(query="rust lang", limit=5)
    assert input_data.query == "rust lang"
    assert input_data.limit == 5


def test_decorator_metadata():
    """Test that the decorator attaches OAM metadata"""

    @wrap_tool_call(name="web_search", description="Searches the web")
    def search_tool(query: str) -> str:
        return f"Results for {query}"

    # Verify metadata is attached
    assert hasattr(search_tool, "oam_tool_def")
    tool_def = search_tool.oam_tool_def
    assert tool_def["name"] == "web_search"
    assert tool_def["description"] == "Searches the web"

    # Verify execution still works
    assert search_tool("test") == "Results for test"
