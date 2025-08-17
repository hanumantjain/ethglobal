# mcp_main.py
from mcp_app import mcp  # MCP app
# Importing tools registers them via decorators
import mcp_tools_core      # noqa: F401
import mcp_tools_wrappers  # noqa: F401

if __name__ == "__main__":
    try:
        mcp.run(host="0.0.0.0", port=8000, transport="http")
    except TypeError:
        from fastmcp import Transport
        mcp.run(transport=Transport.HTTP, host="0.0.0.0", port=8000)
