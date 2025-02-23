"""Entry point: python -m mcp_server"""
from mcp_server.server import MCPServer

if __name__ == "__main__":
    server = MCPServer()
    server.run()
