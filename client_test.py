"""
To test the MCP server in Cursor:

1. Start the server in a terminal:
   python market_insights_server.py

2. In Cursor, use the Command Palette (Cmd/Ctrl + Shift + P) and type:
   'MCP: Connect to Server'

3. Then you can use the server's tools through Cursor's interface:

Example commands to try in Cursor's Command Palette:
- analyze_entity Tesla
- analyze_market "artificial intelligence" --timeframe "1mo"
- analyze_market cryptocurrency --timeframe "3mo"

The results will be displayed in Cursor's output panel, and any generated visualizations
will be saved as HTML files in the current directory.
"""

from mcp.client import client
import asyncio
from pprint import pprint

async def main():
    # Connect to the MCP server
    mcp_client = await client.connect()
    
    print("Connected to MCP server. Testing tools...\n")
    
    # Test 1: Analyze Tesla as an entity
    print("Test 1: Analyzing Tesla...")
    tesla_analysis = await mcp_client.call("analyze_entity", {"entity": "Tesla"})
    print("\nEntity Analysis Results:")
    print(tesla_analysis['analysis'])
    print(f"Data sources: {tesla_analysis['data_sources']}\n")
    
    # Test 2: Analyze market data for AI companies
    print("\nTest 2: Analyzing AI market...")
    ai_market = await mcp_client.call("analyze_market", {"entity": "artificial intelligence", "timeframe": "1mo"})
    print("\nMarket Analysis Results:")
    print(f"Companies analyzed: {ai_market['companies_analyzed']}")
    print(f"Visualization saved to: {ai_market['visualization_path']}\n")
    
    # Test 3: Analyze cryptocurrency market
    print("\nTest 3: Analyzing cryptocurrency market...")
    crypto_market = await mcp_client.call("analyze_market", {"entity": "cryptocurrency", "timeframe": "3mo"})
    print("\nMarket Analysis Results:")
    print(f"Companies analyzed: {crypto_market['companies_analyzed']}")
    print(f"Visualization saved to: {crypto_market['visualization_path']}")

    await mcp_client.close()

if __name__ == "__main__":
    asyncio.run(main()) 