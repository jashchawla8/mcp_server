import asyncio
from market_insights_server import MarketInsightsServer

async def test_tools():
    server = MarketInsightsServer()
    
    try:
        # Test entity analysis
        print("Testing entity analysis for 'lithium'...")
        entity_result = await server.analyze_entity("lithium")
        print("\nEntity Analysis Results:")
        print(f"Analysis: {entity_result['analysis']}")
        print(f"Data sources: {entity_result['data_sources']}")
        
        # Test market analysis
        print("\nTesting market analysis for 'electric vehicles'...")
        market_result = await server.analyze_market("electric vehicles")
        print("\nMarket Analysis Results:")
        print(f"Companies analyzed: {market_result['companies_analyzed']}")
        print(f"Visualization saved to: {market_result['visualization_path']}")
    finally:
        # Close the Reddit client
        await server.reddit.close()

if __name__ == "__main__":
    asyncio.run(test_tools()) 