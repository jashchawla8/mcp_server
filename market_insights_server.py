import asyncio
from mcp.server.fastmcp import FastMCP
import logging
from typing import Dict, Any
import aiohttp
from newsapi import NewsApiClient
import yfinance as yf
import pandas as pd
from openai import OpenAI
import os
from dotenv import load_dotenv
import json

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class MarketInsightsServer:
    def __init__(self):
        self.name = "market_insights_server"
        logger.info("Initializing Market Insights Server...")
        
        # Store API credentials
        self.reddit_credentials = {
            'client_id': os.getenv('REDDIT_CLIENT_ID'),
            'client_secret': os.getenv('REDDIT_CLIENT_SECRET'),
            'user_agent': os.getenv('REDDIT_USER_AGENT')
        }
        self.news_api = NewsApiClient(api_key=os.getenv('NEWS_API_KEY'))
        self.openai_client = OpenAI()
        
        # Define search configurations for different entities
        self.search_config = {
            'iron': {
                'subreddits': [
                    'stocks', 'investing', 'commodities', 'wallstreetbets',
                    'mining', 'steel', 'economy', 'metalworking'
                ],
                'search_terms': [
                    'iron ore price', 'steel price', 'iron mining',
                    'iron market', 'steel market', 'iron ore demand',
                    'VALE earnings', 'BHP iron', 'RIO iron'
                ],
                'companies': ['VALE', 'BHP', 'RIO', 'MT', 'CLF'],
                'keywords': ['iron', 'steel', 'ore']
            },
            'electric vehicles': {
                'subreddits': [
                    'electricvehicles', 'teslamotors', 'stocks', 'investing',
                    'cars', 'technology', 'futurology', 'wallstreetbets'
                ],
                'search_terms': [
                    'EV market', 'electric vehicle sales', 'EV battery',
                    'Tesla earnings', 'NIO earnings', 'Rivian production',
                    'Lucid delivery', 'EV charging', 'EV adoption'
                ],
                'companies': ['TSLA', 'NIO', 'RIVN', 'LCID', 'GM', 'F', 'XPEV', 'LI'],
                'keywords': ['ev', 'electric vehicle', 'tesla', 'charging', 'battery']
            },
            'ev commodities': {
                'subreddits': [
                    'electricvehicles', 'batteries', 'commodities', 'investing',
                    'stocks', 'mining', 'RareEarthElements', 'energy',
                    'sustainability', 'Lithium', 'teslamotors'
                ],
                'search_terms': [
                    'lithium supply', 'battery materials', 'nickel mining',
                    'cobalt shortage', 'rare earth ev', 'graphite battery',
                    'copper ev demand', 'battery metals', 'lithium mining',
                    'ev supply chain', 'battery shortage', 'battery production'
                ],
                'companies': [
                    'ALB',  # Albemarle (Lithium)
                    'SQM',  # Sociedad Química y Minera (Lithium)
                    'LTHM', # Livent (Lithium)
                    'LAC',  # Lithium Americas
                    'PLL',  # Piedmont Lithium
                    'VALE', # Vale (Nickel)
                    'FCX',  # Freeport-McMoRan (Copper)
                    'MP',   # MP Materials (Rare Earth)
                    'GNENF', # Ganfeng Lithium
                    'BHP'   # BHP Group (Diversified Mining)
                ],
                'keywords': [
                    'lithium', 'nickel', 'copper', 'cobalt', 'graphite',
                    'rare earth', 'battery materials', 'battery metals',
                    'battery supply', 'mining', 'battery production',
                    'battery shortage', 'supply chain'
                ]
            }
        }
        
        logger.info("APIs initialized successfully")

    async def get_reddit_data(self, entity: str, timeframe: str = "week") -> list:
        """Helper function to get Reddit data using aiohttp directly."""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        async with aiohttp.ClientSession() as session:
            reddit_data = []
            
            # Get configuration for the entity
            config = self.search_config.get(entity.lower(), {
                'subreddits': ['stocks', 'investing', 'news', 'worldnews'],
                'search_terms': [entity],
                'companies': [],
                'keywords': [entity.lower()]
            })
            
            # Search in each subreddit
            for subreddit in config['subreddits']:
                # Search for each term
                for term in config['search_terms']:
                    url = f"https://www.reddit.com/r/{subreddit}/search.json"
                    params = {
                        'q': term,
                        't': timeframe,
                        'limit': 10,
                        'sort': 'relevance',
                        'restrict_sr': 'on'
                    }
                    
                    try:
                        async with session.get(url, headers=headers, params=params) as response:
                            if response.status == 200:
                                data = await response.json()
                                for post in data.get('data', {}).get('children', []):
                                    post_data = post['data']
                                    title = post_data.get('title', '').lower()
                                    
                                    # Check if post is relevant
                                    is_relevant = False
                                    for keyword in config['keywords']:
                                        if keyword in title:
                                            is_relevant = True
                                            break
                                    for company in config['companies']:
                                        if company in title.upper():
                                            is_relevant = True
                                            break
                                            
                                    if is_relevant:
                                        reddit_data.append({
                                            'title': post_data.get('title'),
                                            'score': post_data.get('score', 0),
                                            'num_comments': post_data.get('num_comments', 0),
                                            'created_utc': post_data.get('created_utc', 0),
                                            'subreddit': post_data.get('subreddit', subreddit),
                                            'search_term': term
                                        })
                    except Exception as e:
                        logger.error(f"Error fetching Reddit data for r/{subreddit} with term '{term}': {str(e)}")
                        continue
            
            # Remove duplicates and sort by engagement
            seen_titles = set()
            filtered_data = []
            for post in sorted(reddit_data, key=lambda x: (x['score'] + x['num_comments']), reverse=True):
                if post['title'] not in seen_titles:
                    seen_titles.add(post['title'])
                    filtered_data.append(post)
            
            return filtered_data[:10]  # Return top 10 most relevant and engaging posts

    async def analyze_entity(self, entity: str, timeframe: str = "1w") -> dict:
        """
        Analyze sentiment and trends for an entity from Reddit and news sources.
        """
        logger.info(f"Analyzing entity: {entity} with timeframe: {timeframe}")
        try:
            # Map timeframe to Reddit format
            reddit_timeframe = {
                '1d': 'day',
                '1w': 'week',
                '1mo': 'month',
                '3mo': 'year',
                '1y': 'year'
            }.get(timeframe, 'week')
            
            # Collect Reddit data using direct API calls
            reddit_data = await self.get_reddit_data(entity, reddit_timeframe)
            
            # Initialize news data
            news_articles = []
            
            # Try to collect news data, but continue if it fails
            try:
                # For iron, use more specific search terms
                if entity.lower() == 'iron':
                    news_query = '(iron ore OR steel) AND (price OR market OR demand OR mining)'
                else:
                    news_query = entity
                    
                news_data = self.news_api.get_everything(
                    q=news_query,
                    language='en',
                    sort_by='relevancy',
                    page_size=5
                )
                news_articles = news_data.get('articles', [])
            except Exception as e:
                logger.error(f"Error fetching news data: {str(e)}")
                logger.info("Continuing with Reddit data only")

            # Format data for analysis
            formatted_posts = []
            if reddit_data:
                for post in reddit_data:
                    formatted_posts.append(
                        f"- [{post['subreddit']}] {post['title']} "
                        f"(Score: {post['score']}, Comments: {post['num_comments']}, "
                        f"Search term: {post['search_term']})"
                    )
            formatted_reddit = "\n".join(formatted_posts) if formatted_posts else "No Reddit data available"
            
            formatted_news = "\n".join([f"- {article['title']}: {article['description']}" 
                                      for article in news_articles]) if news_articles else "No news data available"

            if not reddit_data and not news_articles:
                return {
                    'entity': entity,
                    'error': "No data available from either Reddit or News API",
                    'data_sources': {
                        'reddit_posts': 0,
                        'news_articles': 0
                    }
                }

            # Analyze sentiment using GPT
            analysis_prompt = f"""
            Analyze the following data about {entity}:
            
            Reddit Posts:
            {formatted_reddit}
            
            News Articles:
            {formatted_news}
            
            Provide a concise analysis with:
            1. Overall sentiment (positive/negative/neutral)
            2. Key trends and patterns
            3. Main concerns or opportunities
            4. Market outlook and price trends (if available)
            5. Confidence level in the analysis
            
            Note: Some data sources might be unavailable. Focus on analyzing the available data.
            If limited data is available, acknowledge this in your confidence level.
            For iron specifically, consider both iron ore prices and steel market conditions.
            Consider the credibility and relevance of each source when forming your analysis.
            """

            try:
                response = self.openai_client.chat.completions.create(
                    model="gpt-4",
                    messages=[{"role": "user", "content": analysis_prompt}]
                )
                analysis = response.choices[0].message.content
            except Exception as e:
                logger.error(f"Error in OpenAI analysis: {str(e)}")
                analysis = "Error performing sentiment analysis. Raw data is provided below:\n\n" + \
                          "Reddit Posts:\n" + formatted_reddit + "\n\n" + \
                          "News Articles:\n" + formatted_news

            return {
                'entity': entity,
                'analysis': analysis,
                'data_sources': {
                    'reddit_posts': len(reddit_data),
                    'news_articles': len(news_articles)
                }
            }
        except Exception as e:
            logger.error(f"Error in analyze_entity: {str(e)}")
            return {
                'entity': entity,
                'error': f"Analysis failed: {str(e)}",
                'data_sources': {
                    'reddit_posts': 0,
                    'news_articles': 0
                }
            }

    async def analyze_market(self, entity: str, timeframe: str = "1mo") -> dict:
        """
        Analyze stock market data for companies related to an entity.
        """
        logger.info(f"Analyzing market for: {entity} with timeframe: {timeframe}")
        try:
            # Explicitly define symbols based on entity
            if entity.lower() == 'electric vehicles':
                stock_symbols = ['TSLA', 'NIO', 'RIVN', 'LCID', 'GM', 'F', 'XPEV', 'LI']
            elif entity.lower() == 'iron':
                stock_symbols = ['VALE', 'BHP', 'RIO', 'MT', 'CLF']
            else:
                stock_symbols = ['SPY']  # Default to SPY if no specific symbols
                
            logger.info(f"Using symbols for {entity}: {stock_symbols}")
            
            # Collect stock data
            market_analysis = []
            market_analysis.append(f"Market Analysis for {entity} over {timeframe}:")
            market_analysis.append("=" * 50)
            market_analysis.append(f"\nAnalyzing major {entity} companies: {', '.join(stock_symbols)}\n")
            
            successful_analyses = 0
            
            # Add delay between requests to avoid rate limiting
            async def get_stock_data(symbol):
                await asyncio.sleep(1)  # Add delay between requests
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
                
                try:
                    async with aiohttp.ClientSession() as session:
                        url = f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}'
                        async with session.get(url, headers=headers) as response:
                            if response.status == 200:
                                data = await response.json()
                                
                                # Extract basic info
                                meta = data['chart']['result'][0]['meta']
                                quote = data['chart']['result'][0]['indicators']['quote'][0]
                                
                                current_price = meta.get('regularMarketPrice', 0)
                                previous_close = meta.get('previousClose', 0)
                                company_name = symbol  # Simplified for now
                                
                                if current_price and previous_close:
                                    change_pct = ((current_price - previous_close) / previous_close * 100)
                                    
                                    result = []
                                    result.append(f"\n{company_name}:")
                                    result.append(f"Current Price: ${current_price:.2f}")
                                    result.append(f"Previous Close: ${previous_close:.2f}")
                                    result.append(f"Change: {change_pct:.1f}%")
                                    
                                    # Add volume if available
                                    if 'volume' in quote and quote['volume']:
                                        volume = quote['volume'][-1]
                                        if volume:
                                            result.append(f"Volume: {int(volume):,}")
                                    
                                    result.append("-" * 30)
                                    return True, result
                                else:
                                    return False, [f"\nError analyzing {symbol}: Missing price data"]
                            else:
                                return False, [f"\nError analyzing {symbol}: HTTP {response.status}"]
                except Exception as e:
                    logger.error(f"Error analyzing {symbol}: {str(e)}")
                    return False, [f"\nError analyzing {symbol}: {str(e)}"]
            
            # Process stocks concurrently with rate limiting
            tasks = []
            for symbol in stock_symbols:
                tasks.append(get_stock_data(symbol))
            
            results = await asyncio.gather(*tasks)
            
            for success, lines in results:
                if success:
                    successful_analyses += 1
                market_analysis.extend(lines)

            if successful_analyses == 0:
                return {
                    'entity': entity,
                    'timeframe': timeframe,
                    'error': f"Could not retrieve market data for any of the symbols: {', '.join(stock_symbols)}",
                    'data_available': False
                }

            # Add summary section
            market_analysis.append("\nSummary:")
            market_analysis.append("=" * 50)
            market_analysis.append(f"Successfully analyzed {successful_analyses} out of {len(stock_symbols)} companies.")
            market_analysis.append("\nNote: This data represents real-time market information.")

            return {
                'entity': entity,
                'timeframe': timeframe,
                'analysis': "\n".join(market_analysis),
                'data_available': True
            }
        except Exception as e:
            logger.error(f"Error in analyze_market: {str(e)}")
            return {
                'entity': entity,
                'error': f"Market analysis failed: {str(e)}",
                'data_available': False
            }

# Create MCP server instance
mcp = FastMCP("Market Insights")

# Create market insights instance
market_insights = MarketInsightsServer()

@mcp.tool(name="analyze_entity", description="Analyze sentiment and trends for an entity from Reddit and news sources")
async def analyze_entity(entity: str, timeframe: str = "1w") -> dict:
    """Analyze sentiment and trends for an entity from Reddit and news sources.
    
    Args:
        entity: The entity to analyze (e.g., 'iron market', 'tesla')
        timeframe: Timeframe for analysis (e.g., '1w', '1mo')
    """
    return await market_insights.analyze_entity(entity, timeframe)

@mcp.tool(name="analyze_market", description="Analyze stock market data for companies related to an entity")
async def analyze_market(entity: str, timeframe: str = "1mo") -> dict:
    """Analyze stock market data for companies related to an entity.
    
    Args:
        entity: The entity to analyze (e.g., 'iron', 'tech')
        timeframe: Timeframe for market data (e.g., '1mo', '3mo')
    """
    return await market_insights.analyze_market(entity, timeframe)

if __name__ == "__main__":
    logger.info("Starting Market Insights Server...")
    mcp.run() 