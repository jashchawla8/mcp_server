import os
import logging
import aiohttp
import yfinance as yf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, array_contains, to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, ArrayType
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from datetime import datetime, timedelta
from dotenv import load_dotenv
from newsapi import NewsApiClient
from openai import OpenAI
from googleapiclient.discovery import build
from youtube_transcript_api import YouTubeTranscriptApi
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SparkMarketInsightsServer:
    def __init__(self):
        self.name = "spark_market_insights_server"
        logger.info("Initializing Spark Market Insights Server...")
        
        # Load environment variables
        load_dotenv()
        
        # Initialize API clients
        self.reddit_credentials = {
            'client_id': os.getenv('REDDIT_CLIENT_ID'),
            'client_secret': os.getenv('REDDIT_CLIENT_SECRET'),
            'user_agent': os.getenv('REDDIT_USER_AGENT')
        }
        self.news_api = NewsApiClient(api_key=os.getenv('NEWS_API_KEY'))
        self.openai_client = OpenAI()
        
        # Initialize YouTube API client
        self.youtube = build('youtube', 'v3', developerKey=os.getenv('YOUTUBE_API_KEY'))
        
        # Initialize Spark session
        self.spark = SparkSession.builder \
            .appName("MarketInsights") \
            .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:4.4.0") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .getOrCreate()
        
        # Define schema for Reddit data
        self.reddit_schema = StructType([
            StructField("id", StringType(), True),
            StructField("title", StringType(), True),
            StructField("subreddit", StringType(), True),
            StructField("score", IntegerType(), True),
            StructField("num_comments", IntegerType(), True),
            StructField("created_utc", TimestampType(), True),
            StructField("selftext", StringType(), True),
            StructField("search_term", StringType(), True)
        ])
        
        # Define schema for YouTube data
        self.youtube_schema = StructType([
            StructField("video_id", StringType(), True),
            StructField("title", StringType(), True),
            StructField("channel_title", StringType(), True),
            StructField("published_at", TimestampType(), True),
            StructField("view_count", IntegerType(), True),
            StructField("transcript_text", StringType(), True),
            StructField("search_term", StringType(), True)
        ])
        
        # Define base search configurations
        self.search_config = {
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
                    'ALB', 'SQM', 'LTHM', 'LAC', 'PLL', 'VALE',
                    'FCX', 'MP', 'GNENF', 'BHP'
                ],
                'keywords': [
                    'lithium', 'nickel', 'copper', 'cobalt', 'graphite',
                    'rare earth', 'battery materials', 'battery metals',
                    'battery supply', 'mining', 'battery production',
                    'battery shortage', 'supply chain'
                ]
            },
            'precious metals': {
                'subreddits': [
                    'commodities', 'investing', 'stocks', 'gold', 'silverbugs',
                    'mining', 'economy', 'wallstreetsilver', 'preciousmetals'
                ],
                'search_terms': [
                    'gold price', 'silver market', 'platinum demand',
                    'palladium supply', 'precious metals outlook',
                    'mining production', 'metal shortage', 'bullion market'
                ],
                'companies': [
                    'NEM', 'GOLD', 'FNV', 'WPM', 'RGLD', 'PAAS',
                    'AG', 'HL', 'CDE', 'SSRM'
                ],
                'keywords': [
                    'gold', 'silver', 'platinum', 'palladium', 'precious metals',
                    'mining', 'bullion', 'spot price', 'metal production'
                ]
            },
            'energy commodities': {
                'subreddits': [
                    'energy', 'oil', 'commodities', 'investing', 'stocks',
                    'naturalgas', 'oilandgasworkers', 'renewableenergy'
                ],
                'search_terms': [
                    'oil price', 'natural gas demand', 'energy market',
                    'crude supply', 'gas storage', 'oil production',
                    'energy outlook', 'renewable impact'
                ],
                'companies': [
                    'XOM', 'CVX', 'COP', 'BP', 'SHEL', 'TTE',
                    'EOG', 'PXD', 'OXY', 'SLB'
                ],
                'keywords': [
                    'oil', 'natural gas', 'crude', 'energy', 'petroleum',
                    'production', 'reserves', 'drilling', 'exploration'
                ]
            }
        }
        
        logger.info("Spark session and APIs initialized successfully")

    def generate_commodity_config(self, commodity: str) -> dict:
        """Dynamically generate search configuration for any commodity."""
        # Base subreddits for any commodity
        base_subreddits = [
            'commodities', 'investing', 'stocks', 'economy',
            'trading', 'finance', 'wallstreetbets'
        ]
        
        # Generate search terms
        search_terms = [
            f"{commodity} price",
            f"{commodity} market",
            f"{commodity} supply",
            f"{commodity} demand",
            f"{commodity} shortage",
            f"{commodity} production",
            f"{commodity} outlook",
            f"{commodity} trading"
        ]
        
        # Add the commodity name as a subreddit if it exists
        if commodity.replace(" ", "").isalnum():
            base_subreddits.append(commodity.replace(" ", ""))
        
        return {
            'subreddits': base_subreddits,
            'search_terms': search_terms,
            'companies': ['SPY'],  # Default to SPY, can be updated manually
            'keywords': [commodity, 'market', 'price', 'supply', 'demand', 'trading']
        }

    async def get_reddit_data(self, entity: str, timeframe: str = "week") -> list:
        """Helper function to get Reddit data using aiohttp directly."""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        async with aiohttp.ClientSession() as session:
            reddit_data = []
            
            # Get configuration for the entity or generate one
            config = self.search_config.get(entity.lower(),
                self.generate_commodity_config(entity)
            )
            
            for subreddit in config['subreddits']:
                for search_term in config['search_terms']:
                    url = f"https://www.reddit.com/r/{subreddit}/search.json"
                    params = {
                        'q': search_term,
                        't': timeframe,
                        'sort': 'relevance',
                        'limit': 100
                    }
                    
                    try:
                        async with session.get(url, headers=headers, params=params) as response:
                            if response.status == 200:
                                data = await response.json()
                                posts = data.get('data', {}).get('children', [])
                                
                                for post in posts:
                                    post_data = post['data']
                                    reddit_data.append({
                                        'id': post_data['id'],
                                        'title': post_data['title'],
                                        'subreddit': post_data['subreddit'],
                                        'score': post_data['score'],
                                        'num_comments': post_data['num_comments'],
                                        'created_utc': datetime.fromtimestamp(post_data['created_utc']),
                                        'selftext': post_data.get('selftext', ''),
                                        'search_term': search_term
                                    })
                    except Exception as e:
                        logger.error(f"Error fetching Reddit data: {e}")
                        continue
            
            return reddit_data

    def process_reddit_data(self, reddit_data: list) -> dict:
        """Process Reddit data using Spark for sentiment analysis and trend detection."""
        if not reddit_data:
            return {"error": "No Reddit data available"}
        
        # Convert list to Spark DataFrame
        reddit_df = self.spark.createDataFrame(reddit_data, self.reddit_schema)
        
        # Text processing pipeline
        tokenizer = Tokenizer(inputCol="title", outputCol="words")
        remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
        cv = CountVectorizer(inputCol="filtered_words", outputCol="raw_features")
        idf = IDF(inputCol="raw_features", outputCol="features")
        
        pipeline = Pipeline(stages=[tokenizer, remover, cv, idf])
        model = pipeline.fit(reddit_df)
        processed_df = model.transform(reddit_df)
        
        # Analyze engagement and trends
        top_posts = processed_df.orderBy(col("score").desc()).limit(10)
        
        # Convert to format suitable for analysis
        analysis_data = {
            "top_posts": [
                {
                    "subreddit": row.subreddit,
                    "title": row.title,
                    "score": row.score,
                    "comments": row.num_comments,
                    "search_term": row.search_term
                }
                for row in top_posts.collect()
            ]
        }
        
        return analysis_data

    async def get_news_data(self, entity: str) -> list:
        """Get news articles related to the entity."""
        try:
            news_response = self.news_api.get_everything(
                q=entity,
                language='en',
                sort_by='relevancy',
                page_size=100
            )
            return news_response.get('articles', [])
        except Exception as e:
            logger.error(f"Error fetching news data: {e}")
            return []

    def process_news_data(self, news_data: list) -> dict:
        """Process news data using Spark for sentiment analysis and trend detection."""
        if not news_data:
            return {"error": "No news data available"}
        
        # Convert news data to Spark DataFrame
        news_schema = StructType([
            StructField("title", StringType(), True),
            StructField("description", StringType(), True),
            StructField("publishedAt", TimestampType(), True),
            StructField("source", StringType(), True)
        ])
        
        news_records = [
            {
                "title": article.get("title", ""),
                "description": article.get("description", ""),
                "publishedAt": datetime.strptime(article.get("publishedAt", ""), "%Y-%m-%dT%H:%M:%SZ"),
                "source": article.get("source", {}).get("name", "")
            }
            for article in news_data
        ]
        
        news_df = self.spark.createDataFrame(news_records, news_schema)
        
        # Process and analyze news data
        tokenizer = Tokenizer(inputCol="title", outputCol="words")
        remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
        cv = CountVectorizer(inputCol="filtered_words", outputCol="raw_features")
        idf = IDF(inputCol="raw_features", outputCol="features")
        
        pipeline = Pipeline(stages=[tokenizer, remover, cv, idf])
        model = pipeline.fit(news_df)
        processed_df = model.transform(news_df)
        
        # Get recent and relevant news
        recent_news = processed_df.orderBy(col("publishedAt").desc()).limit(5)
        
        # Convert to format suitable for analysis
        analysis_data = {
            "recent_news": [
                {
                    "title": row.title,
                    "description": row.description,
                    "source": row.source,
                    "publishedAt": row.publishedAt.strftime("%Y-%m-%d %H:%M:%S")
                }
                for row in recent_news.collect()
            ]
        }
        
        return analysis_data

    async def get_youtube_data(self, entity: str, timeframe: str = "week") -> list:
        """Get YouTube videos and their transcripts related to the entity."""
        try:
            # Calculate the published after date based on timeframe
            published_after = {
                "day": datetime.now() - timedelta(days=1),
                "week": datetime.now() - timedelta(weeks=1),
                "month": datetime.now() - timedelta(days=30),
                "year": datetime.now() - timedelta(days=365)
            }.get(timeframe.lower().replace("1", ""), datetime.now() - timedelta(weeks=1))

            # Search for videos
            search_response = self.youtube.search().list(
                q=entity,
                part='id,snippet',
                maxResults=10,
                type='video',
                relevanceLanguage='en',
                publishedAfter=published_after.isoformat() + 'Z',
                order='relevance'
            ).execute()

            youtube_data = []
            
            for item in search_response.get('items', []):
                video_id = item['id']['videoId']
                
                try:
                    # Get video statistics
                    video_response = self.youtube.videos().list(
                        part='statistics',
                        id=video_id
                    ).execute()
                    
                    # Get video transcript
                    try:
                        transcript_list = YouTubeTranscriptApi.get_transcript(video_id)
                        transcript_text = ' '.join([entry['text'] for entry in transcript_list])
                    except Exception as e:
                        logger.warning(f"Could not get transcript for video {video_id}: {e}")
                        transcript_text = ""

                    # Combine all data
                    video_data = {
                        'video_id': video_id,
                        'title': item['snippet']['title'],
                        'channel_title': item['snippet']['channelTitle'],
                        'published_at': datetime.strptime(
                            item['snippet']['publishedAt'],
                            '%Y-%m-%dT%H:%M:%SZ'
                        ),
                        'view_count': int(video_response['items'][0]['statistics'].get('viewCount', 0)),
                        'transcript_text': transcript_text,
                        'search_term': entity
                    }
                    
                    youtube_data.append(video_data)
                    
                except Exception as e:
                    logger.error(f"Error processing video {video_id}: {e}")
                    continue

            return youtube_data
            
        except Exception as e:
            logger.error(f"Error fetching YouTube data: {e}")
            return []

    def process_youtube_data(self, youtube_data: list) -> dict:
        """Process YouTube data using Spark for sentiment analysis and trend detection."""
        if not youtube_data:
            return {"error": "No YouTube data available"}
        
        # Convert list to Spark DataFrame
        youtube_df = self.spark.createDataFrame(youtube_data, self.youtube_schema)
        
        # Text processing pipeline for transcripts
        tokenizer = Tokenizer(inputCol="transcript_text", outputCol="words")
        remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
        cv = CountVectorizer(inputCol="filtered_words", outputCol="raw_features")
        idf = IDF(inputCol="raw_features", outputCol="features")
        
        pipeline = Pipeline(stages=[tokenizer, remover, cv, idf])
        model = pipeline.fit(youtube_df)
        processed_df = model.transform(youtube_df)
        
        # Get top videos by views
        top_videos = processed_df.orderBy(col("view_count").desc()).limit(5)
        
        # Convert to format suitable for analysis
        analysis_data = {
            "top_videos": [
                {
                    "title": row.title,
                    "channel": row.channel_title,
                    "views": row.view_count,
                    "published_at": row.published_at.strftime("%Y-%m-%d %H:%M:%S"),
                    "video_id": row.video_id,
                    "transcript_summary": row.transcript_text[:500] + "..." if row.transcript_text else "No transcript available"
                }
                for row in top_videos.collect()
            ]
        }
        
        return analysis_data

    async def analyze_entity(self, entity: str, timeframe: str = "1w") -> dict:
        """Analyze sentiment and trends for an entity from Reddit, news sources, and YouTube."""
        try:
            # Gather data from all sources
            reddit_data = await self.get_reddit_data(entity, timeframe)
            news_data = await self.get_news_data(entity)
            youtube_data = await self.get_youtube_data(entity, timeframe)
            
            # Process data using Spark
            reddit_analysis = self.process_reddit_data(reddit_data)
            news_analysis = self.process_news_data(news_data)
            youtube_analysis = self.process_youtube_data(youtube_data)
            
            # Combine analyses
            combined_data = {
                "reddit_posts": len(reddit_data),
                "news_articles": len(news_data),
                "youtube_videos": len(youtube_data),
                "reddit_analysis": reddit_analysis,
                "news_analysis": news_analysis,
                "youtube_analysis": youtube_analysis
            }
            
            # Generate summary using OpenAI
            prompt = f"""
            Analyze the following market data for {entity}:
            
            Reddit Discussion:
            {json.dumps(reddit_analysis.get('top_posts', []), indent=2)}
            
            Recent News:
            {json.dumps(news_analysis.get('recent_news', []), indent=2)}
            
            YouTube Content:
            {json.dumps(youtube_analysis.get('top_videos', []), indent=2)}
            
            Provide a comprehensive analysis including:
            1. Overall Sentiment
            2. Key Trends and Patterns
            3. Main Concerns or Opportunities
            4. Confidence Level in the Analysis
            
            Consider insights from all three sources (Reddit, News, YouTube) in your analysis.
            Format the response in a clear, structured way.
            """
            
            response = self.openai_client.chat.completions.create(
                model="gpt-4",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.7,
                max_tokens=1000
            )
            
            analysis = response.choices[0].message.content
            
            return {
                "entity": entity,
                "analysis": analysis,
                "data_sources": {
                    "reddit_posts": len(reddit_data),
                    "news_articles": len(news_data),
                    "youtube_videos": len(youtube_data)
                }
            }
            
        except Exception as e:
            logger.error(f"Error in analyze_entity: {e}")
            return {
                "entity": entity,
                "analysis": f"Error performing sentiment analysis. Raw data is provided below:\n\nReddit Posts:\n{self.format_reddit_data(reddit_data)}\n\nNews Articles:\n{self.format_news_data(news_data)}\n\nYouTube Videos:\n{self.format_youtube_data(youtube_data)}",
                "data_sources": {
                    "reddit_posts": len(reddit_data),
                    "news_articles": len(news_data),
                    "youtube_videos": len(youtube_data)
                }
            }

    def format_reddit_data(self, reddit_data: list) -> str:
        """Format Reddit data for error output."""
        formatted_posts = []
        for post in reddit_data:
            formatted_posts.append(
                f"- [{post['subreddit']}] {post['title']} "
                f"(Score: {post['score']}, Comments: {post['num_comments']}, "
                f"Search term: {post['search_term']})"
            )
        return "\n".join(formatted_posts)

    def format_news_data(self, news_data: list) -> str:
        """Format news data for error output."""
        if not news_data:
            return "No news data available"
        formatted_news = []
        for article in news_data:
            formatted_news.append(
                f"- {article.get('title', 'No title')} "
                f"(Source: {article.get('source', {}).get('name', 'Unknown')})"
            )
        return "\n".join(formatted_news)

    def format_youtube_data(self, youtube_data: list) -> str:
        """Format YouTube data for error output."""
        if not youtube_data:
            return "No YouTube data available"
        formatted_videos = []
        for video in youtube_data:
            formatted_videos.append(
                f"- {video.get('title', 'No title')} "
                f"(Channel: {video.get('channel_title', 'Unknown')}, "
                f"Views: {video.get('view_count', 0)})"
            )
        return "\n".join(formatted_videos)

    async def analyze_market(self, entity: str, timeframe: str = "1mo") -> dict:
        """Analyze stock market data for companies related to an entity."""
        logger.info(f"Analyzing market for: {entity} with timeframe: {timeframe}")
        try:
            # Get stock symbols from search config
            config = self.search_config.get(entity.lower(), {})
            stock_symbols = config.get('companies', ['SPY'])  # Default to SPY if no specific symbols
            logger.info(f"Using symbols for {entity}: {stock_symbols}")
            
            # Collect stock data
            market_analysis = []
            market_analysis.append(f"Market Analysis for {entity} over {timeframe}:")
            market_analysis.append("=" * 50)
            market_analysis.append(f"\nAnalyzing major {entity} companies: {', '.join(stock_symbols)}\n")
            
            successful_analyses = 0
            
            for symbol in stock_symbols:
                try:
                    stock = yf.Ticker(symbol)
                    info = stock.info
                    current_price = info.get('regularMarketPrice', 0)
                    previous_close = info.get('regularMarketPreviousClose', 0)
                    
                    if current_price and previous_close:
                        change = ((current_price - previous_close) / previous_close) * 100
                        market_analysis.append(f"\n{symbol}:")
                        market_analysis.append(f"Current Price: ${current_price:.2f}")
                        market_analysis.append(f"Previous Close: ${previous_close:.2f}")
                        market_analysis.append(f"Change: {change:.1f}%")
                        market_analysis.append("-" * 30)
                        successful_analyses += 1
                
                except Exception as e:
                    logger.error(f"Error analyzing {symbol}: {e}")
                    continue
            
            market_analysis.append(f"\nSummary:")
            market_analysis.append("=" * 50)
            market_analysis.append(f"Successfully analyzed {successful_analyses} out of {len(stock_symbols)} companies.")
            market_analysis.append("\nNote: This data represents real-time market information.")
            
            return {
                "entity": entity,
                "timeframe": timeframe,
                "analysis": "\n".join(market_analysis),
                "data_available": successful_analyses > 0
            }
            
        except Exception as e:
            logger.error(f"Error in analyze_market: {e}")
            return {
                "entity": entity,
                "timeframe": timeframe,
                "analysis": f"Error analyzing market data: {str(e)}",
                "data_available": False
            }

    def __del__(self):
        """Cleanup method to stop Spark session."""
        if hasattr(self, 'spark'):
            self.spark.stop()
            logger.info("Spark session stopped") 