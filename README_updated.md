# Market Insights Server

A real-time commodity tracking system using Apache Spark, OpenAI GPT, and the MCP protocol to generate actionable market insights.

## Overview

This system is designed to provide intelligent, AI-powered insights on any commodity market using real-time data from Reddit, news sources, Yahoo Finance, and now YouTube. It uses Apache Spark for scalable processing, GPT-4 for natural language summaries, and a dynamic configuration system to adapt to any commodity—from lithium to wheat.

## Features

- Real-time data collection from Reddit, News APIs, Yahoo Finance, and YouTube
- Scalable processing using Apache Spark (PySpark 3.5.0)
- Natural language insights powered by GPT-4
- Dynamic configuration generation and subreddit discovery
- Built-in error handling, retries, and async collection
- Flexible support for tracking any commodity type

## Requirements

- Python 3.x
- PySpark 3.5.0
- Spark NLP 4.4.0
- aiohttp
- yfinance
- openai
- beautifulsoup4
- pytube

Install dependencies:

```bash
pip install -r requirements.txt
```

## Usage

```bash
python spark_market_insights_server.py --commodity "nickel"
```

This command will:
- Scrape Reddit posts and YouTube transcripts
- Fetch relevant news articles and price data
- Process all text data using Spark and TF-IDF
- Generate an AI-powered market insight report

## Architecture

1. Data Collection Layer
    - Reddit async scraping
    - News article collection using NewsAPI
    - Yahoo Finance data using yfinance
    - YouTube video transcript scraping with pytube

2. Spark Processing Layer
    - Tokenization
    - Stop word removal
    - TF-IDF vectorization
    - Spark NLP pipelines

3. AI Insight Layer
    - Uses OpenAI GPT-4 to synthesize all collected data

4. Configuration Layer
    - Automatically detects relevant subreddits and YouTube channels
    - Generates optimized search terms per commodity

## Example Output

> Nickel prices rose sharply after Indonesia’s new export ban. Reddit sentiment is bullish, with posts anticipating supply constraints. Market data shows correlated uptick in EV-related equities like NIO and LI. YouTube videos by key market analysts echo the bullish sentiment.

## Troubleshooting

| Problem | Fix |
|--------|-----|
| Spark job hanging | Check memory settings and repartition input |
| API limit errors | Use throttling and API key rotation |
| No Reddit or YouTube results | Update keywords and validate config |
| GPT returns empty | Increase context or use latest finetuned model |

## Roadmap

- Add machine learning models for price forecasting
- Real-time alerting via Slack or email
- Dashboard integration with Smithery.ai
- Modular plugin system for additional commodity types

## Contributing

Pull requests are welcome. Please include test cases or sample commodities.

## License

MIT License