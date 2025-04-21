# Market Insights Server

A real-time commodity tracking system using Apache Spark, OpenAI GPT, and the MCP protocol to generate actionable market insights.

---
![image](https://github.com/user-attachments/assets/c5025e4d-bd10-472d-a1f1-cb517495d68b)


## Features

- Real-time data collection from Reddit, News APIs, and Yahoo Finance
- Scalable processing using Apache Spark (PySpark 3.5.0)
- Natural language insights powered by GPT-4
- Configurable for *any* commodity market: energy, metals, agriculture, and more
- Built-in dynamic configuration generation and subreddit discovery
- Ready for deployment with error handling, retries, and async collection

---

## Requirements

- Python 3.x
- PySpark 3.5.0
- Spark NLP 4.4.0
- aiohttp
- yfinance
- openai
- beautifulsoup4

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## Usage

```bash
python spark_market_insights_server.py --commodity "nickel"
```

Outputs:
- Cleaned text data from Reddit and news sources
- TF-IDF features
- GPT-4-powered insight report
- JSON export of insights

---

## Architecture

1. **Data Collection Layer**
    - Async scraping of Reddit and news articles
    - Yahoo Finance for live price feeds

2. **Processing Layer (Apache Spark)**
    - Tokenization → Stop words removal → TF-IDF vectorization
    - Supports Spark NLP pipelines

3. **AI Insight Layer**
    - Uses OpenAI GPT-4 to summarize and synthesize market narratives

4. **Configuration Layer**
    - Automatically identifies relevant subreddits and keywords per commodity

---

## 🔍 Example Output

> “Nickel prices rose sharply after Indonesia’s new export ban. Reddit sentiment is bullish, with posts anticipating supply constraints. Market data shows correlated uptick in EV-related equities like NIO and LI.”

---

## 🛠 Troubleshooting

| Problem | Fix |
|--------|-----|
| Spark stage stuck | Check memory settings, repartition input |
| API returns 429 | Add backoff/retry logic, rotate API keys |
| GPT returns empty | Use latest models, tune prompt |

---

## Coming Soon

- Youtube videos to be analyzed as well for the commodities
- Public dashboard on [Smithery.ai](https://smithery.ai)

---

## Contributing

Have a new data source or insight model? PRs welcome!

---

## License

MIT License
