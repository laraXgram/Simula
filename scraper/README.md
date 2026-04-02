# Scraper

Python-based scraper for extracting all Telegram Bot API methods and types from official documentation.

## Purpose

This scraper ensures that our API implementation is 100% accurate and up-to-date with Telegram's official Bot API by:
- Fetching latest documentation from `https://core.telegram.org/bots/api`
- Parsing all methods with their parameters and return types
- Parsing all types with their properties
- Generating JSON schemas for code generation

## Output

- `output/methods.json` - All Bot API methods
- `output/types.json` - All Telegram types

## Usage

```bash
# Install dependencies
pip install -r requirements.txt

# Run scraper
python src/scraper.py

# Generate code from JSON
python src/generator.py
```

## Files

- `src/scraper.py` - Main scraper logic
- `src/parser.py` - HTML parsing utilities
- `src/generator.py` - Code generator (Rust + TypeScript)
- `requirements.txt` - Python dependencies

## Requirements

- Python 3.11+
- beautifulsoup4
- requests
- jinja2
