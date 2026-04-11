"""
Simula - Telegram Bot API Scraper

This scraper fetches the official Telegram Bot API documentation
and extracts all methods and types with their parameters/properties.

Output:
- output/methods.json: All Bot API methods
- output/types.json: All Telegram types
"""

import json
import logging
import os
import re
from dataclasses import asdict, dataclass, field
from typing import Optional

import requests
from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)

# Pre-compiled regex patterns for return type extraction
_RE_RETURNS_TRUE = re.compile(r'Returns?\s+True\b', re.IGNORECASE)
_RE_ARRAY_OF_LINK = re.compile(
    r'[Aa]rray\s+of\s+<a[^>]*>([A-Z][a-zA-Z]+)</a>',
)
_RE_RETURNS_LINK = re.compile(
    r'[Rr]eturns?[^<]*<a[^>]*>([A-Z][a-zA-Z]+)</a>',
)
_RE_SENT_LINK = re.compile(
    r'the\s+sent\s+<a[^>]*>([A-Z][a-zA-Z]+)</a>',
)
_RE_SUCCESS_LINK = re.compile(
    r'[Oo]n\s+success[^<]*<a[^>]*>([A-Z][a-zA-Z]+)</a>',
)
_RE_FORM_OF_LINK = re.compile(
    r'form\s+of\s+(?:a|an)\s+<a[^>]*>([A-Z][a-zA-Z]+)</a>',
)
_RE_RETURN_OR_SUCCESS = re.compile(r'[Rr]eturn|success')


# Data structures
@dataclass
class Parameter:
    name: str
    type: str
    required: bool
    description: str


@dataclass
class Method:
    name: str
    description: str
    parameters: list[Parameter]
    return_type: str
    return_description: str = ""


@dataclass
class Property:
    name: str
    type: str
    optional: bool
    description: str


@dataclass
class TelegramType:
    name: str
    description: str
    properties: list[Property]
    is_subtype_of: list[str] = field(default_factory=list)


class TelegramAPIScraper:
    """Scrapes Telegram Bot API documentation for methods and types."""

    API_URL = "https://core.telegram.org/bots/api"
    CACHE_FILE = "output/api_page.html"

    def __init__(self, use_cache: bool = False):
        self.use_cache = use_cache
        self.soup: Optional[BeautifulSoup] = None
        self.methods: list[Method] = []
        self.types: list[TelegramType] = []

    def fetch_page(self) -> str:
        """Fetch the API documentation page."""
        # Check cache first
        if self.use_cache and os.path.exists(self.CACHE_FILE):
            logger.info("Loading from cache: %s", self.CACHE_FILE)
            with open(self.CACHE_FILE, 'r', encoding='utf-8') as f:
                return f.read()

        logger.info("Fetching: %s", self.API_URL)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(self.API_URL, headers=headers, timeout=30)
        response.raise_for_status()

        # Cache the response
        os.makedirs("output", exist_ok=True)
        with open(self.CACHE_FILE, 'w', encoding='utf-8') as f:
            f.write(response.text)
        logger.info("Cached to: %s", self.CACHE_FILE)

        return response.text

    def parse(self, html: str) -> None:
        """Parse the HTML and extract methods and types."""
        self.soup = BeautifulSoup(html, 'lxml')

        # Find the main content
        content = self.soup.find('div', id='dev_page_content')
        if not content:
            raise ValueError("Could not find main content div")

        # Process all h4 elements (methods and types are defined under h4)
        h4_elements = content.find_all('h4')

        logger.info("Found %d h4 elements", len(h4_elements))

        for h4 in h4_elements:
            name = h4.get_text(strip=True)

            # Skip navigation/section headers
            if not name or name.startswith('#'):
                continue

            # Determine if it's a method or type
            # Methods start with lowercase, types start with uppercase
            if name and name[0].islower():
                method = self._parse_method(h4)
                if method:
                    self.methods.append(method)
            elif name and name[0].isupper():
                telegram_type = self._parse_type(h4)
                if telegram_type:
                    self.types.append(telegram_type)

        logger.info("Parsed %d methods", len(self.methods))
        logger.info("Parsed %d types", len(self.types))

    def _parse_method(self, h4: Tag) -> Optional[Method]:
        """Parse a method definition."""
        name = h4.get_text(strip=True)

        # Get description (paragraphs between h4 and table or next h4)
        description_parts = []
        return_type = "Boolean"  # Default
        return_description = ""

        sibling = h4.find_next_sibling()
        table = None

        while sibling and sibling.name != 'h4':
            if sibling.name == 'p':
                text = sibling.get_text(strip=True)
                description_parts.append(text)

                # Extract return type from links and text in this paragraph
                extracted_type = self._extract_return_type_from_element(sibling)
                if extracted_type:
                    return_type = extracted_type
                    return_description = text

            elif sibling.name == 'table':
                table = sibling
                break
            sibling = sibling.find_next_sibling()

        description = ' '.join(description_parts)

        # Parse parameters from table
        parameters = []
        if table:
            parameters = self._parse_parameters_table(table)

        return Method(
            name=name,
            description=description,
            parameters=parameters,
            return_type=return_type,
            return_description=return_description
        )

    def _extract_return_type_from_element(self, element: Tag) -> Optional[str]:
        """Extract return type from paragraph element including links."""
        text = element.get_text()

        # Check for True/Boolean return first
        if _RE_RETURNS_TRUE.search(text):
            return "Boolean"

        # Find all links in the element
        links = element.find_all('a')
        type_links = []
        for link in links:
            href = link.get('href', '')
            if href.startswith('#') and len(href) > 1:
                link_text = link.get_text(strip=True)
                # Check if it's a Type (starts with uppercase)
                if link_text and link_text[0].isupper():
                    type_links.append(link_text)

        # Look for patterns in text that indicate return type
        html = str(element)

        # Check for "Array of X" pattern
        array_match = _RE_ARRAY_OF_LINK.search(html)
        if array_match:
            return f"Array<{array_match.group(1)}>"

        # Check for "Returns ... <a>Type</a>" pattern
        returns_link_match = _RE_RETURNS_LINK.search(html)
        if returns_link_match:
            return returns_link_match.group(1)

        # Check for "the sent <a>Message</a>" pattern
        sent_match = _RE_SENT_LINK.search(html)
        if sent_match:
            return sent_match.group(1)

        # Check for "On success, <a>Type</a> is returned"
        success_match = _RE_SUCCESS_LINK.search(html)
        if success_match:
            return success_match.group(1)

        # Check for "form of a <a>Type</a>"
        form_match = _RE_FORM_OF_LINK.search(html)
        if form_match:
            return form_match.group(1)

        # If we have type links and the text mentions returns/success
        if type_links and _RE_RETURN_OR_SUCCESS.search(text):
            return type_links[-1]  # Usually the return type is last

        return None

    def _parse_type(self, h4: Tag) -> Optional[TelegramType]:
        """Parse a type definition."""
        name = h4.get_text(strip=True)

        # Skip if it looks like a section header
        if ' ' in name or len(name) < 2:
            return None

        # Get description
        description_parts = []
        is_subtype_of = []

        sibling = h4.find_next_sibling()
        table = None

        while sibling and sibling.name != 'h4':
            if sibling.name == 'p':
                text = sibling.get_text(strip=True)
                description_parts.append(text)

            elif sibling.name == 'ul':
                # Sometimes types have a list of subtypes
                pass

            elif sibling.name == 'table':
                table = sibling
                break
            sibling = sibling.find_next_sibling()

        description = ' '.join(description_parts)

        # Parse properties from table
        properties = []
        if table:
            properties = self._parse_properties_table(table)

        return TelegramType(
            name=name,
            description=description,
            properties=properties,
            is_subtype_of=is_subtype_of
        )

    def _parse_parameters_table(self, table: Tag) -> list[Parameter]:
        """Parse method parameters from a table."""
        parameters = []

        rows = table.find_all('tr')

        for row in rows[1:]:  # Skip header row
            cells = row.find_all('td')
            if len(cells) >= 4:
                name = cells[0].get_text(strip=True)
                param_type = cells[1].get_text(strip=True)
                required_text = cells[2].get_text(strip=True)
                description = cells[3].get_text(strip=True)

                required = required_text.lower() == 'yes'

                parameters.append(Parameter(
                    name=name,
                    type=param_type,
                    required=required,
                    description=description
                ))
            elif len(cells) == 3:
                # Some tables have Required merged with description
                name = cells[0].get_text(strip=True)
                param_type = cells[1].get_text(strip=True)
                description = cells[2].get_text(strip=True)

                required = not description.lower().startswith('optional')

                parameters.append(Parameter(
                    name=name,
                    type=param_type,
                    required=required,
                    description=description
                ))

        return parameters

    def _parse_properties_table(self, table: Tag) -> list[Property]:
        """Parse type properties from a table."""
        properties = []

        rows = table.find_all('tr')

        for row in rows[1:]:  # Skip header row
            cells = row.find_all('td')
            if len(cells) >= 3:
                name = cells[0].get_text(strip=True)
                prop_type = cells[1].get_text(strip=True)
                description = cells[2].get_text(strip=True)

                # Check if optional
                optional = 'optional' in description.lower()

                properties.append(Property(
                    name=name,
                    type=prop_type,
                    optional=optional,
                    description=description
                ))

        return properties

    def save_output(self, output_dir: str = "output") -> None:
        """Save methods and types to JSON files."""
        os.makedirs(output_dir, exist_ok=True)

        # Convert to dict for JSON serialization
        methods_data = {
            "version": "Bot API",
            "scraped_from": self.API_URL,
            "total_methods": len(self.methods),
            "methods": [self._method_to_dict(m) for m in self.methods]
        }

        types_data = {
            "version": "Bot API",
            "scraped_from": self.API_URL,
            "total_types": len(self.types),
            "types": [self._type_to_dict(t) for t in self.types]
        }

        # Save methods
        methods_file = os.path.join(output_dir, "methods.json")
        with open(methods_file, 'w', encoding='utf-8') as f:
            json.dump(methods_data, f, indent=2, ensure_ascii=False)
        logger.info("Saved %d methods to %s", len(self.methods), methods_file)

        # Save types
        types_file = os.path.join(output_dir, "types.json")
        with open(types_file, 'w', encoding='utf-8') as f:
            json.dump(types_data, f, indent=2, ensure_ascii=False)
        logger.info("Saved %d types to %s", len(self.types), types_file)

    def _method_to_dict(self, method: Method) -> dict:
        """Convert Method to dictionary."""
        return {
            "name": method.name,
            "description": method.description,
            "return_type": method.return_type,
            "return_description": method.return_description,
            "parameters": [asdict(p) for p in method.parameters]
        }

    def _type_to_dict(self, t: TelegramType) -> dict:
        """Convert TelegramType to dictionary."""
        return {
            "name": t.name,
            "description": t.description,
            "is_subtype_of": t.is_subtype_of,
            "properties": [asdict(p) for p in t.properties]
        }

    def log_summary(self) -> None:
        """Log a summary of scraped data."""
        logger.info("SCRAPING SUMMARY")

        logger.info("Methods (%d):", len(self.methods))
        for m in self.methods[:10]:
            params = len(m.parameters)
            logger.info("  %s(%d params) -> %s", m.name, params, m.return_type)
        if len(self.methods) > 10:
            logger.info("  ... and %d more", len(self.methods) - 10)

        logger.info("Types (%d):", len(self.types))
        for t in self.types[:10]:
            props = len(t.properties)
            logger.info("  %s (%d properties)", t.name, props)
        if len(self.types) > 10:
            logger.info("  ... and %d more", len(self.types) - 10)


def main():
    """Main entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    logger.info("Simula - Telegram Bot API Scraper")

    # Check if we should use cache (for offline development)
    use_cache = os.environ.get('USE_CACHE', '').lower() == 'true'

    scraper = TelegramAPIScraper(use_cache=use_cache)

    try:
        # Fetch and parse
        html = scraper.fetch_page()
        scraper.parse(html)

        # Save output
        scraper.save_output()

        # Log summary
        scraper.log_summary()

        logger.info("Scraping completed successfully!")

    except requests.RequestException as e:
        logger.error("Network error: %s", e)
        logger.info("Tip: Set USE_CACHE=true to use cached HTML")
        return 1
    except Exception as e:
        logger.error("Error: %s", e)
        raise

    return 0


if __name__ == "__main__":
    exit(main())
