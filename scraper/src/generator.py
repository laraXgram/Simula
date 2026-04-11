"""
Simula - Code Generator

Generates Rust structs and TypeScript types from scraped API data.
"""

import json
import logging
import os
import re
from pathlib import Path

from jinja2 import Environment, BaseLoader

logger = logging.getLogger(__name__)


# Type mappings
RUST_TYPE_MAP = {
    'Integer': 'i64',
    'String': 'String',
    'Boolean': 'bool',
    'Float': 'f64',
    'Float number': 'f64',
    'True': 'bool',
    'False': 'bool',
}

TS_TYPE_MAP = {
    'Integer': 'number',
    'String': 'string',
    'Boolean': 'boolean',
    'Float': 'number',
    'Float number': 'number',
    'True': 'boolean',
    'False': 'boolean',
}

RUST_KEYWORDS = {
    'as', 'break', 'const', 'continue', 'crate', 'else', 'enum', 'extern', 'false',
    'fn', 'for', 'if', 'impl', 'in', 'let', 'loop', 'match', 'mod', 'move', 'mut',
    'pub', 'ref', 'return', 'self', 'Self', 'static', 'struct', 'super', 'trait',
    'true', 'type', 'unsafe', 'use', 'where', 'while', 'async', 'await', 'dyn',
}

_RE_WHITESPACE = re.compile(r'\s+')
_RE_ARRAY_OF = re.compile(r'Array\s*of\s*', re.IGNORECASE)
_RE_ARRAY_ANGLE = re.compile(r'Array\s*<\s*(.+?)\s*>')
_RE_WORD_TOKEN = re.compile(r'[A-Za-z_][A-Za-z0-9_]*')
_RE_GLUED_OR = re.compile(r'(.+?)or\s*([A-Z].+)')
_RE_GLUED_AND = re.compile(r'(.+?)and\s*([A-Z].+)')

CUSTOM_TYPE_NAMES: set[str] = set()


def extract_custom_type_tokens(type_str: str) -> set[str]:
    """Extract referenced custom Telegram type names from a raw type expression."""
    normalized = normalize_type_expr(type_str)
    tokens = set(_RE_WORD_TOKEN.findall(normalized))
    return {token for token in tokens if token in CUSTOM_TYPE_NAMES}


def has_dependency_path(graph: dict[str, set[str]], start: str, target: str, visited: set[str] | None = None) -> bool:
    """Return True if a dependency path exists from start -> target."""
    if start == target:
        return True

    if visited is None:
        visited = set()
    if start in visited:
        return False

    visited.add(start)
    for neighbor in graph.get(start, set()):
        if neighbor == target:
            return True
        if has_dependency_path(graph, neighbor, target, visited):
            return True

    return False


def normalize_type_expr(type_str: str) -> str:
    """Normalize type strings scraped from docs into parseable expressions."""
    s = (type_str or '').strip()
    if not s:
        return 'serde_json::Value'

    s = _RE_WHITESPACE.sub(' ', s)
    s = _RE_ARRAY_OF.sub('Array of ', s)

    return _RE_WHITESPACE.sub(' ', s).strip()


def is_known_atomic_type(token: str) -> bool:
    token = token.strip()
    if not token:
        return False

    if token in RUST_TYPE_MAP or token in TS_TYPE_MAP:
        return True

    if token == 'InputFile':
        return True

    return token in CUSTOM_TYPE_NAMES


def split_union_parts(type_str: str) -> list[str]:
    """Split union-like type strings into parts."""
    if ' or ' in type_str:
        return [p.strip() for p in type_str.split(' or ') if p.strip()]

    # Handle lists like "A, B and C" as unions.
    candidate = type_str.replace(' and ', ', ')
    if ',' in candidate:
        parts = [p.strip() for p in candidate.split(',') if p.strip()]
        if len(parts) > 1:
            return parts

    for pattern in (_RE_GLUED_OR, _RE_GLUED_AND):
        glued = pattern.fullmatch(type_str)
        if not glued:
            continue

        left = glued.group(1).strip()
        right = glued.group(2).strip()
        if is_known_atomic_type(left) and right and right[0].isupper():
            return [left, right]

    return [type_str]


def parse_type(type_str: str, lang: str = 'rust', ts_use_namespace: bool = False) -> str:
    """Parse a Telegram type string to Rust or TypeScript type."""
    type_map = RUST_TYPE_MAP if lang == 'rust' else TS_TYPE_MAP
    array_wrapper = 'Vec<{}>' if lang == 'rust' else '{}[]'

    type_str = normalize_type_expr(type_str)

    # Handle "Array of X"
    if type_str.startswith('Array of '):
        inner = type_str[9:].strip()
        inner_type = parse_type(inner, lang, ts_use_namespace)
        return array_wrapper.format(inner_type)

    # Handle "Array<X>" as an alternative notation.
    array_match = _RE_ARRAY_ANGLE.fullmatch(type_str)
    if array_match:
        inner = array_match.group(1).strip()
        inner_type = parse_type(inner, lang, ts_use_namespace)
        return array_wrapper.format(inner_type)

    # Handle union/list-like types.
    parts = split_union_parts(type_str)
    if len(parts) > 1:
        if lang == 'rust':
            # Use a dynamic type for heterogeneous unions.
            return 'serde_json::Value'

        types = [parse_type(p.strip(), lang, ts_use_namespace) for p in parts]
        return ' | '.join(types)

    # Handle basic types
    if type_str in type_map:
        return type_map[type_str]

    # Handle InputFile specially
    if type_str == 'InputFile':
        if lang == 'rust':
            return 'InputFile'
        return 'Types.InputFile | string' if ts_use_namespace else 'InputFile | string'

    # It's a custom Telegram type
    if lang == 'ts' and ts_use_namespace:
        return f'Types.{type_str}'

    return type_str


def to_pascal_case(name: str) -> str:
    """Convert snake_case/camelCase to PascalCase."""
    if '_' in name:
        return ''.join(word.capitalize() for word in name.split('_'))
    if not name:
        return name
    return name[0].upper() + name[1:]


def sanitize_rust_field_name(name: str) -> str:
    """Return a Rust-safe field identifier."""
    if name == 'type':
        return 'r#type'
    if name in RUST_KEYWORDS:
        return f'{name}_field'
    return name


# Rust template for types
RUST_TYPES_TEMPLATE = '''// Auto-generated by Simula Code Generator
// DO NOT EDIT MANUALLY

use serde::{Deserialize, Serialize};

{% for type in types %}
/// {{ type.description[:100] }}...
#[derive(Debug, Clone, Serialize, Deserialize)]
{% if type.is_placeholder %}
pub struct {{ type.name }} {
    #[serde(flatten)]
    pub extra: serde_json::Value,
}
{% else %}
pub struct {{ type.name }} {
{% for prop in type.properties %}
    {% if prop.optional %}#[serde(skip_serializing_if = "Option::is_none")]
    {% endif %}{% if prop.needs_rename %}#[serde(rename = "{{ prop.original_name }}")]
    {% endif %}pub {{ prop.name }}: {% if prop.optional %}Option<{{ prop.rust_type }}>{% else %}{{ prop.rust_type }}{% endif %},
{% endfor %}
}
{% endif %}

{% endfor %}
'''

# Rust template for methods
RUST_METHODS_TEMPLATE = '''// Auto-generated by Simula Code Generator
// DO NOT EDIT MANUALLY

use serde::{Deserialize, Serialize};
use super::types::*;

{% for method in methods %}
/// {{ method.description[:80] }}...
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct {{ method.struct_name }}Request {
{% for param in method.parameters %}
    {% if not param.required %}#[serde(skip_serializing_if = "Option::is_none")]
    {% endif %}{% if param.needs_rename %}#[serde(rename = "{{ param.original_name }}")]
    {% endif %}pub {{ param.name }}: {% if not param.required %}Option<{{ param.rust_type }}>{% else %}{{ param.rust_type }}{% endif %},
{% endfor %}
}

{% endfor %}
'''

# TypeScript template for types
TS_TYPES_TEMPLATE = '''// Auto-generated by Simula Code Generator
// DO NOT EDIT MANUALLY

{% for type in types %}
/** {{ type.description[:100] }}... */
{% if type.is_placeholder %}
export type {{ type.name }} = Record<string, unknown>;
{% else %}
export interface {{ type.name }} {
{% for prop in type.properties %}
  {{ prop.ts_name }}{% if prop.optional %}?{% endif %}: {{ prop.ts_type }};
{% endfor %}
}
{% endif %}

{% endfor %}
'''

# TypeScript template for methods
TS_METHODS_TEMPLATE = '''// Auto-generated by Simula Code Generator
// DO NOT EDIT MANUALLY

import type * as Types from './types';

{% for method in methods %}
/** {{ method.description[:80] }}... */
export interface {{ method.struct_name }}Request {
{% for param in method.parameters %}
  {{ param.ts_name }}{% if not param.required %}?{% endif %}: {{ param.ts_type }};
{% endfor %}
}

export type {{ method.struct_name }}Response = {{ method.ts_return_type }};

{% endfor %}

// All method names
export type MethodName = {% for method in methods %}'{{ method.name }}'{% if not loop.last %} | {% endif %}{% endfor %};
'''


class CodeGenerator:
    """Generates code from scraped API data."""

    def __init__(self, types_file: str, methods_file: str):
        self.types_file = types_file
        self.methods_file = methods_file
        self.types_data = []
        self.methods_data = []
        self.env = Environment(loader=BaseLoader())

    def load_data(self) -> None:
        """Load JSON data files."""
        global CUSTOM_TYPE_NAMES

        with open(self.types_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            self.types_data = data['types']

        CUSTOM_TYPE_NAMES = {t['name'] for t in self.types_data}

        with open(self.methods_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            self.methods_data = data['methods']

        logger.info("Loaded %d types", len(self.types_data))
        logger.info("Loaded %d methods", len(self.methods_data))

    def process_types(self) -> list:
        """Process types for code generation."""
        processed = []

        type_dependency_graph: dict[str, set[str]] = {}
        for t in self.types_data:
            dependencies: set[str] = set()
            for p in t.get('properties', []):
                dependencies |= extract_custom_type_tokens(p.get('type', ''))
            type_dependency_graph[t['name']] = dependencies

        for t in self.types_data:
            if not t['properties']:
                processed.append({
                    'name': t['name'],
                    'description': t['description'][:100] if t['description'] else '',
                    'properties': [],
                    'is_placeholder': True,
                })
                continue

            props = []
            for p in t['properties']:
                rust_name = sanitize_rust_field_name(p['name'])
                rust_type = parse_type(p['type'], 'rust')

                # Break recursive cycles in Rust structs using indirection.
                # This covers both direct self-recursion and indirect cycles like A -> B -> A.
                if rust_type in CUSTOM_TYPE_NAMES and has_dependency_path(type_dependency_graph, rust_type, t['name']):
                    rust_type = f'Box<{rust_type}>'

                props.append({
                    'name': rust_name,
                    'ts_name': p['name'],
                    'original_name': p['name'],
                    'rust_type': rust_type,
                    'ts_type': parse_type(p['type'], 'ts', False),
                    'optional': p['optional'],
                    'needs_rename': rust_name != p['name'] and not rust_name.startswith('r#'),
                    'description': p['description'][:50] if p['description'] else '',
                })

            processed.append({
                'name': t['name'],
                'description': t['description'][:100] if t['description'] else '',
                'properties': props,
                'is_placeholder': False,
            })

        return processed

    def process_methods(self) -> list:
        """Process methods for code generation."""
        processed = []

        for m in self.methods_data:
            params = []
            for p in m['parameters']:
                rust_name = p['name']
                if rust_name in RUST_KEYWORDS:
                    rust_name = f"{rust_name}_param"

                params.append({
                    'name': rust_name,
                    'ts_name': p['name'],
                    'original_name': p['name'],
                    'rust_type': parse_type(p['type'], 'rust'),
                    'ts_type': parse_type(p['type'], 'ts', True),
                    'required': p['required'],
                    'needs_rename': rust_name != p['name'],
                    'description': p['description'][:30] if p['description'] else '',
                })

            return_type = m['return_type']
            rust_return = parse_type(return_type, 'rust')
            ts_return = parse_type(return_type, 'ts', True)

            processed.append({
                'name': m['name'],
                'struct_name': to_pascal_case(m['name']),
                'description': m['description'][:80] if m['description'] else '',
                'parameters': params,
                'rust_return_type': rust_return,
                'ts_return_type': ts_return,
            })

        return processed

    def _render_to_file(
        self, template_str: str, context: dict, output_file: str,
    ) -> None:
        """Render a Jinja2 template to an output file."""
        template = self.env.from_string(template_str)
        output = template.render(**context)

        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(output)

    def generate_all(
        self, rust_output_dir: str, ts_output_dir: str,
    ) -> None:
        """Generate all code files.

        Processes types and methods once, then renders all four
        output files (Rust types/methods, TypeScript types/methods).
        """
        types = self.process_types()
        methods = self.process_methods()

        outputs = [
            (RUST_TYPES_TEMPLATE, {'types': types},
             os.path.join(rust_output_dir, 'types.rs'),
             f"{len(types)} types"),
            (RUST_METHODS_TEMPLATE, {'methods': methods},
             os.path.join(rust_output_dir, 'methods.rs'),
             f"{len(methods)} methods"),
            (TS_TYPES_TEMPLATE, {'types': types},
             os.path.join(ts_output_dir, 'types.ts'),
             f"{len(types)} types"),
            (TS_METHODS_TEMPLATE, {'methods': methods},
             os.path.join(ts_output_dir, 'methods.ts'),
             f"{len(methods)} methods"),
        ]

        for template_str, context, path, desc in outputs:
            self._render_to_file(template_str, context, path)
            logger.info("Generated %s: %s", desc, path)


def main():
    """Main entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    logger.info("Simula - Code Generator")

    script_dir = Path(__file__).resolve().parent
    scraper_root = script_dir.parent
    repo_root = scraper_root.parent

    types_file = Path(
        os.environ.get('SCRAPER_TYPES_FILE', str(scraper_root / 'output' / 'types.json'))
    ).resolve()
    methods_file = Path(
        os.environ.get('SCRAPER_METHODS_FILE', str(scraper_root / 'output' / 'methods.json'))
    ).resolve()
    rust_output_dir = Path(
        os.environ.get('RUST_OUTPUT_DIR', str(repo_root / 'api-server' / 'src' / 'generated'))
    ).resolve()
    ts_output_dir = Path(
        os.environ.get('TS_OUTPUT_DIR', str(repo_root / 'client' / 'src' / 'types' / 'generated'))
    ).resolve()

    generator = CodeGenerator(
        types_file=str(types_file),
        methods_file=str(methods_file)
    )

    generator.load_data()

    generator.generate_all(
        rust_output_dir=str(rust_output_dir),
        ts_output_dir=str(ts_output_dir)
    )

    logger.info("Code generation completed!")


if __name__ == "__main__":
    main()
