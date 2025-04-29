#!/usr/bin/env python3

import json
import argparse
import sys
from collections import defaultdict

def get_json_type(value):
    """Maps Python types to JSON schema types."""
    if isinstance(value, str):
        return "string"
    elif isinstance(value, bool):
        return "boolean"
    elif isinstance(value, int):
        return "integer"
    elif isinstance(value, float):
        return "number"
    elif isinstance(value, list):
        return "array"
    elif isinstance(value, dict):
        return "object"
    elif value is None:
        return "null"
    else:
        # Fallback for unexpected types
        return "unknown"

def merge_schemas(schema1, schema2):
    """Merges two schema definitions, primarily focusing on type."""
    if not schema1: return schema2
    if not schema2: return schema1
    if schema1 == schema2: return schema1

    merged = {}
    types1 = set(schema1.get("type", [])) if isinstance(schema1.get("type"), list) else {schema1.get("type")}
    types2 = set(schema2.get("type", [])) if isinstance(schema2.get("type"), list) else {schema2.get("type")}

    merged_types = list(types1.union(types2))
    if len(merged_types) == 1:
        merged["type"] = merged_types[0]
    else:
        # Prefer list of types if simple, otherwise use anyOf for complex diffs
        if all(t in ["string", "number", "integer", "boolean", "null"] for t in merged_types):
             merged["type"] = sorted([t for t in merged_types if t is not None]) # Sort for consistency
        else:
            # If structures differ significantly, use anyOf
             schemas_to_combine = []
             if schema1.get("type") == "object" or schema1.get("anyOf"):
                 schemas_to_combine.extend(schema1.get("anyOf", [schema1]))
             else:
                  schemas_to_combine.append({"type": schema1["type"]}) # Wrap simple type

             if schema2.get("type") == "object" or schema2.get("anyOf"):
                  schemas_to_combine.extend(schema2.get("anyOf", [schema2]))
             else:
                  schemas_to_combine.append({"type": schema2["type"]}) # Wrap simple type

             # Basic deduplication of simple types within anyOf
             unique_schemas = []
             seen_types = set()
             for s in schemas_to_combine:
                 s_type = s.get("type")
                 if isinstance(s_type, str) and s_type in seen_types:
                     continue
                 unique_schemas.append(s)
                 if isinstance(s_type, str):
                     seen_types.add(s_type)

             if len(unique_schemas) == 1:
                 return unique_schemas[0]
             else:
                merged["anyOf"] = unique_schemas
                # Remove 'type' if 'anyOf' is present
                merged.pop("type", None)


    # Very basic merging for array items (could be more sophisticated)
    if merged.get("type") == "array" or "anyOf" in merged:
        items1 = schema1.get("items")
        items2 = schema2.get("items")
        if items1 and items2:
            merged["items"] = merge_schemas(items1, items2)
        elif items1:
            merged["items"] = items1
        elif items2:
            merged["items"] = items2

    # Very basic merging for object properties (could be more sophisticated)
    if merged.get("type") == "object" or "anyOf" in merged:
         props1 = schema1.get("properties", {})
         props2 = schema2.get("properties", {})
         all_keys = set(props1.keys()) | set(props2.keys())
         merged_props = {}
         for key in all_keys:
             prop_schema = merge_schemas(props1.get(key), props2.get(key))
             if prop_schema:
                 merged_props[key] = prop_schema
         if merged_props:
            merged["properties"] = merged_props
            # Note: Required fields are not inferred here

    return merged


def infer_schema(data):
    """Recursively infers the JSON schema from a Python object."""
    data_type = get_json_type(data)
    schema = {"type": data_type}

    if data_type == "object":
        if not data: # Empty object
             schema["properties"] = {}
             # schema["required"] = [] # Cannot infer required fields reliably
             return schema

        properties = {}
        for key, value in data.items():
            properties[key] = infer_schema(value) # Recurse for value
        schema["properties"] = properties
        # schema["required"] = list(properties.keys()) # Could assume all found keys are required, but risky

    elif data_type == "array":
        if not data: # Empty array
            schema["items"] = {} # No data to infer item type, use empty schema (allows anything)
            return schema

        # Infer schema from all items and merge
        item_schema = None
        for item in data:
            current_item_schema = infer_schema(item) # Recurse for item
            item_schema = merge_schemas(item_schema, current_item_schema)

        schema["items"] = item_schema if item_schema else {}

    # For primitive types (string, number, integer, boolean, null),
    # the {"type": data_type} is sufficient for this basic inference.

    return schema

def main():
    parser = argparse.ArgumentParser(description="Generate a JSON Schema from an example JSON file.")
    parser.add_argument("json_file", help="Path to the input JSON file.")
    parser.add_argument("-o", "--output", help="Path to the output JSON Schema file (optional, prints to stdout if omitted).")

    args = parser.parse_args()

    try:
        with open(args.json_file, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError as e:
                print(f"Error: Invalid JSON file '{args.json_file}': {e}", file=sys.stderr)
                sys.exit(1)

    except FileNotFoundError:
        print(f"Error: Input file not found: '{args.json_file}'", file=sys.stderr)
        sys.exit(1)
    except IOError as e:
        print(f"Error reading file '{args.json_file}': {e}", file=sys.stderr)
        sys.exit(1)

    # Infer the schema
    try:
        schema = infer_schema(data)
        # Add top-level schema details
        schema["$schema"] = "http://json-schema.org/draft-07/schema#" # Specify schema version
        schema["title"] = f"Generated schema for {args.json_file}"
        # schema["description"] = "Schema auto-generated from an example JSON file."

    except Exception as e:
        print(f"Error generating schema: {e}", file=sys.stderr)
        sys.exit(1)

    # Output the schema
    output_json = json.dumps(schema, indent=4)

    if args.output:
        try:
            with open(args.output, 'w', encoding='utf-8') as f:
                f.write(output_json)
            print(f"Schema successfully written to '{args.output}'")
        except IOError as e:
            print(f"Error writing output file '{args.output}': {e}", file=sys.stderr)
            sys.exit(1)
    else:
        print(output_json)

if __name__ == "__main__":
    main()