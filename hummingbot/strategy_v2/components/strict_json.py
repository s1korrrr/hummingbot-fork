"""Unambiguous JSON decoding shared by recipes and feature specifications."""
import json


def strict_json(raw: str):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError(f'duplicate JSON key: {key}')
            result[key] = value
        return result

    def constant(value):
        raise ValueError(f'nonfinite JSON constant: {value}')

    return json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)
