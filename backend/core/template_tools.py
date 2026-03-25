import re


def extract_template_cols(template_str):
    pattern = r"row\s*(?:\[\s*['\"]([^'\"]+)['\"]\s*\]|\.\s*get\(\s*['\"]([^'\"]+)['\"]\s*\)|\.\s*(\w+))"
    matches = re.findall(pattern, template_str)
    cols = []
    for m in matches:
        col = m[0] or m[1] or m[2]
        if col and col not in cols:
            cols.append(col)
    return cols


def analyze_template_requirements(template_str):
    all_cols = extract_template_cols(template_str)
    template_without_ifs = re.sub(r"{%\s*if.*?%}.*?{%\s*endif\s*%}", "", template_str, flags=re.DOTALL)
    required_cols = extract_template_cols(template_without_ifs)
    optional_cols = [c for c in all_cols if c not in required_cols]
    return required_cols, optional_cols
