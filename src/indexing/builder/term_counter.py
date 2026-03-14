from collections import Counter

def count_terms(tokens: list[str]) -> dict[str, int]:
    """Count token frequencies for a processed document."""
    return dict(Counter(tokens))
