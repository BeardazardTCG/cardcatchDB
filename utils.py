# utils.py

import re

EXCLUDED_TERMS = [
    "psa", "bgs", "cgc", "graded", "beckett", "sgc",
    "lot", "joblot", "bundle", "proxy", "custom",
    "fake", "counterfeit", "damage", "damaged", "played", "heavy", "poor",
    "choose", "multi",
    "japanese", "german", "french", "italian", "spanish", "korean", "chinese"
]

def filter_outliers(prices):
    if not prices:
        return []
    sorted_prices = sorted(prices)
    q1 = sorted_prices[len(sorted_prices) // 4]
    q3 = sorted_prices[(len(sorted_prices) * 3) // 4]
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    return [p for p in prices if lower_bound <= p <= upper_bound]

def calculate_median(prices):
    n = len(prices)
    if n == 0:
        return None
    sorted_prices = sorted(prices)
    mid = n // 2
    if n % 2 == 0:
        return (sorted_prices[mid - 1] + sorted_prices[mid]) / 2
    return sorted_prices[mid]

def calculate_average(prices):
    if not prices:
        return None
    return sum(prices) / len(prices)

def is_valid_price(price):
    return price is not None and price >= 0.50

def is_valid_condition(condition):
    if not condition:
        return True
    lowered = condition.strip().lower()
    return lowered not in ["damaged", "poor"]

def is_valid_title(title, character, digits):
    lowered = title.lower()

    if any(term in lowered for term in EXCLUDED_TERMS):
        return False
    if character and character not in lowered:
        return False
    if digits:
        numeric = "".join(filter(str.isdigit, lowered))
        if digits not in numeric:
            return False
    # Allow x1 but block bulk quantities like 2x, 3x, etc.
if re.search(r"(?:^|\\s)[2-9]x|x[2-9]|\\dx\\d", lowered):
    return False
    if "&" in lowered or "+" in lowered:
        return False
    return True

def detect_holo_type(title):
    lowered = title.lower()
    if "reverse holo" in lowered or "rev holo" in lowered or "rh" in lowered:
        return "Reverse Holo"
    if "non holo" in lowered or "non-holo" in lowered or "nh" in lowered:
        return "Non-Holo"
    if "holo" in lowered or "holofoil" in lowered or "holo rare" in lowered:
        return "Holo"
    return "Unknown"

def parse_card_meta(query):
    parts = query.split()
    character = parts[0].lower() if parts else ""
    number_match = re.search(r"\d+/\d+", query)
    card_number = number_match.group(0) if number_match else ""
    digits_only = re.sub(r"[^\d]", "", card_number)
    return character, digits_only
