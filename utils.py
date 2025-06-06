# utils.py

EXCLUDED_TERMS = [
    "psa", "bgs", "cgc", "graded", "beckett", "sgc",
    "lot", "joblot", "bundle", "proxy", "custom",
    "fake", "counterfeit", "damage", "damaged", "played", "heavy", "poor",
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
    """Reject obviously invalid prices like 0 or below 0.50"""
    return price is not None and price >= 0.50

def is_valid_condition(condition):
    """Reject cards explicitly marked as damaged or poor"""
    if not condition:
        return True
    lowered = condition.strip().lower()
    return lowered not in ["damaged", "poor"]

def is_valid_title(title, character, digits):
    """Title exclusion logic shared across all eBay scrapers"""
    lowered = title.lower()
    if any(term in lowered for term in EXCLUDED_TERMS):
        return False
    if character and character not in lowered:
        return False
    if digits:
        numeric = "".join(filter(str.isdigit, lowered))
        if digits not in numeric:
            return False
    # Reject bundles like "x4" or "×3"
    if "x" in lowered or "×" in lowered:
        if any(char.isdigit() for char in lowered.split("x")[0]):
            return False
    return True
