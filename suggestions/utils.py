import numpy as np
import scipy.stats
from typing import Dict, Optional


def calculate_entropy(distribution: Dict[str, float]) -> float:
    """Calculate entropy of a distribution"""
    values = np.array(list(distribution.values()))
    probabilities = values / values.sum() if values.sum() > 0 else values
    return scipy.stats.entropy(probabilities)


def calculate_time_by_percentile_from_distribution(distribution: Dict[str, int], percentiles=90) -> Optional[int]:
    """Calculate the 97th percentile (p97) from a distribution.

    Args:
        distribution: A dictionary where keys are categories (e.g., time buckets) and values are counts.
    Returns:
        The 97th percentile value as an integer, or None if the distribution is empty.
    """
    if not distribution:
        return None

    sorted_buckets = sorted((int(bucket[:2]) * 60 + int(bucket[2:]), count) for bucket, count in distribution.items())
    total_count = sum(count for _, count in sorted_buckets)

    cumulative_count = 0
    for time_in_minutes, count in sorted_buckets:
        cumulative_count += count
        if (cumulative_count / total_count) * 100 >= percentiles:
            return time_in_minutes * 60

    return None
