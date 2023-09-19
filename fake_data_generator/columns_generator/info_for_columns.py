import re
import math
from numpy import linspace, NaN
from scipy.stats import gaussian_kde
from pandas import Timestamp


def get_info_for_categorical_column(column_values):
    normalized_frequencies_of_values = column_values.value_counts(normalize=True, dropna=False)
    values = normalized_frequencies_of_values.index.tolist()
    if any(isinstance(value, Timestamp) for value in values):
        values = list(map(lambda x: x.to_pydatetime() if isinstance(x, Timestamp) else x, values))
    elif any(isinstance(value, str) for value in values):
        values = list(map(lambda x: x if x is not NaN else None, values))
    elif any(isinstance(value, float) and not math.isnan(value) for value in values):
        values = list(map(lambda x: int(x) if not math.isnan(x) else None, values))
    probabilities = normalized_frequencies_of_values.to_list()
    return values, probabilities


CONVERTERS_TO_FLOAT = {
    'int': float,
    'float': lambda x: x,
    'date': lambda x: x.toordinal(),
    'datetime': lambda x: x.timestamp(),
}


def get_info_for_continuous_column(column_values, input_data_type: str, number_of_intervals: int):
    float_column_values_without_null = column_values.dropna().apply(CONVERTERS_TO_FLOAT[input_data_type])
    kde = gaussian_kde(float_column_values_without_null.values)
    x = linspace(min(float_column_values_without_null), max(float_column_values_without_null), num=(number_of_intervals + 1))
    intervals = []
    probabilities = []
    for index, value in enumerate(x[:-1]):
        interval = (value, x[index + 1])
        probability = kde.integrate_box_1d(*interval)
        intervals.append(interval)
        probabilities.append(probability)
    k = 1 / sum(probabilities)
    return intervals, list(map(lambda p: p * k, probabilities))


def get_info_for_string_column(strings) -> str:
    """
    Function that returns common regular expression of given strings.

    Parameters
    ----------
     strings: Iterable object returning strings for which common regex should be found

    Returns
    -------
     String representing common regular expression of specified strings

    Examples
    --------
    # >>> get_common_regex(['123', '32314', '131'])
    # '[0-9][0-9][0-9][0-9][0-9]'
    # >>> get_common_regex(['abc', 'a0c', 'xyz'])
    # '[a-z][a-z0-9][a-z]'
    # >>> get_common_regex(['1234-2314', '1241-1234', '2514-2141'])
    '[0-9][0-9][0-9][0-9][-][0-9][0-9][0-9][0-9]'
    """
    common_pattern = {}
    for string in strings:
        for index, char in enumerate(string):
            if re.match(r"[0-9]", char):
                if '0-9' not in common_pattern.get(index, ''):
                    common_pattern[index] = common_pattern.get(index, '') + '0-9'
                else:
                    continue
            elif re.match(r"[A-Z]", char):
                if 'A-Z' not in common_pattern.get(index, ''):
                    common_pattern[index] = common_pattern.get(index, '') + 'A-Z'
                else:
                    continue
            elif re.match(r"[a-z]", char):
                if 'a-z' not in common_pattern.get(index, ''):
                    common_pattern[index] = common_pattern.get(index, '') + 'a-z'
                else:
                    continue
            elif re.match(r"[А-Я]", char):
                if 'А-Я' not in common_pattern.get(index, ''):
                    common_pattern[index] = common_pattern.get(index, '') + 'А-Я'
                else:
                    continue
            elif re.match(r"[а-я]", char):
                if 'а-я' not in common_pattern.get(index, ''):
                    common_pattern[index] = common_pattern.get(index, '') + 'а-я'
                else:
                    continue
            else:
                if char not in common_pattern.get(index, ''):
                    common_pattern[index] = common_pattern.get(index, '') + char
    common_pattern_string = ''
    for _, symbol_regex in sorted(common_pattern.items()):
        common_pattern_string += '[' + symbol_regex + ']'
    return common_pattern_string
