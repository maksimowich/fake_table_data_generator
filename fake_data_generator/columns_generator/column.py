from typing import Generator
from pandas import NaT


class Column:
    def __init__(self,
                 column_name: str,
                 data_type: str = None,
                 generator: Generator = None):
        self.column_name = column_name
        self.data_type = data_type
        self.generator = generator

    def get_as_dict(self):
        return {self.column_name: {'data_type': self.data_type}}

    def set_generator(self, generator):
        next(generator)
        self.generator = generator

    def get_generator(self):
        return self.generator

    def set_data_type(self, data_type):
        self.data_type = data_type

    def get_data_type(self):
        return self.data_type

    def get_column_name(self):
        return self.column_name


class CategoricalColumn(Column):
    def __init__(self,
                 column_name: str,
                 data_type: str = None,
                 generator: Generator = None,
                 values: list = None,
                 probabilities: list = None):
        super().__init__(column_name, data_type, generator)
        self.values = values
        self.probabilities = probabilities

    def get_as_dict(self):
        super_dict = super().get_as_dict()
        if self.data_type == 'date':
            values = list(map(lambda x: x.strftime("%Y-%m-%d") if x is not NaT else None, self.values))
        elif self.data_type == 'timestamp':
            values = list(map(lambda x: x.strftime("%Y-%m-%d %H:%M:%S") if x is not NaT else None, self.values))
        else:
            values = self.values
        super_dict[self.column_name].update({
            'type': 'CATEGORICAL',
            'values': values,
            'probabilities': self.probabilities
        })
        return super_dict

    def set_values(self, values):
        self.values = values

    def get_values(self):
        return self.values

    def set_probabilities(self, probabilities):
        self.probabilities = probabilities

    def get_probabilities(self):
        return self.probabilities


class ContinuousColumn(Column):
    def __init__(self,
                 column_name: str,
                 data_type: str = None,
                 generator: Generator = None,
                 intervals: list = None,
                 probabilities: list = None,
                 date_flag: bool = False):
        super().__init__(column_name, data_type, generator)
        self.intervals = intervals
        self.probabilities = probabilities
        self.date_flag = date_flag

    def get_as_dict(self):
        super_dict = super().get_as_dict()
        super_dict[self.column_name].update({
            'type': 'CONTINUES',
            'intervals': self.intervals,
            'probabilities': self.probabilities,
            'date_flag': self.date_flag,
        })
        return super_dict

    def set_intervals(self, intervals):
        self.intervals = intervals

    def get_intervals(self):
        return self.intervals

    def set_probabilities(self, probabilities):
        self.probabilities = probabilities

    def get_probabilities(self):
        return self.probabilities

    def set_date_flag(self, date_flag: bool):
        self.date_flag = date_flag

    def get_date_flag(self):
        return self.date_flag


class StringColumn(Column):
    def __init__(self,
                 column_name: str,
                 data_type: str = None,
                 generator: Generator = None,
                 common_regex: str = None,
                 string_copy_of: str = None):
        super().__init__(column_name, data_type, generator)
        self.common_regex = common_regex
        self.string_copy_of = string_copy_of

    def get_as_dict(self):
        super_dict = super().get_as_dict()
        super_dict[self.column_name].update({
            'type': 'STRING',
            'common_regex': self.common_regex,
            'string_copy_of': self.string_copy_of,
        })
        return super_dict

    def set_string_copy_of(self, string_copy_of):
        self.string_copy_of = string_copy_of

    def get_string_copy_of(self):
        return self.string_copy_of

    def set_common_regex(self, common_regex):
        self.common_regex = common_regex

    def get_common_regex(self):
        return self.common_regex


class CurrentTimestampColumn(Column):
    def __init__(self,
                 column_name: str,
                 data_type: str = None,
                 generator: Generator = None):
        super().__init__(column_name, data_type, generator)

    def get_as_dict(self):
        super_dict = super().get_as_dict()
        super_dict[self.column_name].update({
            'type': 'CURRENT_TIMESTAMP',
        })
        return super_dict
