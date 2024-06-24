from dataclasses import dataclass
from data.abstract import ExpandableData
from data.metric_code import MetricCode
from data.region_code import RegionCode
from typing import ClassVar


@dataclass
class MetricData(ExpandableData):
    """
    Represents the data of a particular metric
    """
    metric_code: MetricCode = None
    region_code: RegionCode | None = None
    date_num: int = None
    metric_value: float = None
    is_deleted: bool = False
    valid_attribute_names: ClassVar[set[str]] = {'metric_str_value'}

    def __init__(self,
                 metric_code: MetricCode,
                 date_num: int,
                 region_code: RegionCode | None,
                 metric_value: float,
                 is_deleted: bool = False,
                 **kwargs) -> None:
        """
        Init the metric data object.
        :param metric_code: the metric code of the data
        :param date_num: the date of the metric data
        :param region_code: the code of the region the data belongs to
        :param metric_value: the value of the data
        :param is_deleted: is the data not available in the source or not
        :param kwargs:
        """

        super().__init__(**kwargs)

        self.metric_code = metric_code
        self.date_num = date_num
        self.region_code= region_code
        self.metric_value = metric_value
        self.is_deleted = is_deleted

        pass

    def __str__(self) -> str:
        return (f'MetricData('
                f'metric_code={self.metric_code.code}, '
                f'db_code={self.metric_code.db_code.code}, '
                f'date_num={self.date_num}, '
                f'region_code={None if self.region_code is None else self.region_code.reg_code}, '
                f'metric_value={self.metric_value}, '
                f'is_deleted={self.is_deleted}'
                f'{super().__str__()})@{id(self)}')

    def get_key(self) -> (MetricCode, RegionCode | None):
        return self.metric_code, self.region_code
    #
    # @staticmethod
    # def import_metric_data(metric_code: MetricCode, since: datetime, to: datetime) -> None:
    #     """
    #     Imports metric data from API to database
    #     :param metric_code: The parent metric code of the metrics.
    #     :param since: date range to import
    #     :param to: date range to import
    #     :return: None
    #     """
    #     print(f'Starting to download metric data under {metric_code.metric_code} - {metric_code.name}.')
    #     lst: list[cn_stats_data.data.models.MetricData] = cn_stats_data.data.dao.StatsRawDataDao.download_metric_data(
    #         metric_code=metric_code,
    #         since=since,
    #         to=to)
    #     codes = set()
    #     for item in lst:
    #         codes.add(item.metric_code)
    #     print(f'Retrieved {len(lst)} records, {len(codes)} metrics.')
    #     if len(codes) > 0:
    #         MetricCode.add_or_update([i for i in codes])
    #         print('Save the metric code of the data.')
    #     if len(lst) > 0:
    #         MetricData.add_or_update(lst)
    #         print('Saved the metric data.')
