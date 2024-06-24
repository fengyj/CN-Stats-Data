# from datetime import datetime
#
# import psycopg2
# from cn_stats_data.db import db_config
# from cn_stats_data.data.models import *
# from cnstats.common import easyquery
# from cnstats.stats import stats
# import cn_stats_data.data.models
# import time
# from dateutil.relativedelta import relativedelta
# from functools import cache
#
# __all__ = ['StatsRawDataDao']
#
#
# class StatsRawDataDao:
#
#     @staticmethod
#     def get_conn():
#         return psycopg2.connect(
#             database=db_config.db,
#             user=db_config.user,
#             password=db_config.password,
#             host=db_config.server,
#             port=db_config.port)
#
#     @staticmethod
#     @cache
#     def download_metrics(db_code: DbCode, metric_code: str = 'zb') -> list[cn_stats_data.data.models.MetricCode]:
#         """
#         Downloads metric codes from CN-Stats via the getTree API
#         :param db_code: Downloads the codes under the specified db code.
#         :param metric_code: Downloads the metric and its descendants. If it's None means downloads all the codes
#         :return: Returns the metric codes returned by the API
#         """
#         lst = []
#         parents = []
#         for n in easyquery(m='getTree', dbcode=db_code.code, wdcode='zb', id=metric_code):
#             lst.append(cn_stats_data.data.models.MetricCode(
#                 n['id'],
#                 db_code,
#                 n['pid'],
#                 kwarg={
#                     'name': n['name'],
#                     'english_name': None,
#                     'explanation': None,
#                     'memo': None
#                 }))
#             if n['isParent']:
#                 parents.append(n["id"])
#
#         for item in parents:
#             time.sleep(2)  # sleep two seconds to reduce the access frequency to the API
#             lst.extend(StatsRawDataDao.download_metrics(db_code, item))
#
#         return lst
#
#     @staticmethod
#     @cache
#     def download_metric_data(metric_code: cn_stats_data.data.models.MetricCode,
#                              since: datetime,
#                              to: datetime) -> list[cn_stats_data.data.models.MetricData]:
#         """
#         Downloads metric data from CN-Stats via the API
#         :param metric_code: Downloads the metric data of the code's descendants.
#         :param since: date range to download
#         :param to: date range to download
#         :return: Returns the metric data returned by the API
#         """
#         db_code = metric_code.db_code
#         dates = []
#         if db_code.is_annual():
#             dates = [db_code.get_date_code(i) for i in range(since.year, to.year + 1)]
#         elif db_code.is_quarterly():
#             date = since + relativedelta(day=1)
#             to = to + relativedelta(day=1)
#             while date <= to:
#                 dates.append(db_code.get_date_code(date.year * 100 + ((date.month + 2) // 3)))
#                 date = date + relativedelta(months=3)
#         elif db_code.is_monthly():
#             date = since + relativedelta(day=1)
#             to = to + relativedelta(day=1)
#             while date <= to:
#                 dates.append((db_code.get_date_code(date.year * 100 + date.month)))
#                 date = date + relativedelta(months=1)
#
#         lst: list[cn_stats_data.data.models.MetricData] = []
#         resp = stats(zbcode=metric_code.metric_code, datestr=','.join(dates), dbcode=db_code.code)
#         if resp is None:
#             return lst
#
#         for n in resp:
#             code = cn_stats_data.data.models.MetricCode(
#                 metric_code=n[1],
#                 db_code=db_code,
#                 name=n[0],
#                 parent=metric_code)
#             data = cn_stats_data.data.models.MetricData(
#                 metric_code=code,
#                 date_num= db_code.get_date_num(n[2]),
#                 metric_value=n[3])
#             lst.append(data)
#
#         return lst
#
#
# def main():
#
#     pass
#
#
# if __name__ == "__main__":
#     main()
