from functools import cache
import logging
import time
from typing import List, Optional

from cn_stats_util.models import Category, Metric, Region, HistoricalData
from cn_stats_data.db.metric_code_dao import MetricCodeDao
from cn_stats_data.db.region_code_dao import RegionCodeDao
from cn_stats_data.db.metric_data_dao import MetricDataDao
from cn_stats_data.db.models import MetricCode, RegionCode, MetricHistoricalData
from cn_stats_util.apis import ChinaStatsDataApis

__all__ = ['download_metric_data']


@cache
def _load_metric_codes(db: Category) -> list[MetricCode]:
    return MetricCodeDao.list(db)

def _find_metric_codes_to_download(db: Category, metric_codes: List[str] | None) -> List[MetricCode]:
    codes_in_db = _load_metric_codes(db)

    if metric_codes:
        return [c for c in codes_in_db if c.code in metric_codes]  # ignore the codes doesn't belong to the db_code
    else:
        return [c for c in codes_in_db if c.parent is None]  # all the codes in the db code if not giving

def _get_non_parent_codes(c: MetricCode, lst: List[MetricCode]) -> None:
    if c.children is None or len(c.children) == 0:
        lst.append(c)
    else:
        for child in c.children:
            _get_non_parent_codes(child, lst)

def _get_metric_codes_to_download(db: Category, metric_codes: Optional[List[str]]) -> List[MetricCode]:
    """get the metric codes need to be downloaded according to the giving codes."""

    root_codes = _find_metric_codes_to_download(db, metric_codes)

    non_parent_codes = []
    for c in root_codes:
        _get_non_parent_codes(c, non_parent_codes)

    codes_to_download: dict[str, MetricCode] = {}
    if db.is_regional():
        for c in non_parent_codes:  # have to download it one by one if want to get all the regions at once
            codes_to_download[c.code] = c
    else:
        for c in non_parent_codes:  # get parent of non-parent codes for downloading
            if c.parent is not None:
                codes_to_download[c.parent.code] = c.parent

    for c in root_codes:
        if c.children is None:
            codes_to_download[c.code] = c

    return [c for c in codes_to_download.values()]


def _download_metric_data(
        db: Category,
        code: Metric,
        years: List[int],
        logger: logging) -> None:
    
    apis = ChinaStatsDataApis()

    data_loaded = apis.fetch_history(
        category=db,
        metrics=[code.code],
        years=years,
        is_row_region=db.is_regional()
    )
    logger.info(f'Received {len(data_loaded)} records of {db.db_code}-{code.code}.')

    metric_codes_in_db = [code.code] if db.is_regional() else [c.code for c in code.children]

    data_in_db = MetricDataDao.list(
        db_codes=[db.db_code], 
        metric_codes=metric_codes_in_db,
        region_codes=None,
        date_nums=db.get_periods_from_years(years))
    logger.info(f'Loaded {len(data_in_db)} metric historcial data of {db.db_code}-{metric_codes_in_db} from database.')

    dict_of_downloaded = {(d.metric_code, d.period, d.region_code): d for d in data_loaded}    
    data_to_delete = []

    for c in data_in_db:
        d = dict_of_downloaded.get((c.metric_code, c.period, c.region_code))
        if d is None:
            c.is_deleted = True
            data_to_delete.append(c)
        elif HistoricalData.__eq__(d, c):
            dict_of_downloaded.pop((c.metric_code, c.period, c.region_code))

    data_to_update = list(dict_of_downloaded.values())

    # save the metric data which is not marked as deleted
    data_updated = MetricDataDao.add_or_update(data_to_update)
    # delete the metric data which is marked as deleted
    data_deleted = MetricDataDao.delete(data_to_delete)
    logger.info(f'Updated {data_updated} metric historical data of {db.db_code}-{metric_codes_in_db}, and deleted {data_deleted}.')


def download_metric_data(
        db_code: Optional[Category] = None,
        metric_code: Optional[str] = None,        
        years: Optional[List[int]] = None,) -> None:
    
    logger = logging.getLogger(__name__)
    if not years: 
        years = [x for x in range(time.localtime().tm_year - 4, time.localtime().tm_year + 1)]
    logger.info(f'Starts to download metric data from data.stats.gov.cn, db_code: {db_code.db_code}, metric_code: {metric_code}, years: {years}.')
    
    db_codes: List[Category] = [db_code] if db_code else list(Category)
    
    # TODO: load checkpoint

    for db in db_codes:

        # TODO: check checkpoint

        codes_to_download = _get_metric_codes_to_download(db, [metric_code] if not metric_code else None)

        count = 0
        total = len(codes_to_download)
        logger.info(f'{len(codes_to_download)} metric codes in {db.db_code} need to be downloaded.')

        for code in codes_to_download:
            #TODO: check checkpoint
            _download_metric_data(db, code, years, logger)
            count += 1
            logger.info(f'Progress of {db.db_code}: {count}/{total}.')
            #TODO: update checkpoint

    # TODO: update checkpoint
    logger.info('All data has been downloaded.')
