import logging
from data.metric_code import MetricCode
from data.db_code import DbCode
from data.region_code import RegionCode
from data.metric_data import MetricData
from api.cn_stats_api import CNStatsAPIs
from api.models import *
from dataclasses import asdict
from db.metric_code_dao import MetricCodeDao
from db.metric_data_dao import MetricDataDao
from db.region_code_dao import RegionCodeDao


def download_metric_codes(db_code: str | None = None, metric_code: str | None = None):
    logger = logging.getLogger(__name__)
    db_codes: list[DbCode] = [DbCode.get_code(db_code)] if db_code is not None else [i for i in DbCode]
    logger.info(f'Starts to download metric codes from data.stats.gov.cn, '
                f'db_code is {','.join([i.code for i in db_codes])}, and metric_code is {metric_code}.')

    for db in db_codes:

        codes_in_db = MetricCodeDao.list(db)
        logger.info(f'Loaded {len(codes_in_db)} metric codes from database.')

        data: list[MetricCodeResponseData] = CNStatsAPIs.retrieve_metric_codes(db.code, metric_code)
        metric_codes: list[MetricCode] = []
        for i in data:
            code = MetricCode(
                code=i.code,
                db_code=db,
                name=i.name,
                parent=i.parent if i.parent != 'zb' else None,
                is_deleted=False,
                explanation=i.exp,
                memo=i.memo,
                unit=i.unit
            )
            metric_codes.append(code)
        logger.info(f'Downloaded {len(metric_codes)} metric codes for db_code {db.code}.')
        logger.info(f'Starts to download english information for db_code {db.code}.')
        data: list[MetricCodeResponseData] = CNStatsAPIs.retrieve_metric_codes(db.code, metric_code, 'english')
        for i in data:
            code = MetricCode(
                code=i.code,
                db_code=db,
                name=None,
                english_name=i.name,
                english_explanation=i.exp,
                english_memo=i.memo,
                english_unit=i.unit
            )

        dict_of_downloaded = {i.code for i in metric_codes}
        for c in codes_in_db:
            if c.code not in dict_of_downloaded:
                c.is_deleted = True

        updated_count = MetricCodeDao.add_or_update(metric_codes)
        deleted_count = MetricCodeDao.delete(codes_in_db)
        logger.info(f'Updated {updated_count} metric codes, and deleted {deleted_count}.')

    logger.info(f'All codes have been downloaded.')
    pass


def download_region_codes(db_code: str | None = None, region_code: str | None = None) -> None:
    logger = logging.getLogger(__name__)
    db_codes: list[DbCode] = [DbCode.get_code(db_code)] if db_code is not None else DbCode.get_region_codes()
    logger.info(f'Starts to download region codes from data.stats.gov.cn, '
                f'db_code is {','.join([i.code for i in db_codes])}, and region_code is {region_code}.')

    for db in db_codes:

        codes_in_db = RegionCodeDao.list(db)
        logger.info(f'Loaded {len(codes_in_db)} region codes from database.')

        data: list[RegionCodeResponseData] = CNStatsAPIs.retrieve_region_codes(db.code, region_code)
        region_codes: list[RegionCode] = []
        for i in data:
            code = RegionCode(
                code=i.code,
                db_code=db,
                name=i.name,
                parent=i.parent if i.parent != 'reg' else None,
                is_deleted=False,
                explanation=i.exp
            )
            region_codes.append(code)
        logger.info(f'Downloaded {len(region_codes)} region codes for db_code {db.code}.')
        logger.info(f'Starts to download english information for db_code {db.code}.')
        data: list[RegionCodeResponseData] = CNStatsAPIs.retrieve_region_codes(db.code, region_code, 'english')
        for i in data:
            code = RegionCode(
                code=i.code,
                db_code=db,
                name=None,
                english_name=i.name,
                english_explanation=i.exp
            )

        dict_of_downloaded = {i.code for i in region_codes}
        for c in codes_in_db:
            if c.code not in dict_of_downloaded:
                c.is_deleted = True

        updated_count = RegionCodeDao.add_or_update(region_codes)
        deleted_count = RegionCodeDao.delete(codes_in_db)
        logger.info(f'Updated {updated_count} region codes, and deleted {deleted_count}.')

    logger.info(f'All codes have been downloaded.')
    pass


def _get_metric_codes_to_download(db: DbCode, metric_codes: list[str] | None) -> list[MetricCode]:
    """get the metric codes need to be downloaded according to the giving codes."""
    codes_in_db = MetricCodeDao.list(db)
    if metric_codes is None:
        root_codes = [c for c in codes_in_db if c.parent is None]  # all the codes in the db code if not giving
    else:
        root_codes = [c for c in codes_in_db if c.code in metric_codes]
    non_parent_codes = []
    for c in root_codes:
        if c.children is not None:  # get non-parent codes for parent codes
            non_parent_codes.extend(c.get_non_parent_metrics())
    codes_to_download: dict[str, MetricCode] = {}
    if db in DbCode.get_region_codes():
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


def _download_metric_data(db: DbCode, code: MetricCode, years: list[str], row_code: str, logger: logging) -> None:

    response = CNStatsAPIs.retrieve_metric_data(
        row_code=row_code,
        col_code='sj',
        db_code=db.code,
        metric_code=code.code,
        dates=years
    )
    logger.info(f'Received {len(response)} records of {code.code}.')

    data_to_update = []
    codes_to_update = {}
    for i in response:

        c = MetricCode(code=i.metric_code, db_code=db, name=i.metric_node.name)
        c.memo = i.metric_node.memo
        c.explanation = i.metric_node.exp
        c.unit =i.metric_node.unit
        codes_to_update[c.code] = c

        r = None
        if i.region_node is not None:
            r = RegionCode(code=i.region_code, db_code=db, name=i.region_node.name)
        data = MetricData(
            metric_code=c,
            date_num=db.get_date_num(i.date_code),
            region_code=i.region_code,
            metric_value=i.data,
            is_deleted=not i.has_data
        )
        data_to_update.append(data)

    # update metric codes for saving the information like memo, unit, etc.
    codes_updated = MetricCodeDao.add_or_update([c for c in codes_to_update.values()])
    logger.info(f'{codes_updated} metric codes are updated.')
    # save the metric data which is not marked as deleted
    data_updated = MetricDataDao.add_or_update(data_to_update)
    logger.info(f'{data_updated} metric data are updated.')
    # delete the metric data which is marked as deleted
    data_deleted = MetricDataDao.delete(data_to_update)
    logger.info(f'{data_deleted} metric data are deleted.')


def download_metric_data(
        years: list[str],
        db_code: str | None = None,
        metric_codes: list[str] | None = None) -> None:

    logger = logging.getLogger(__name__)
    db_codes: list[DbCode] = [DbCode.get_code(db_code)] if db_code is not None else DbCode.get_macro_economic_codes()
    logger.info(f'Starts to download metric data from data.stats.gov.cn, '
                f'db_code is {','.join([i.code for i in db_codes])}.')

    for db in db_codes:

        codes_to_download = _get_metric_codes_to_download(db, metric_codes)
        row_code = 'zb'
        if db in DbCode.get_region_codes():
            row_code = 'reg'

        logger.info(f'Metric codes in {db.code} need to be downloaded: {','.join([c.code for c in codes_to_download])}')

        for code in codes_to_download:
            _download_metric_data(db, code, years, row_code, logger)
    pass
