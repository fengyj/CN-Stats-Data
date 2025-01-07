import logging
from typing import List, Optional

from cn_stats_util.models import Metric, Category
from cn_stats_util.apis import ChinaStatsDataApis
from cn_stats_data.db.metric_code_dao import MetricCodeDao
from cn_stats_data.db.models import MetricCode, MetricCodeDownloadCheckpoint
from cn_stats_data.db.process_data_dao import ProcessDataDao

__all__ = ["download_metric_codes"]


def download_metric_codes(
    db_code: Optional[Category] = None, 
    metric_code: Optional[str] = None
) -> None:
    """
    Download metric codes and save them to the database.
    :param db_code: Specify which db_code's metric codes should be downloaded. None means to download all.
    :param metric_code: Specify which metric code and its descendants need to be downloaded.
        None means all the codes of the db_code will be downloaded.
    """
    logger = logging.getLogger(__name__)

    checkpoint = ProcessDataDao.get_metric_code_download_checkpoint() or MetricCodeDownloadCheckpoint(
        db_code=db_code.db_code if db_code else None, metric_code=metric_code
    )
    checkpoint.reset_if_parameters_changed(db_code=db_code.db_code if db_code else None, metric_code=metric_code)

    # Determine the categories to download
    db_codes: List[Category] = [db_code] if db_code else list(Category)
    logger.info(
        f"Starting to download metric codes from data.stats.gov.cn, "
        f'db_code is {",".join([i.db_code for i in db_codes])}, and metric_code is {metric_code}.'
    )

    for db in db_codes:

        if checkpoint.need_skip_db(db.db_code):
            logger.info(f"Skip db_code {db.db_code} because of the checkpoint.")
            continue     

        ProcessDataDao.add_or_update_metric_code_download_checkpoint(checkpoint)

        parent = Metric.of(db_code=db.db_code, code=metric_code)
        # Get the codes from the database for comparison
        codes_in_db = MetricCodeDao.list(db, metric_code)
        logger.info(f"Loaded {len(codes_in_db)} metric codes from the database.")

        # Create a map of existing codes for comparison
        existing_codes_map = {c.code: c for c in codes_in_db}

        _download_metric_code(
            db_code=db,
            metric=parent,
            metrics_in_db=existing_codes_map,
            checkpoint=checkpoint,
            logger=logger,
        )

        logger.info(
            f"Downloaded all descendant metric codes of {metric_code} for db_code {db.db_code}."
        )

    checkpoint.finish()
    ProcessDataDao.add_or_update_metric_code_download_checkpoint(checkpoint)
    logger.info("All codes have been downloaded.")


def _download_metric_code(
    db_code: Category,
    metric: Metric,
    metrics_in_db: dict[str, MetricCode],
    checkpoint: MetricCodeDownloadCheckpoint,
    logger: logging.Logger,
) -> None:
    """
    Download a single metric code and its descendants.
    :param db_code: The db_code of the metric code to download.
    :param metric_code: The code of the metric code to download.
    """

    if checkpoint.need_skip_metric(metric=metric):
        logger.info(f"Skip metric {metric.code} because of the checkpoint.")
        return

    # Fetch the metric code from the API
    children_downloaded = ChinaStatsDataApis().fetch_metrics(
        db_code, parent=metric, recursive_fetch=False
    )
    logger.info(
        f"Downloaded {len(children_downloaded)} children metric codes of {metric.code} for db_code {db_code.db_code}."
    )

    # Get the codes from the database for comparison
    children_in_db = (
        metrics_in_db.get(metric.code, Metric.of(db_code.db_code)).children or []
    )
    if not metric.code:
        children_in_db = [i for i in metrics_in_db.values() if i.parent is None]
    logger.info(f"Loaded {len(children_in_db)} metric codes from the database.")

    # Create a map of existing codes for comparison
    existing_codes_map = {c.code: c for c in children_in_db}

    # Determine which codes need to be updated or deleted
    data_to_update = []
    data_to_delete = []
    for child in children_downloaded:
        existing_code = existing_codes_map.get(child.code)
        if existing_code:
            if not existing_code.__eq__(child):
                data_to_update.append(child)
            existing_codes_map.pop(child.code)
        else:
            data_to_update.append(child)

    data_to_delete = list(existing_codes_map.values())

    # Update and delete metric codes in the database
    updated_count = MetricCodeDao.add_or_update(data_to_update)
    deleted_count = MetricCodeDao.delete(data_to_delete)
    ProcessDataDao.add_or_update_metric_code_download_checkpoint(checkpoint)
    logger.info(
        f"Updated {updated_count} metric codes of {db_code.db_code}, and deleted {deleted_count}."
    )

    if metric._further_fetch:
        for child in children_downloaded:
            _download_metric_code(
                db_code=db_code, metric=child, metrics_in_db=metrics_in_db, checkpoint=checkpoint, logger=logger
            )
    else:
        logger.info(f"Skip further fetch for grandchildren of metric {metric.code}, because its __further_fetch is false.")
