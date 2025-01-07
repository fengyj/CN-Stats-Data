import logging
from typing import List, Optional

from cn_stats_util.models import Region, Category
from cn_stats_util.apis import ChinaStatsDataApis
from cn_stats_data.db.region_code_dao import RegionCodeDao
from cn_stats_data.db.models import RegionCode, RegionCodeDownloadCheckpoint
from cn_stats_data.db.process_data_dao import ProcessDataDao

__all__ = ["download_region_codes"]


def download_region_codes(
    db_code: Optional[Category] = None, 
    region_code: Optional[str] = None
) -> None:
    """
    Download region codes and save them to the database.
    :param db_code: Specify which db_code's region codes should be downloaded. None means to download all.
    :param region_code: Specify which region code and its descendants need to be downloaded.
        None means all the codes of the db_code will be downloaded.
    """
    logger = logging.getLogger(__name__)

    checkpoint = ProcessDataDao.get_region_code_download_checkpoint() or RegionCodeDownloadCheckpoint(
        db_code=db_code.db_code if db_code else None, region_code=region_code
    )
    checkpoint.reset_if_parameters_changed(db_code=db_code.db_code if db_code else None, region_code=region_code)

    # Determine the categories to download
    db_codes: List[Category] = [db_code] if db_code else list(Category)
    db_codes = [i for i in db_codes if i.is_regional()]
    if len(db_codes) == 0:
        logger.info("No regional db_code to download.")
        return
    logger.info(
        f"Starting to download region codes from data.stats.gov.cn, "
        f'db_code is {",".join([i.db_code for i in db_codes])}, and region_code is {region_code}.'
    )

    for db in db_codes:
        if checkpoint.need_skip_db(db.db_code):
            logger.info(f"Skip db_code {db.db_code} because of the checkpoint.")
            continue     

        ProcessDataDao.add_or_update_region_code_download_checkpoint(checkpoint)

        parent = Region.of(db_code=db.db_code, code=region_code)
        # Get the codes from the database for comparison
        codes_in_db = RegionCodeDao.list(db, region_code)
        logger.info(f"Loaded {len(codes_in_db)} region codes from the database.")

        # Create a map of existing codes for comparison
        existing_codes_map = {c.code: c for c in codes_in_db}

        _download_region_code(
            db_code=db,
            region=parent,
            regions_in_db=existing_codes_map,
            checkpoint=checkpoint,
            logger=logger,
        )

        logger.info(
            f"Downloaded all descendant region codes of {region_code} for db_code {db.db_code}."
        )

    checkpoint.finish()
    ProcessDataDao.add_or_update_region_code_download_checkpoint(checkpoint)
    logger.info("All codes have been downloaded.")


def _download_region_code(
    db_code: Category,
    region: Region,
    regions_in_db: dict[str, RegionCode],
    checkpoint: RegionCodeDownloadCheckpoint,
    logger: logging.Logger,
) -> None:
    """
    Download a single region code and its descendants.
    :param db_code: The db_code of the region code to download.
    :param region: The region to download.
    :param regions_in_db: The existing regions in the database.
    :param checkpoint: The checkpoint for tracking download progress.
    :param logger: The logger instance.
    """
    if checkpoint.need_skip_region(region=region):
        logger.info(f"Skip region {region.code} because of the checkpoint.")
        return

    # Fetch the region code from the API
    children_downloaded = ChinaStatsDataApis().fetch_regions(
        db_code, parent=region, recursive_fetch=False
    )
    logger.info(
        f"Downloaded {len(children_downloaded)} children region codes of {region.code} for db_code {db_code.db_code}."
    )

    # Get the codes from the database for comparison
    children_in_db = (
        regions_in_db.get(region.code, Region.of(db_code.db_code)).children or []
    )
    if not region.code:
        children_in_db = [i for i in regions_in_db.values() if i.parent is None]
    logger.info(f"Loaded {len(children_in_db)} region codes from the database.")

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

    # Update and delete region codes in the database
    updated_count = RegionCodeDao.add_or_update(data_to_update)
    deleted_count = RegionCodeDao.delete(data_to_delete)
    ProcessDataDao.add_or_update_region_code_download_checkpoint(checkpoint)
    logger.info(
        f"Updated {updated_count} region codes of {db_code.db_code}, and deleted {deleted_count}."
    )

    if region._further_fetch:
        for child in children_downloaded:
            _download_region_code(
                db_code=db_code,
                region=child,
                regions_in_db=regions_in_db,
                checkpoint=checkpoint,
                logger=logger
            )
    else:
        logger.info(f"Skip further fetch for grandchildren of region {region.code}, because its _further_fetch is false.")
