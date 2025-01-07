from itertools import groupby
from typing import List, Optional
import json
from cn_stats_data import db
from cn_stats_data.db.models import MetricCodeDownloadCheckpoint, ProcessData, RegionCodeDownloadCheckpoint

__all__ = ["ProcessDataDao"]

class ProcessDataDao:

    METRIC_CODE_DOWNLOAD_ID: str = "metric_code_download"
    REGION_CODE_DOWNLOAD_ID: str = "region_code_download"

    @classmethod
    def add_or_update(cls, data: ProcessData) -> int:

        sql = """
INSERT INTO process_data AS t (
    process_id,
    data,
    created_time,
    last_updated_time)
VALUES(%s, %s, now(), now())
ON CONFLICT(process_id)
DO UPDATE SET
    data = EXCLUDED.data,
    last_updated_time = now()
WHERE t.process_id = EXCLUDED.process_id;
        """
        with db.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (data.process_id, data.data))
                return cursor.rowcount
            
    @classmethod
    def get(cls, process_id: str) -> Optional[ProcessData]:
        sql = """
SELECT process_id, data FROM process_data WHERE process_id = %s;
        """
        with db.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (process_id,))
                row = cursor.fetchone()
                if row is None:
                    return None
                return ProcessData(process_id=row[0], data=json.dumps(row[1]))
            
    @classmethod
    def delete(cls, process_id: str) -> int:
        sql = """
DELETE FROM process_data WHERE process_id = %s;
        """
        with db.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (process_id,))
                return cursor.rowcount
            
    @classmethod
    def add_or_update_metric_code_download_checkpoint(cls, data: MetricCodeDownloadCheckpoint) -> int:
        cls.add_or_update(ProcessData(cls.METRIC_CODE_DOWNLOAD_ID, data.to_json()))

    @classmethod
    def get_metric_code_download_checkpoint(cls) -> Optional[MetricCodeDownloadCheckpoint]:
        data = cls.get(cls.METRIC_CODE_DOWNLOAD_ID)
        return MetricCodeDownloadCheckpoint.from_json(data.data) if data and data.data else None
    
    @classmethod
    def add_or_update_region_code_download_checkpoint(cls, data: RegionCodeDownloadCheckpoint) -> int:
        cls.add_or_update(ProcessData(cls.REGION_CODE_DOWNLOAD_ID, data.to_json()))

    @classmethod
    def get_region_code_download_checkpoint(cls) -> Optional[RegionCodeDownloadCheckpoint]:
        data = cls.get(cls.REGION_CODE_DOWNLOAD_ID)
        return RegionCodeDownloadCheckpoint.from_json(data.data) if data and data.data else None