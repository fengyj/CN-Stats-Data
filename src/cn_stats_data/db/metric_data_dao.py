import json
from data.db_code import DbCode
from data.region_code import RegionCode
from data.metric_data import MetricData
from data.metric_code import MetricCode
import db


class MetricDataDao:
    """
    The class for interacting with database.
    """

    @classmethod
    def add_or_update(cls, lst: list[MetricData]) -> int:
        """
        Insert or update the data to database.
        :param lst: The list of metric data
        :return: The record count are saved
        """

        if not lst:
            return 0
        data = [
            (
                i.metric_code.code,
                i.metric_code.db_code.code,
                i.date_num,
                '' if i.region_code is None else i.region_code.code,
                i.metric_value,
                json.dumps(i.extra_attributes)
            )
            for i in lst if not i.is_deleted]
        if len(data) == 0:
            return 0

        sql = """        
INSERT INTO cn_stats_metric_data AS t (
    metric_code, 
    db_code, 
    date_num, 
    region_code,
    metric_value, 
    extra_attributes,
    is_deleted,
    created_time, 
    last_updated_time) 
VALUES(%s, %s, %s, %s, %s, %s::JSONB, False, now(), now()) 
ON CONFLICT(metric_code, db_code, date_num, region_code) 
DO UPDATE SET 
    metric_value = EXCLUDED.metric_value, 
    extra_attributes = EXCLUDED.extra_attributes,
    is_deleted = False,
    last_updated_time = EXCLUDED.last_updated_time 
WHERE t.metric_value <> EXCLUDED.metric_value
    OR t.extra_attributes <> EXCLUDED.extra_attributes
    OR t.is_deleted = True;
        """
        with db.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(sql, data)
                return cursor.rowcount

    @classmethod
    def delete(cls, lst: list[MetricData]) -> int:
        """
        Delete the data from database
        :param lst: the list of metric data
        :return: The record count are deleted
        """

        if lst is None or len(lst) == 0:
            return 0
        data = [
            (
                i.metric_code.code,
                i.metric_code.db_code.code,
                i.date_num,
                '' if i.region_code is None else i.region_code.code
            )
            for i in lst if i.is_deleted
        ]
        if len(data) == 0:
            return 0

        sql = """
UPDATE cn_stats_metric_data SET
    is_deleted = True,
    last_updated_time = now() 
WHERE metric_code = %s 
    AND db_code = %s 
    AND date_num = %s
    AND region_code = %s
    AND is_deleted = False;
        """

        with db.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(sql, data)
                return cursor.rowcount

    @classmethod
    def list(
            cls,
            db_codes: list[str] | None = None,
            metric_codes: list[str] | None = None,
            region_codes: list[str | None] | None = None,
            date_nums: list[int] | None = None
    ) -> list[MetricData]:
        """
        Get metric data by the giving criteria
        :param db_codes: Specific the db codes or None for all db codes
        :param metric_codes:  Specific the metric codes or None for metrics
        :param region_codes: Specific the region codes or None for metrics
        :param date_nums: Specific the date nums or None for metrics
        :return: Returns the metric data for the specified criteria
        """

        sql = """
SELECT 
    metric_code, 
    db_code, 
    date_num, 
    region_code, 
    metric_value, 
    extra_attributes, 
    is_deleted, 
    created_time, 
    last_updated_time
FROM cn_stats_metric_data
WHERE (%s OR metric_code = ANY(%s))
    AND (%s OR db_code = ANY(%s))
    AND (%s OR date_num = ANY(%s))
    AND (%s OR region_code = ANY(%s));
        """
        with db.get_conn() as conn:
            with conn.cursor() as cursor:
                criteria = (
                    metric_codes is None, [] if metric_codes is None else metric_codes,
                    db_codes is None, [] if db_codes is None else db_codes,
                    date_nums is None, [] if date_nums is None else date_nums,
                    region_codes is None, [] if region_codes is None else [
                        '' if i is None else i
                        for i in region_codes
                    ]
                )
                cursor.execute(sql, criteria)
                data = [
                    MetricData(
                        metric_code=MetricCode(code=i[0], db_code=DbCode.get_code(i[1]), name=None),
                        date_num=i[2],
                        region_code=None if i[3] is None else RegionCode(
                            code=i[3],
                            db_code=DbCode.get_code(i[1]), name=None),
                        metric_value=i[4],
                        **i[5],
                        is_deleted=i[6],
                        created_time=i[7],
                        last_updated_time=i[8]
                    )
                    for i in cursor.fetchall()]
                return data
