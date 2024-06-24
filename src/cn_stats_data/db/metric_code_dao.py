import json
from data.db_code import DbCode
from data.metric_code import MetricCode
import db


class MetricCodeDao:
    """
    The class for interacting with database.
    """

    @classmethod
    def add_or_update(cls, lst: list[MetricCode]) -> int:
        """
        Insert or update the data to database.
        :param lst: The list of metric codes
        :return: The record count are saved
        """

        if not lst:
            return 0
        data = [
            (
                i.code,
                i.db_code.code,
                i.name,
                i.explanation,
                i.memo,
                i.unit,
                i.parent if i.parent is None else i.parent.code,
                json.dumps(i.extra_attributes)
            )
            for i in lst if not i.is_deleted]
        if len(data) == 0:
            return 0

        sql = """        
INSERT INTO cn_stats_metric_codes AS t (
    metric_code, 
    db_code, 
    name, 
    explanation, 
    memo, 
    unit,
    parent_metric_code, 
    extra_attributes,
    is_deleted,
    created_time, 
    last_updated_time) 
VALUES(%s, %s, %s, %s, %s, %s, %s, %s::JSONB, False, now(), now()) 
ON CONFLICT(metric_code, db_code) 
DO UPDATE SET 
    name = EXCLUDED.name, 
    explanation = EXCLUDED.explanation,
    memo = EXCLUDED.memo,
    unit = EXCLUDED.unit,
    parent_metric_code = EXCLUDED.parent_metric_code, 
    extra_attributes = EXCLUDED.extra_attributes,
    is_deleted = False,
    last_updated_time = EXCLUDED.last_updated_time 
WHERE t.name <> EXCLUDED.name 
    OR t.explanation <> EXCLUDED.explanation 
    OR t.memo <> EXCLUDED.memo 
    OR t.unit <> EXCLUDED.unit 
    OR t.parent_metric_code <> EXCLUDED.parent_metric_code
    OR t.extra_attributes <> EXCLUDED.extra_attributes
    OR t.is_deleted = True;
        """
        with db.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(sql, data)
                return cursor.rowcount

    @classmethod
    def delete(cls, lst: list[MetricCode]) -> int:
        """
        Delete the data from database
        :param lst: the list of metric codes
        :return: The record count are deleted
        """

        if not lst:
            return 0
        data = [(i.code, i.db_code.code) for i in lst if i.is_deleted]
        if len(data) == 0:
            return 0

        sql = """
UPDATE cn_stats_metric_codes SET
    is_deleted = True,
    last_updated_time = now()
WHERE metric_code = %s 
    AND db_code = %s
    AND is_deleted = False;
        """

        with db.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(sql, data)
                return cursor.rowcount

    @classmethod
    def get(cls, metric_code: str, db_code: DbCode) -> MetricCode | None:
        """
        Get metric code from DB
        :param metric_code: code of the metric
        :param db_code: db code of the metric
        :return: Returns the metric code object if found, otherwise returns None
        """

        sql = """
SELECT 
    metric_code, 
    db_code, 
    name, 
    explanation, 
    memo, 
    unit,
    parent_metric_code, 
    extra_attributes,
    is_deleted,
    created_time, 
    last_updated_time 
FROM cn_stats_metric_codes 
WHERE metric_code = %s AND db_code = %s;
        """

        with db.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (metric_code, db_code.code))
                data = [
                    MetricCode(
                        code=i[0],
                        db_code=DbCode.get_code(i[1]),
                        name=i[2],
                        explanation=i[3],
                        memo=i[4],
                        unit=i[5],
                        parent=i[6],
                        **i[7],
                        is_deleted=i[8],
                        created_time=i[9],
                        last_updated_time=i[10]
                    )
                    for i in cursor.fetchall()]
                if len(data) == 0:
                    return None
                else:
                    return data[0]

    @classmethod
    def list(cls, db_code: DbCode | None = None, metric_code: str | None = None) -> list[MetricCode]:
        """
        Get metrics via db code and metric code and its descendants
        :param db_code: Specific the db code or None for all db codes
        :param metric_code:  Specific the metric code or None for metrics
        :return: Returns the metrics and its descendants
        """

        sql = """
WITH RECURSIVE cte_metrics (
    metric_code, 
    db_code, 
    name, 
    explanation, 
    memo, 
    unit,
    parent_metric_code, 
    extra_attributes,
    is_deleted,
    created_time, 
    last_updated_time )
AS(
    SELECT 
        metric_code, 
        db_code, 
        name, 
        explanation, 
        memo, 
        unit,
        parent_metric_code, 
        extra_attributes,
        is_deleted,
        created_time, 
        last_updated_time 
    FROM cn_stats_metric_codes 
    WHERE (%s OR metric_code = %s) AND (%s OR db_code = %s)
    UNION
    SELECT 
        c.metric_code, 
        c.db_code, 
        c.name, 
        c.explanation, 
        c.memo, 
        c.unit,
        c.parent_metric_code, 
        c.extra_attributes,
        c.is_deleted,
        c.created_time, 
        c.last_updated_time 
    FROM cn_stats_metric_codes c
    INNER JOIN cte_metrics r ON c.metric_code = r.parent_metric_code AND c.db_code = r.db_code
) 
SELECT * FROM cte_metrics;        
        """
        with db.get_conn() as conn:
            with conn.cursor() as cursor:
                dbcode = None if db_code is None else db_code.code
                cursor.execute(sql, (metric_code is None, metric_code, dbcode is None, dbcode))
                data = [
                    MetricCode(
                        code=i[0],
                        db_code=DbCode.get_code(i[1]),
                        name=i[2],
                        explanation=i[3],
                        memo=i[4],
                        unit=i[5],
                        parent=i[6],
                        **i[7],
                        is_deleted=i[8],
                        created_time=i[9],
                        last_updated_time=i[10]
                    )
                    for i in cursor.fetchall()]
                return data
