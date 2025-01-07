from itertools import groupby
import json
from typing import List, Optional

from cn_stats_util.models import Category

from cn_stats_data import db
from cn_stats_data.db.models import MetricCode

__all__ = ["MetricCodeDao"]


class MetricCodeDao:
    """
    The class for interacting with database.
    """

    @classmethod
    def add_or_update(cls, lst: List[MetricCode]) -> int:
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
                i.db_code,
                i.name,
                i.explanation,
                i.memo,
                i.unit,
                i.parent.code if i.parent else None,
                json.dumps(i.extra_attributes),
            )
            for i in lst
            if not hasattr(i, "is_deleted") or not i.is_deleted
        ]
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
ON CONFLICT(db_code, metric_code) 
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
    OR ((t.explanation IS NULL AND EXCLUDED.explanation IS NOT NULL) 
        OR (t.explanation IS NOT NULL AND EXCLUDED.explanation IS NULL) 
        OR (t.explanation <> EXCLUDED.explanation))
    OR ((t.memo IS NULL AND EXCLUDED.memo IS NOT NULL) 
        OR (t.memo IS NOT NULL AND EXCLUDED.memo IS NULL) 
        OR (t.memo <> EXCLUDED.memo))
    OR ((t.unit IS NULL AND EXCLUDED.unit IS NOT NULL) 
        OR (t.unit IS NOT NULL AND EXCLUDED.unit IS NULL) 
        OR (t.unit <> EXCLUDED.unit))
    OR ((t.parent_metric_code IS NULL AND EXCLUDED.parent_metric_code IS NOT NULL) 
        OR (t.parent_metric_code IS NOT NULL AND EXCLUDED.parent_metric_code IS NULL) 
        OR (t.parent_metric_code <> EXCLUDED.parent_metric_code))
    OR ((t.extra_attributes IS NULL AND EXCLUDED.extra_attributes IS NOT NULL) 
        OR (t.extra_attributes IS NOT NULL AND EXCLUDED.extra_attributes IS NULL) 
        OR (t.extra_attributes <> EXCLUDED.extra_attributes))
    OR t.is_deleted = True;
        """
        with db.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(sql, data)
                return cursor.rowcount

    @classmethod
    def delete(cls, lst: List[MetricCode]) -> int:
        """
        Delete the data from database
        :param lst: the list of metric codes
        :return: The record count are deleted
        """

        if not lst:
            return 0
        data = [(i.db_code, i.code) for i in lst if not hasattr(i, "is_deleted") or i.is_deleted]
        if len(data) == 0:
            return 0

        sql = """
UPDATE cn_stats_metric_codes SET
    is_deleted = True,
    last_updated_time = now()
WHERE db_code = %s
    AND metric_code = %s 
    AND is_deleted = False;
        """

        with db.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(sql, data)
                return cursor.rowcount

    @classmethod
    def get(cls, metric_code: str, db_code: Category) -> MetricCode | None:
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
    last_updated_time,
    CASE WHEN EXISTS (SELECT 1 FROM cn_stats_metric_codes WHERE is_deleted = FALSE AND db_code = %s AND parent_metric_code = %s) 
         THEN TRUE 
         ELSE FALSE END AS is_parent
FROM cn_stats_metric_codes 
WHERE is_deleted = FALSE AND db_code = %s AND metric_code = %s;
        """

        with db.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (db_code.db_code, metric_code, db_code.db_code, metric_code))
                data = [
                    MetricCode(
                        code=i[0],
                        db_code=i[1],
                        name=i[2],
                        explanation=i[3],
                        is_parent=i[11],
                        memo=i[4],
                        unit=i[5],
                        parent=(
                            MetricCode.of(id=i[6], db_code=i[1])
                            if i[6]
                            else None
                        ),
                        **i[7],
                        is_deleted=i[8],
                        created_time=i[9],
                        last_updated_time=i[10],
                    )
                    for i in cursor.fetchall()
                ]
                if len(data) == 0:
                    return None
                else:
                    return data[0]

    @classmethod
    def list(
        cls, db_code: Category | None = None, metric_code: str | None = None
    ) -> List[MetricCode]:
        """
        Get metrics via db code and metric code and its descendants
        :param db_code: Specific the db code or None for all db codes
        :param metric_code:  Specific the metric code or None for metrics
        :return: Returns the metrics and its descendants
        """

        def manipulate_children(
            parent_dict: dict[(str, str), List[MetricCode]], parent: Optional[MetricCode] = None
        ) -> None:

            if parent is None:
                db_codes = [db_code] if db_code else list(Category)
                for c in db_codes:
                    children = parent_dict.get((c.db_code, ""), [])
                    for i in children:
                        manipulate_children(parent_dict, i)
            else:
                children = parent_dict.get((parent.db_code, parent.code), [])
                parent.children = children if parent.code else None
                for i in children:
                    manipulate_children(parent_dict, i)

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
    last_updated_time)
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
    WHERE is_deleted = FALSE AND (%s OR db_code = %s) AND (%s OR metric_code = %s) 
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
        INNER JOIN cte_metrics r ON c.db_code = r.db_code AND c.parent_metric_code = r.metric_code AND c.is_deleted = FALSE
) 
SELECT * FROM cte_metrics;        
        """
        with db.get_conn() as conn:
            with conn.cursor() as cursor:
                dbcode = None if db_code is None else db_code.db_code
                cursor.execute(
                    sql, (dbcode is None, dbcode, metric_code is None, metric_code)
                )
                data = [
                    MetricCode(
                        code=i[0],
                        db_code=i[1],
                        name=i[2],
                        explanation=i[3],
                        is_parent=False,
                        memo=i[4],
                        unit=i[5],
                        parent=(
                            MetricCode(
                                code=i[6],
                                db_code=i[1],
                                name=None,
                                explanation=None,
                                is_parent=False,
                            )
                            if i[6]
                            else None
                        ),
                        children=None,
                        is_deleted=i[8],
                        created_time=i[9],
                        last_updated_time=i[10],
                        **i[7],
                    )
                    for i in cursor.fetchall()
                ]

                parent_dict = {}
                for i in data:
                    if i.parent:
                        parent_dict.setdefault((i.db_code, i.parent.code or ""), []).append(i)
                    else:
                        parent_dict.setdefault((i.db_code, ""), []).append(i)
                manipulate_children(parent_dict)

                return data
