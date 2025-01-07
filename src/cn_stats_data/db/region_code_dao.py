from itertools import groupby
from typing import List, Optional
import json
from cn_stats_data import db
from cn_stats_util.models import Category
from cn_stats_data.db.models import RegionCode

__all__ = ["RegionCodeDao"]


class RegionCodeDao:
    """
    The class for interacting with database.
    """

    @classmethod
    def add_or_update(cls, lst: List[RegionCode]) -> int:
        """
        Insert or update the data to database.
        :param lst: The list of region codes
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
                i.children if i.children is None else [c.code for c in i.children],
                json.dumps(i.extra_attributes),
            )
            for i in lst
            if not hasattr(i, "is_deleted") or not i.is_deleted
        ]
        if len(data) == 0:
            return 0

        sql = """        
INSERT INTO cn_stats_region_codes AS t (
    region_code,
    db_code,
    name,
    explanation,
    children_region_codes,
    extra_attributes,
    is_deleted,
    created_time,
    last_updated_time)
VALUES(%s, %s, %s, %s, %s, %s::JSONB, False, now(), now()) 
ON CONFLICT(db_code, region_code) 
DO UPDATE SET 
    name = EXCLUDED.name, 
    explanation = EXCLUDED.explanation,
    children_region_codes = EXCLUDED.children_region_codes, 
    extra_attributes = EXCLUDED.extra_attributes,
    is_deleted = False,
    last_updated_time = EXCLUDED.last_updated_time 
WHERE t.name <> EXCLUDED.name 
    OR ((t.explanation IS NULL AND EXCLUDED.explanation IS NOT NULL) 
        OR (t.explanation IS NOT NULL AND EXCLUDED.explanation IS NULL) 
        OR (t.explanation <> EXCLUDED.explanation))
    OR ((t.children_region_codes IS NULL AND EXCLUDED.children_region_codes IS NOT NULL) 
        OR (t.children_region_codes IS NOT NULL AND EXCLUDED.children_region_codes IS NULL) 
        OR (t.children_region_codes <> EXCLUDED.children_region_codes))
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
    def delete(cls, lst: List[RegionCode]) -> int:
        """
        Delete the data from database
        :param lst: the list of region codes
        :return: The record count are deleted
        """

        if lst is None or len(lst) == 0:
            return 0
        data = [(i.db_code, i.code) for i in lst if not hasattr(i, "is_deleted") or i.is_deleted]
        if len(data) == 0:
            return 0

        sql = """
UPDATE cn_stats_region_codes SET
    is_deleted = True,
    last_updated_time = now()    
WHERE db_code = %s
    AND region_code = %s 
    AND is_deleted = False;
        """

        with db.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(sql, data)
                return cursor.rowcount

    @classmethod
    def get(cls, reg_code: str, db_code: Category) -> RegionCode | None:
        """
        Get metric code from DB
        :param reg_code: code of the region
        :param db_code: db code of the region
        :return: Returns the region code object if found, otherwise returns None
        """

        sql = """
SELECT 
    r.region_code, 
    r.db_code,
    r.name, 
    r.explanation,
    r.children_region_codes, 
    r.extra_attributes,
    r.is_deleted,
    r.created_time, 
    r.last_updated_time,
    p.region_code AS parent_region_code
FROM cn_stats_region_codes r
    LEFT JOIN cn_stats_region_codes p ON r.db_code = p.db_code AND r.region_code = ANY(p.children_region_codes)
WHERE r.is_deleted = FALSE AND r.db_code = %s AND r.region_code = %s;
        """

        with db.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (db_code.db_code, reg_code))
                data = [
                    RegionCode(
                        code=i[0],
                        db_code=i[1],
                        name=i[2],
                        explanation=i[3],
                        is_parent=i[4] is not None,
                        parent=(
                            None
                            if not i[9]
                            else RegionCode(
                                db_code=db_code,
                                code=i[9],
                                name=None,
                                explanation=None,
                                is_parent=False,
                            )
                        ),
                        children=(
                            None
                            if not i[4]
                            else [
                                RegionCode(
                                    db_code=i[1],
                                    code=x,
                                    name=None,
                                    explanation=None,
                                    is_parent=False,
                                )
                                for x in i[4]
                            ]
                        ),
                        is_deleted=i[6],
                        created_time=i[7],
                        last_updated_time=i[8],
                        **i[5],
                    )
                    for i in cursor.fetchall()
                ]
                if len(data) == 0:
                    return None
                else:
                    return data[0]

    @classmethod
    def list(
        cls, db_code: Category | None = None, reg_code: str | None = None
    ) -> list[RegionCode]:
        """
        Get regions via db code and region code and its descendants
        :param db_code: Specific the db code or None for all db codes
        :param reg_code:  Specific the region code or None for metrics
        :return: Returns the regions and its descendants
        """

        def manipulate_children(
            dict: dict[(str, str), list[RegionCode]], parent: Optional[RegionCode] = None
        ) -> None:
            
            if parent is None:
                db_codes = [db_code] if db_code else list(Category)
                for c in db_codes:
                    children = dict.get((c.db_code, ""), [])
                    for i in children:
                        manipulate_children(dict, i)
            else:
                children = dict.get((parent.db_code, parent.code), [])
                parent.children = children if parent.code else None
                for i in children:
                    manipulate_children(dict, i)

        sql = """
WITH RECURSIVE cte_regions (
    region_code, 
    db_code,
    name, 
    explanation,
    children_region_codes, 
    extra_attributes,
    is_deleted,
    created_time, 
    last_updated_time,
    parent_region_code)
AS(
    SELECT 
        r.region_code, 
        r.db_code,
        r.name, 
        r.explanation,
        r.children_region_codes, 
        r.extra_attributes,
        r.is_deleted,
        r.created_time, 
        r.last_updated_time,
        p.region_code AS parent_region_code
    FROM cn_stats_region_codes r
        LEFT JOIN cn_stats_region_codes p ON r.db_code = p.db_code AND r.region_code = ANY(p.children_region_codes)
    WHERE (%s OR r.db_code = %s) AND (%s OR r.region_code = %s)
    UNION
    SELECT 
        c.region_code, 
        c.db_code,
        c.name, 
        c.explanation,
        c.children_region_codes, 
        c.extra_attributes, 
        c.is_deleted, 
        c.created_time, 
        c.last_updated_time,
        r.region_code AS parent_region_code 
    FROM cn_stats_region_codes c
        INNER JOIN cte_regions r ON c.db_code = r.db_code AND c.region_code = ANY(r.children_region_codes)
) 
SELECT * FROM cte_regions;        
        """

        with db.get_conn() as conn:
            with conn.cursor() as cursor:
                dbcode = None if db_code is None else db_code.db_code
                cursor.execute(
                    sql, (dbcode is None, dbcode, reg_code is None, reg_code)
                )
                data = [
                    RegionCode(
                        code=i[0],
                        db_code=i[1],
                        name=i[2],
                        explanation=i[3],
                        is_parent=False,
                        parent=(
                            None
                            if not i[9]
                            else RegionCode(
                                db_code=i[1],
                                code=i[9],
                                name=None,
                                explanation=None,
                                is_parent=False,
                            )
                        ),
                        children=(
                            None
                            if not i[4]
                            else [
                                RegionCode(
                                    db_code=i[1],
                                    code=x,
                                    name=None,
                                    explanation=None,
                                    is_parent=False,
                                )
                                for x in i[4]
                            ]
                        ),
                        is_deleted=i[6],
                        created_time=i[7],
                        last_updated_time=i[8],
                        **i[5],
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
