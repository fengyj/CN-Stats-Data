import json
from data.db_code import DbCode
from data.region_code import RegionCode
import db


class RegionCodeDao:
    """
    The class for interacting with database.
    """

    @classmethod
    def add_or_update(cls, lst: list[RegionCode]) -> int:
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
                i.db_code.code,
                i.name,
                i.explanation,
                i.children if i.children is None else [c.code for c in i.children],
                json.dumps(i.extra_attributes)
            )
            for i in lst if not i.is_deleted]
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
ON CONFLICT(region_code, db_code) 
DO UPDATE SET 
    name = EXCLUDED.name, 
    explanation = EXCLUDED.explanation,
    children_region_codes = EXCLUDED.children_region_codes, 
    extra_attributes = EXCLUDED.extra_attributes,
    is_deleted = False,
    last_updated_time = EXCLUDED.last_updated_time 
WHERE t.name <> EXCLUDED.name 
    OR t.explanation <> EXCLUDED.explanation
    OR t.children_region_codes <> EXCLUDED.children_region_codes
    OR t.extra_attributes <> EXCLUDED.extra_attributes
    OR t.is_deleted = True;
        """
        with db.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(sql, data)
                return cursor.rowcount

    @classmethod
    def delete(cls, lst: list[RegionCode]) -> int:
        """
        Delete the data from database
        :param lst: the list of region codes
        :return: The record count are deleted
        """

        if lst is None or len(lst) == 0:
            return 0
        data = [(i.code, i.db_code.code) for i in lst if i.is_deleted]
        if len(data) == 0:
            return 0

        sql = """
UPDATE cn_stats_region_codes SET
    is_deleted = True,
    last_updated_time = now()    
WHERE region_code = %s 
    AND db_code = %s
    AND is_deleted = False;
        """

        with db.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(sql, data)
                return cursor.rowcount

    @classmethod
    def get(cls, reg_code: str, db_code: DbCode) -> RegionCode | None:
        """
        Get metric code from DB
        :param reg_code: code of the region
        :param db_code: db code of the region
        :return: Returns the region code object if found, otherwise returns None
        """

        sql = """
SELECT 
    region_code, 
    db_code,
    name,
    explanation,
    children_region_codes, 
    extra_attributes,
    is_deleted,
    created_time, 
    last_updated_time
FROM cn_stats_region_codes 
WHERE region_code = %s AND db_code = %s;
        """

        with db.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (reg_code, db_code.code))
                data = [
                    RegionCode(
                        code=i[0],
                        db_code=DbCode.get_code(i[1]),
                        name=i[2],
                        explanation=i[3],
                        children=i[4],
                        **i[5],
                        is_deleted=i[6],
                        created_time=i[7],
                        last_updated_time=i[8]
                    )
                    for i in cursor.fetchall()]
                if len(data) == 0:
                    return None
                else:
                    return data[0]

    @classmethod
    def list(cls, db_code: DbCode | None = None, reg_code: str | None = None) -> list[RegionCode]:
        """
        Get regions via db code and region code and its descendants
        :param db_code: Specific the db code or None for all db codes
        :param reg_code:  Specific the region code or None for metrics
        :return: Returns the regions and its descendants
        """

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
    last_updated_time)
AS(
    SELECT 
        region_code, 
        db_code,
        name, 
        explanation,
        children_region_codes, 
        extra_attributes,
        is_deleted,
        created_time, 
        last_updated_time
    FROM cn_stats_region_codes 
    WHERE (%s OR region_code = %s) AND (%s OR db_code = %s)
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
        c.last_updated_time 
    FROM cn_stats_region_codes c
    INNER JOIN cte_regions r ON c.region_code = ANY(r.children_region_codes) AND c.db_code = r.db_code
) 
SELECT * FROM cte_regions;        
        """

        with db.get_conn() as conn:
            with conn.cursor() as cursor:
                dbcode = None if db_code is None else db_code.code
                cursor.execute(sql, (reg_code is None, reg_code, dbcode is None, dbcode))
                data = [
                    RegionCode(
                        code=i[0],
                        db_code=DbCode.get_code(i[1]),
                        name=i[2],
                        explanation=i[3],
                        children=i[4],
                        **i[5],
                        is_deleted=i[6],
                        created_time=i[7],
                        last_updated_time=i[8]
                    )
                    for i in cursor.fetchall()]
                return data
