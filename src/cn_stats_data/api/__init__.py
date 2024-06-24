import time
from typing import Any

import requests
from functools import cache

from data.db_code import DbCode
from data.metric_code import MetricCode

_header = {
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Referer': 'https://data.stats.gov.cn/easyquery.htm?cn=A01',
    'Host': 'data.stats.gov.cn',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36 Edg/99.0.1150.36',
    'X-Requested-With': 'XMLHttpRequest',
    'Cookie': '_trs_uv=l0krufmy_6_30qm; JSESSIONID=JkGLaObMfWG3_P3_bNKa59cUydvE_nJDUpJOsskem4S-E-wgJeA7!-2135294552; u=1'
}


def _random_timestamp():
    return str(int(round(time.time() * 1000)))


def _adv_api(db_code: str, wd: str, tree_id: str, language: str = '') -> list[Any]:
    """
    Invoke adv api to get metric codes or region codes information
    :param db_code: specific the codes belong to which db code
    :param wd: zb - to get metric codes, reg - to get region codes.
    :param tree_id: the parent id of the codes.
        for the root level codes, zb - for metric codes, reg - for region codes.
    :param language: '' - for the name or other data in Chinese, 'english' - for english
    :return:
    """
    url = f'https://data.stats.gov.cn{language}/adv.htm'
    obj = {
        'm': 'findZbXl',
        'dbcode': db_code,
        'wd': wd,
        'treeId': tree_id
    }
    r = requests.get(url, params=obj, headers=_header, verify=False)
    return r.json()


@cache
def retrieve_metric_codes(
        db_code: DbCode,
        metric_code: str = 'zb',
        language: str | None = None) -> list[MetricCode]:
    """
    Downloads metric codes from CN-Stats via the getTree API
    :param db_code: Downloads the codes under the specified db code.
    :param metric_code: Downloads the metric and its descendants. If it's None means downloads all the codes
    :param language: download data with specific language. None means Chinese, and 'english' is another option.
    :return: Returns the metric codes returned by the API
    """
    lst = []
    parents = []
    for n in _adv_api(db_code=db_code.code, wd='zb', tree_id=metric_code, language=language):
        lst.append(MetricCode(
            code=n['id'],
            db_code=db_code,
            name=n['name'],
            parent=n['pid'],
            exp=n['exp'],
            memo=n['memo'],
            unit=n['unit']))
        if n['isParent']:
            parents.append(n["id"])

    for item in parents:
        time.sleep(2)  # sleep two seconds to reduce the access frequency to the API
        lst.extend(retrieve_metric_codes(db_code=db_code, metric_code=item, language=language))

    return lst
