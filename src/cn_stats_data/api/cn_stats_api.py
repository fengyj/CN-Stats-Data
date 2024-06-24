from api.models import *
import requests
import urllib3
import logging
from retry import retry


class CNStatsAPIs:
    _header = {
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Referer': 'https://data.stats.gov.cn/easyquery.htm?cn=A01',
        'Host': 'data.stats.gov.cn',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36 Edg/99.0.1150.36',
        'X-Requested-With': 'XMLHttpRequest',
        'Cookie': '_trs_uv=l0krufmy_6_30qm; JSESSIONID=JkGLaObMfWG3_P3_bNKa59cUydvE_nJDUpJOsskem4S-E-wgJeA7!-2135294552; u=1'
    }
    _sleep_seconds: float = 2
    logger = logging.getLogger(__name__)

    @classmethod
    def _random_timestamp(cls):
        return str(int(round(time.time() * 1000)))

    @classmethod
    @retry(tries=3, delay=10, logger=logger)
    def _adv_api(cls, db_code: str, wd: str, tree_id: str, language: str = '') -> list[Any]:
        """
        Invoke adv api to get metric codes or region codes information
        :param db_code: specific the codes belong to which db code
        :param wd: zb - to get metric codes, reg - to get region codes.
        :param tree_id: the parent id of the codes.
            for the root level codes, zb - for metric codes, reg - for region codes.
        :param language: '' - for the name or other data in Chinese, 'english' - for english
        :return:
        """
        if language is None:
            language = ''
        elif len(language) > 0:
            language = '/' + language

        url = f'https://data.stats.gov.cn{language}/adv.htm'
        obj = {
            'm': 'findZbXl',
            'db': db_code,
            'wd': wd,
            'treeId': tree_id
        }
        urllib3.disable_warnings()
        cls.logger.info(f'Starts to invoke adv api, with parameters {str(obj)}.')
        r = requests.get(url, params=obj, headers=cls._header, verify=False)
        return r.json()

    @classmethod
    @retry(tries=3, delay=10, logger=logger)
    def retrieve_metric_data(
            cls,
            row_code: str,
            col_code: str,
            db_code: str,
            metric_code: str,
            dates: list[str],
            region_code: str | None = None,
            language: str = '') -> list[MetricDataResponseData]:
        """
        Invoke easyquery api to get metric codes or region codes information
        :param row_code: specific sj (time), reg (region), zb (metric), which one is the row
        :param col_code: specific sj (time), reg (region), zb (metric), which one is the column
        :param db_code: specific the codes belong to which db code
        :param metric_code: When the metric code is col or row,
            It can be a parent code of the code which is not a parent code,
            so that the API can return all the children metric values.
            it also can be a non parent code.
            If it's either col nor row, then it's only can be the metric which is not parent code.
        :param dates: When the sj (time) is column or row, can specific multiple values,
            otherwise, only one is allowed.
        :param region_code: When the db code is provence data or city data, this is required.
            But when reg (region) is column or row, it can be omitted,
            and then the API will return all data of the regions
        :param language: '' - for the name or other data in Chinese, 'english' - for english
        :return:
        """
        if language is None:
            language = ''
        elif len(language) > 0:
            language = '/' + language

        dfwds = [
            {'wdcode': 'zb', 'valuecode': metric_code},
            {'wdcode': 'sj', 'valuecode': ','.join(dates)}
        ]
        if region_code is not None:
            dfwds.append({'wdcode': 'reg', 'valuecode': region_code})
        url = f'https://data.stats.gov.cn{language}/easyquery.htm'
        obj = {
            'm': 'QueryData',
            'dbcode': db_code,
            'rowcode': row_code,
            'colcode': col_code,
            'wds': json.dumps([]),
            'dfwds': json.dumps(dfwds),
            'k1': cls._random_timestamp()
        }
        urllib3.disable_warnings()
        cls.logger.info(f'Starts to invoke easyquery api, with parameters {str(obj)}.')
        r = requests.get(url, params=obj, headers=cls._header, verify=False)
        return_data = r.json()['returndata']

        zb_nodes: dict[str, MetricDataResponseNode] = {}
        sj_nodes: dict[str, MetricDataResponseNode] = {}
        reg_nodes: dict[str, MetricDataResponseNode] = {}

        for i in return_data['wdnodes']:
            wd_code = i['wdcode']
            match wd_code:
                case 'zb':
                    zb_nodes = {n['code']: MetricDataResponseNode(**n) for n in i['nodes']}
                case 'sj':
                    sj_nodes = {n['code']: MetricDataResponseNode(**n) for n in i['nodes']}
                case 'reg':
                    reg_nodes = {n['code']: MetricDataResponseNode(**n) for n in i['nodes']}

        data = []
        for i in return_data['datanodes']:
            nd = i['data']
            item = MetricDataResponseData(data=nd['data'], has_data=nd['hasdata'], str_data=nd['strdata'])
            for wd in i['wds']:
                wd_code = wd['wdcode']
                wd_value = wd['valuecode']
                match wd_code:
                    case 'zb':
                        item.metric_code = wd_value
                        item.metric_node = zb_nodes[item.metric_code]
                    case 'sj':
                        item.date_code = wd_value
                        item.date_node = sj_nodes[item.date_code]
                    case 'reg':
                        item.region_code = wd_value
                        item.region_node = reg_nodes[item.region_code]
            data.append(item)
        return data

    @classmethod
    @cache
    def retrieve_metric_codes(
            cls,
            db_code: str,
            metric_code: str = 'zb',
            language: str | None = '') -> list[MetricCodeResponseData]:
        """
        Downloads metric codes from CN-Stats via the getTree API
        :param db_code: Downloads the codes under the specified db code.
        :param metric_code: Downloads the metric and its descendants. If it's None means downloads all the codes
        :param language: download data with specific language. None means Chinese, and 'english' is another option.
        :return: Returns the metric codes returned by the API
        """

        if metric_code is None:
            metric_code = 'zb'
        lst = []
        parents = []
        response = cls._adv_api(db_code=db_code, wd='zb', tree_id=metric_code, language=language)
        for n in response:
            lst.append(MetricCodeResponseData(
                code=n['id'],
                db_code=db_code,
                name=n['name'],
                parent=n['pid'],
                exp=n['exp'],
                memo=n['memo'] if 'memo' in n else None,
                unit=n['unit'] if 'unit' in n else None,
                is_parent=n['isParent']
            ))
            # if n['isParent']:  # don't know why the value is not correct,
            # and avoid to spend too much time on non parent codes, check the code length,
            # if it's larger than 9, considering it as a non parent code.
            if len(n['id']) <= 9:
                parents.append(n["id"])

        cls.logger.info(f'Got {len(lst)} codes, and {len(parents)} are parent codes.')

        for item in parents:
            cls.logger.info(f'Sleep {cls._sleep_seconds} seconds then try to get descendants of {item}.')
            time.sleep(cls._sleep_seconds)  # sleep two seconds to reduce the access frequency to the API
            lst.extend(cls.retrieve_metric_codes(db_code=db_code, metric_code=item, language=language))

        return lst

    @classmethod
    @cache
    def retrieve_region_codes(
            cls,
            db_code: str,
            region_code: str = 'reg',
            language: str | None = '') -> list[RegionCodeResponseData]:
        """
        Downloads region codes from CN-Stats via the getTree API
        :param db_code: Downloads the codes under the specified db code.
        :param region_code: Downloads the region and its descendants. If it's None means downloads all the codes
        :param language: download data with specific language. None means Chinese, and 'english' is another option.
        :return: Returns the region codes returned by the API
        """
        if region_code is None:
            region_code = 'reg'

        response = cls._adv_api(db_code=db_code, wd='reg', tree_id=region_code, language=language)

        cls.logger.info(f'Got {len(response)} codes.')

        lst = []
        for n in response:
            item = RegionCodeResponseData(
                code=n['id'],
                db_code=db_code,
                name=n['name'],
                parent=n['pid'],
                exp=n['exp'],
                is_parent=not n['isParent'],  # don't know why but looks like the value returned by the API is reversed.
                children=None
            )
            lst.append(item)
            if item.is_parent:
                cls.logger.info(f'Sleep {cls._sleep_seconds} second then try to get descendants of {item.code}.')
                time.sleep(cls._sleep_seconds)  # sleep a while to reduce the access frequency to the API
                children = cls.retrieve_region_codes(db_code=db_code, region_code=item.code, language=language)
                lst.extend(children)
                if len(children) > 0:
                    item.children = [i.code for i in children]

        return lst


    @classmethod
    def test(cls) -> list[Any]:

        return cls.retrieve_metric_data('reg', 'sj', 'fsyd', 'A01010101', ['2022'], language='english')
        # url = f'https://data.stats.gov.cn/easyquery.htm'
        # obj = {
        #     'm': 'QueryData',
        #     'dbcode': 'fsyd',
        #     'rowcode': 'reg',
        #     'colcode': 'sj',
        #     'wds': json.dumps([{"wdcode":"zb","valuecode":"A01010101"}]),
        #     'dfwds': json.dumps([{"wdcode":"sj","valuecode":"202212"}]),
        #     'k1': cls._random_timestamp()
        # }
        # urllib3.disable_warnings()
        # logging.info(f'Starts to invoke easyquery api, with parameters {str(obj)}.')
        # r = requests.get(url, params=obj, headers=cls._header, verify=False)
        # return r.json()