from data.db_code import DbCode
from data import MetricCode


def main():
    metric_code_1 = MetricCode('A01', DbCode.MACRO_ECONOMIC_MONTHLY, 'name 1', None)
    metric_code_2 = MetricCode('A01', DbCode.MACRO_ECONOMIC_MONTHLY, 'name 2', None)
    print(metric_code_1 == metric_code_2)
    print(metric_code_1.name == metric_code_2.name)
    #
    # MetricCode.add_or_update([MetricCode('TEST01', DbCode.MACRO_ECONOMIC_ANNUAL, 'TEST Code', None)])
    # print(MetricCode.get('TEST01', DbCode.MACRO_ECONOMIC_ANNUAL))
    #
    # MetricData.add_or_update([MetricData(MetricCode('TEST01', DbCode.MACRO_ECONOMIC_ANNUAL), 2024, 100.2)])
    # print(len(MetricData.list(DbCode.MACRO_ECONOMIC_ANNUAL, 'TEST01')))
    #
    # MetricData.delete([MetricData(MetricCode('TEST01', DbCode.MACRO_ECONOMIC_ANNUAL), 2024, 100.2)])
    # print(len(MetricData.list(DbCode.MACRO_ECONOMIC_ANNUAL, 'TEST01')))
    #
    # MetricCode.delete([MetricCode('TEST01', DbCode.MACRO_ECONOMIC_ANNUAL, None, None)])
    # print(len(MetricCode.list()))
    #
    # MetricCode.import_metrics(DbCode.HMT_MONTHLY)
    # MetricData.import_metric_data(
    #     MetricCode(metric_code='A010301', db_code=DbCode.MACRO_ECONOMIC_ANNUAL),
    #     datetime(year=2010, month=1, day=1),
    #     datetime(year=2024, month=6, day=1))

    pass


if __name__ == '__main__':
    main()
