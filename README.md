# China Statistics Data Downloader

## How to Run the Downloader

### Download MetricCode

The downloader process supports two ways to retrieve the meetric code.

1. Download all the metric code of each Category.

   ```bash
   cn-stats-data download metric
   ```

2. Download the descendants of a particular metric code.

   ```bash
   cn-stats-data download metric -category=hgnd -metric=A02080H
   ```

Actually the first way can be thought as a special case of the second way, which the metric code is None. In another word, the `-metric` parameter in the second sample also can be omitted. That means to download all the metric codes under the specificed category.

Because of the access rate limitation, downloading the metric codes is a time consuming work. It needs dozens of hours to get all the metric codes. Considering the network issue or the service not available issue could cause the process failed. Avoid re-downloading all the data from scratch, and when the program is restarted, the program can continue processing from where it failed.

Here are some key points to implement the function:

* The download function invokes `ChinaStatsDataApis.fetch_metrics` function (set parameter recursive_fetch to false) to get the children codes.
  Here is the sample how to get the children codes.

  ```python
  apis = ChinaStatsDataApis()
  parent = Metric.of(db_code=Category.MACRO_MONTHLY.db_code, code="")
  children = apis.fetch_metrics(Category.MACRO_MONTHLY, parent=parent, recursive_fetch=False)
  ```

* The download function uses `MetricCodeDao.list` function to get the metric codes from database. So it can use them to compare with the codes from the website to know which codes are changed, and which should be deleted. Uses `MetricCodeDao.add_or_update` function to save the codes to database, and uses `MetricCodeDao.delete` to delete the codes which are not existing any more.
* When comparing the data, uses the `MetricCode.__eq__` function to check if the data is changed.
* `MetricCodeDao.list` can be invoked per each category, and put it into a map, the code of the metric is the key. Then when need to compare the data returned by the `ChinaStatsDataApis.fetch_metrics` function, can use the code of the parent code to lookup the data from the database, and get the children from its `children` property.
* After saved the changes, go through each child, uses it as the parent to run the logic above.
