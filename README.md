![](https://disk.yandex.ru/i/njMwZoCT4EdwnA)


# Привет, меня зовут Анастасия!

The purpose of the project:
1. Create a data showcase for analysts that will contain aggregated metrics on product cards, popular categories, and others.
2. Create a table view with a list of unreliable sellers.
3. Create a table view with a report on brands

Requirements for the Airflow DAG:

1.The DAG has no schedule (schedule_interval=None)
2. Each Spark task is executed in its own task (separate submit)
3. All tasks are performed in parallel
4. Tasks are named identically to reports.

Processing data for the LineItems report.
It is necessary to enrich the source data with additional parameters and aggregates based on indicators.

Project outcome: 
- Implemented automated data processing processes in Apache Airflow using SQLExecuteQueryOperator to perform queries to the Greenplum database.
- Created external tables and views, including "seller_items", "unreliable_sellers_view" and "item_brands_view", which improved the analysis of product and seller data.
- Optimized the process of obtaining analytical information, which contributed to more effective reporting and data-based decision-making.
