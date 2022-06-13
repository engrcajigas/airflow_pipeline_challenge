# Using Apache Airflow

## Pre-requisites:<br/>
- Apache Airflow ([See Installation Guide](https://github.com/apache/airflow#installing-from-pypi))
- Python3 
- Other pre-requisite
- SQLite (Note: Not recommended to use in production)
- 


For this project, I worked with the following datasets:

- Meta Data: `http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/meta_Amazon_Instant_Video.json.gz` <br/>
- Review Data: `http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Amazon_Instant_Video_5.json.gz`<br/>

## Schema (Data Model)
### Fact Table 
<b>`product_reviews`</b> - records in logs associated with product reviews <br/>
```
  asin, reviewerID, unixReviewTime, reviewText, summary, overall
``` 
### Dimension Tables <br/> 
<b>`product`</b> - product ids in the dataset <br/>
```
  asin(PK), price, categories, overall_ratings
```

<b>`users`</b> - users that give reviews to the products <br/>
```
  reviewerId(PK), reviewerName
```

<b>`time`</b> - timestamps of records of product reviews <br/>
```
  unixReviewTime, asin, reviewerID, year, month, day
``` 

The project consists of the following components:

  1. A <b>DAG template</b> that has all the imports and task dependencies
  2. A <b>helper class</b> for SQL transformations
  3. The <b>operators folder</b> with the operator templates


## Graph view of the tasks dependencies

![alt text](/images/airflow_pipeline.PNG "Airflow pipeline diagram")

