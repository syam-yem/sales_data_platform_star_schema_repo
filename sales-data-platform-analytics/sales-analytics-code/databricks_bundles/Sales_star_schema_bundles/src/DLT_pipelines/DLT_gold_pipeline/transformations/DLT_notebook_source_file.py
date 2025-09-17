import dlt
from pyspark.sql.functions import *


My_rules = {
  "rule1":"product_id is not null",
  "rule2":"product_name is not null"
}


@dlt.table()
@dlt.expect_all_or_drop(My_rules)
def DimProducts_stage():
    df = spark.readStream.table("databricksete.silver.products_silver")
    return df 
    


@dlt.view
def DimProducts_view():
  df = spark.readStream.table("live.DimProducts_stage")
  return df 


dlt.create_streaming_table("Dimproducts")


dlt.apply_changes(
    target = "Dimproducts",
    source = "Live.DimProducts_view",
    keys = ["product_id"],
    sequence_by ="product_id",
    stored_as_scd_type =2
)




