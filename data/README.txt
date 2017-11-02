1. cqlsh < create.cql
2. update load.cql to point to the provided CSV; example

copy source_data(dt,lat,lon,base) from '/tmp/csv/uber.csv';

3. cqlsh < load.cql
