# HIVE HILO Sequence
The project supports an uno incrementing (incrementing by 1) sequence in hive utilizing hilo allocation alogorithm.

-----------------
High Level Design
-----------------

- A stateful UDF is created which maintains a HI/LO counters to increment the sequences.
- The HI value is maintained as distribute atomic long in zookeeper.
- The HI value is incremented & fetched for every n LO (default 200) iterations.
- The UDF supports a single String argument which is the sequence name used to maintain zNodes in zookeeper.
    - No need to create any znode manually prior to this, UDF will do it itself
- Sequence function can only be used in select queries, which can be nested as sub-select inside Insert queries.
    - for example: you can insert into final table from staging table and can use UDF there.
- You can create multiple sequences by changing the sequence name argument while invoking the select. 
- You can configure low values and seed values per sequence basis.
- Seed value will only be used if sequence is being used for first time i.e. no zookeeper node exists for /sequenceName.
- If seed value is not provided the sequence will start from 0 if fresh or on the basis of last HI value if already used.
- If LO value is not provided the sequence will use default value of 200.

-----
Usage
-----

- checkout the repository
- change the properties like zookeeper address, seed value, low values in the src/main/resources/UDFproperties.properties
- make the package
- add the jar (without dependencies) to hive
- create a temporary/permanent function using the class 'com.bigdata.hive.udf.HLSequenceGenerator_v01' 
- If the hive execution engine is 'mr' set below otherwise ignore this step.
    - set hive.plan.serialization.format=javaXML;
- use the function in your select queries
    - Ex usage: select seq("modelIds") from models;
 
----------
Trade Offs
----------

- The UDF increments the HI values for every 200 iterations by default. This is to ensure that interactions with zookeeper are not done very frequently.
- However this also means that worst case 199 low values per mapper JVM can be wasted if not used.
- You can change these figures to achieve a tradeoff between performance and sequence values wastage.
