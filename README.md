# HIVE HILO Sequence
The project supports an uno incrementing (incrementing by 1) sequence in hive utilizing hilo allocation alogorithm.

-----------------
High Level Design
-----------------

- A stateful UDF is created which maintains a HI/LO counters to increment the sequences.
- The HI value is maintained as distribute atomic long in zookeeper.
- The HI value is incremented & fetched for every n LO (default 200) iterations.
- The UDF supports a single String argument which is the sequence name used to maintain zNodes in zookeeper.
	- The sequence name can't start with /
	- The sequence name can be like a nested structure like "[directoryName]/[subdirectoies]/sequenceName"
    - No need to create any znode manually prior to this, UDF will do it itself
- Sequence function can only be used in select queries, which can be nested as sub-select inside Insert queries.
    - for example: you can insert into final table from staging table and can use UDF there.
- You can create multiple sequences by changing the sequence name argument while invoking the select. 
- You can configure low values and seed values per sequence basis.
- 	- You can configure these permanently in UDF properties file.
- 	- OR you can pass these as arguments while invoking function.
- Seed value will only be used if sequence is being used for first time i.e. no zookeeper node exists for sequence path.
- If seed value is not provided the sequence will start from 0 if fresh or on the basis of last HI value if already used.
- If LO value is not provided the sequence will use default value of 200.

-----
Usage
-----
*FunctionName(<String> sequenceName, <int> lowvalue[optional], <long> seedvalue[optional])*

- checkout the repository
- change the properties like zookeeper address, zookeeper root, seed value, low values in the src/main/resources/UDFproperties.properties
- make the package
- add the jar (without dependencies) to hive
- create a temporary/permanent function using the class 'com.bigdata.hive.udf.HLSequenceGenerator'
- If your select query involves join set auto conversion to map side joins to false. 
	- set hive.auto.convert.join=false;
- If you get error like: [java.lang.ClassCastException: org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray cannot be cast to [Ljava.lang.Object;]
	- set hive.mapjoin.optimized.hashtable=false; 
- If sequence outputs same value for each row, this could be a caching issue bug:
    - set hive.cache.expr.evaluation = false
        - If above doesn't solves the problem, concatenate sequence output with an empty zero length string. Hive will auto cast back the result to number.
            - select concat(seq_func("sequence_name"),'') , other part of query .......  
- use the function in your select queries
    - Ex usage: select seq("modelIds", 300, 327L) from models;
 
----------
Trade Offs
----------

- The UDF increments the HI values for every 200 iterations by default. This is to ensure that interactions with zookeeper are not done very frequently.
- However this also means that worst case 199 low values per mapper JVM can be wasted if not used.
- You can change these figures to achieve a tradeoff between performance and sequence values wastage.
