# CQL
continuous query based on Apache Calcite

Test cases in test/java/TestCQL.java and test/java/TestLinearRoad

The overview of the process:

1. define schema information in resources/sales.json
2. read source data from tableFactory (StreamTableFactory and UserFactory)
3. each operator calls run method consume tuple from the source and output tuple in sink
4. the sink of a child operator is the source of it's parent operator
5. then child operator calls runParent to run parent operator

Currently the whole process runs in a single thread.
