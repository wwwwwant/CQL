# CQL
continuous query based on Apache Calcite

Test cases in test/java/testCQL.java

The overview of the process:

1. define schema information in resources/sales.json
2. read source data from tableFactory (StreamTableFactory and UserFactory)
3. each operator calls run method consume tuple from the source and output tuple in sink
4. each sink of a child operator is the source of the parent operator
5. then it calls runParent to run parent operator

Current the whole process runs in a single thread.
