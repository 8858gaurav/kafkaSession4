# In databricks with Pyspark, .toTable() is a databricks-specifc api used to write a df (batch or streaming)
# directly to a managed or external table - most commonly delta tables

# .toTable method is only available for spark streaming df, not for batch df

# by default, it'll write the files as a table in delta format.


data = [(1, "A"), (2, "B")]
df = spark.createDataFrame(data, ['id', 'value'])
df.write.saveAsTable('misgaurav_catalog.misgaurav_schema.sample_table')

%sql
describe detail misgaurav_catalog.misgaurav_schema.sample_table;

-- format	delta
-- id	0c300920-e08f-4199-a620-b1c007ba70da
-- name	misgaurav_catalog.misgaurav_schema.sample_table
-- description	null
-- location	abfss://unity-catalog-storage@dbstoragenjc7y4v5pg7ps.dfs.core.windows.net/7405608181978035/misgaurav_databricks_ws_7405608181978035/__unitystorage/catalogs/1f55e84b-9a80-413d-bf93-d08958b65524/tables/4c38206a-d960-4dee-8cd3-c0a8bf73340f
-- createdAt	2025-12-31T11:23:25.306Z
-- lastModified	2025-12-31T11:23:28.000Z
-- partitionColumns	[]
-- clusteringColumns	[]
-- numFiles	1
-- sizeInBytes	819
-- properties	{"delta.enableDeletionVectors":"true"}
-- minReaderVersion	3
-- minWriterVersion	7
-- tableFeatures	["appendOnly","deletionVectors","invariants"]
-- statistics	{"numRowsDeletedByDeletionVectors":0,"numDeletionVectors":0}
-- clusterByAuto	FALSE

%sql
select * from misgaurav_catalog.misgaurav_schema.sample_table;

-- id	value
-- 1	  A
-- 2	  B


# Now go here: misgaurav_catalog.misgaurav_schema.sample_table, you'll find the details about the table

# Owner: Gaurav Mishra
# Type: Managed
# Data source :Delta
# Last updated: 58 seconds ago
# Size: 819B, 1 file

# on the same page, detail sections: 
# Created at: Dec 31, 2025, 04:53 PM
# Created by: gauravmishra7080@gmail.com
# Delta runtime properties kv pairs: (empty)
# Storage location: abfss://unity-catalog-storage@dbstoragenjc7y4v5pg7ps.dfs.core.windows.net/7405608181978035/misgaurav_databricks_ws_7405608181978035/__unitystorage/catalogs/1f55e84b-9a80-413d-bf93-d08958b65524/tables/4c38206a-d960-4dee-8cd3-c0a8bf73340f
# Table ID: 4c38206a-d960-4dee-8cd3-c0a8bf73340f
# Type: MANAGED

data = [(3, "A"), (4, "B")]
df = spark.createDataFrame(data, ['id', 'value'])
df.write.mode('append').saveAsTable('misgaurav_catalog.misgaurav_schema.sample_table')

%sql
describe detail misgaurav_catalog.misgaurav_schema.sample_table;

-- format	delta
-- id	0c300920-e08f-4199-a620-b1c007ba70da
-- name	misgaurav_catalog.misgaurav_schema.sample_table
-- description	The table contains a collection of sample data with unique identifiers and associated values. It can be used for testing purposes, data validation, or as a reference for developing and debugging applications. The structure allows for easy retrieval and manipulation of the data.
-- location	abfss://unity-catalog-storage@dbstoragenjc7y4v5pg7ps.dfs.core.windows.net/7405608181978035/misgaurav_databricks_ws_7405608181978035/__unitystorage/catalogs/1f55e84b-9a80-413d-bf93-d08958b65524/tables/4c38206a-d960-4dee-8cd3-c0a8bf73340f
-- createdAt	2025-12-31T11:23:25.306Z
-- lastModified	2025-12-31T11:42:18.000Z
-- partitionColumns	[]
-- clusteringColumns	[]
-- numFiles	2
-- sizeInBytes	1638
-- properties	{"delta.enableDeletionVectors":"true"}
-- minReaderVersion	3
-- minWriterVersion	7
-- tableFeatures	["appendOnly","deletionVectors","invariants"]
-- statistics	{"numRowsDeletedByDeletionVectors":0,"numDeletionVectors":0}
-- clusterByAuto	FALSE


# Now go here: misgaurav_catalog.misgaurav_schema.sample_table, you'll find the details about the table

# Owner: Gaurav Mishra
# Type: Managed
# Data source :Delta
# Last updated: 50 seconds ago
# Size: 819B, 2 file

# on the same page, detail sections: 
# Created at: Dec 31, 2025, 04:53 PM
# Created by: gauravmishra7080@gmail.com
# Delta runtime properties kv pairs: (empty)
# Storage location: abfss://unity-catalog-storage@dbstoragenjc7y4v5pg7ps.dfs.core.windows.net/7405608181978035/misgaurav_databricks_ws_7405608181978035/__unitystorage/catalogs/1f55e84b-9a80-413d-bf93-d08958b65524/tables/4c38206a-d960-4dee-8cd3-c0a8bf73340f
# Table ID: 4c38206a-d960-4dee-8cd3-c0a8bf73340f
# Type: MANAGED
