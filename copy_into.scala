val setup_sql = s"""
CREATE DATABASE SCOPED CREDENTIAL StorageCredential
WITH IDENTITY = 'Managed Identity';
"""

val source_sql = s"""
CREATE EXTERNAL DATA SOURCE MyStorage
WITH (
    TYPE = BLOB_STORAGE,
    LOCATION = 'https://${storage_account}.blob.core.windows.net/${container}',
    CREDENTIAL = StorageCredential
);
"""



val copy_sql = s"""
BULK INSERT tmp_table
FROM 'CSC_UCSC_STG/output_parquet_${file_name}'
WITH (
    DATA_SOURCE = 'MyStorage',
    FORMAT = 'CSV',
    FIELDTERMINATOR = ';',
    FIELDQUOTE = '"',
    FIRSTROW = 2,
    ROWTERMINATOR = '0x0a',
    CODEPAGE = '65001'
);
"""



val cleanup_sql = s"""
IF EXISTS (
    SELECT 1
    FROM sys.external_data_sources
    WHERE name = 'MyStorage'
)
    DROP EXTERNAL DATA SOURCE MyStorage;

IF EXISTS (
    SELECT 1
    FROM sys.database_scoped_credentials
    WHERE name = 'StorageCredential'
)
    DROP DATABASE SCOPED CREDENTIAL StorageCredential;
"""
