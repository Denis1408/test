SELECT
    s.name AS SchemaName,
    t.name AS TableName,
    c.name AS ColumnName,
    ty.name AS DataType,
    c.max_length
FROM sys.columns c
JOIN sys.tables t
    ON c.object_id = t.object_id
JOIN sys.schemas s
    ON t.schema_id = s.schema_id
JOIN sys.types ty
    ON c.user_type_id = ty.user_type_id
WHERE ty.name = 'varchar'
ORDER BY c.max_length DESC;
