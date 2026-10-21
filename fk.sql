
USE [ucsc_state];

SET NOCOUNT ON;

DECLARE @SchemaName SYSNAME = 'ucsc_state';

DROP TABLE IF EXISTS #Tables;
DROP TABLE IF EXISTS #Dependencies;
DROP TABLE IF EXISTS #LoadOrder;

-- 1. Get all tables in the schema
SELECT
    t.object_id AS TableID,
    s.name AS SchemaName,
    t.name AS TableName
INTO #Tables
FROM sys.tables t
JOIN sys.schemas s
    ON t.schema_id = s.schema_id
WHERE s.name = @SchemaName;

-- 2. Find FK relationships
-- Child tables must be loaded after parent tables
SELECT DISTINCT
    fk.parent_object_id AS ChildID,
    fk.referenced_object_id AS ParentID
INTO #Dependencies
FROM sys.foreign_keys fk
JOIN #Tables c
    ON fk.parent_object_id = c.TableID
JOIN #Tables p
    ON fk.referenced_object_id = p.TableID
WHERE fk.parent_object_id <> fk.referenced_object_id;

-- 3. Create loading order table
CREATE TABLE #LoadOrder
(
    TableID INT PRIMARY KEY,
    LoadLevel INT
);

DECLARE @Level INT = 1;
DECLARE @Rows INT = 1;

-- 4. Calculate dependency levels
WHILE @Rows > 0
BEGIN
    INSERT INTO #LoadOrder (TableID, LoadLevel)
    SELECT
        t.TableID,
        @Level
    FROM #Tables t
    WHERE NOT EXISTS (
        SELECT 1
        FROM #LoadOrder l
        WHERE l.TableID = t.TableID
    )
    AND NOT EXISTS (
        SELECT 1
        FROM #Dependencies d
        WHERE d.ChildID = t.TableID
          AND NOT EXISTS (
              SELECT 1
              FROM #LoadOrder l
              WHERE l.TableID = d.ParentID
          )
    );

    SET @Rows = @@ROWCOUNT;
    SET @Level = @Level + 1;
END;

-- 5. Return final loading order
SELECT
    ROW_NUMBER() OVER (
        ORDER BY
            CASE WHEN l.LoadLevel IS NULL THEN 1 ELSE 0 END,
            l.LoadLevel,
            t.TableName
    ) AS LoadOrder,
    l.LoadLevel,
    t.SchemaName,
    t.TableName,
    QUOTENAME(t.SchemaName) + '.' +
        QUOTENAME(t.TableName) AS FullTableName,
    CASE
        WHEN l.TableID IS NULL
            THEN 'CIRCULAR DEPENDENCY'
        ELSE 'READY'
    END AS Status
FROM #Tables t
LEFT JOIN #LoadOrder l
    ON t.TableID = l.TableID
ORDER BY
    CASE WHEN l.LoadLevel IS NULL THEN 1 ELSE 0 END,
    l.LoadLevel,
    t.TableName;
