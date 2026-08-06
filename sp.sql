CREATE OR ALTER PROCEDURE dbo.RunCopyDataByYear
(
    @DatabaseName SYSNAME,
    @StartYear    INT,
    @EndYear      INT,
    @Execute      BIT
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @CurrentYear       INT,
        @ArchiveDatabase   SYSNAME,
        @ProcedureName     SYSNAME,
        @FullProcedureName NVARCHAR(776),
        @Sql               NVARCHAR(MAX),
        @ProcedureExists   BIT,
        @ErrorMessage      NVARCHAR(2048);

    /* Validate the database name */
    IF NULLIF(LTRIM(RTRIM(@DatabaseName)), N'') IS NULL
    BEGIN
        THROW 50001, 'Database name cannot be empty.', 1;
    END;

    SET @DatabaseName = LTRIM(RTRIM(@DatabaseName));

    /* Validate the year parameters */
    IF @StartYear IS NULL OR @EndYear IS NULL
    BEGIN
        THROW 50002, 'Start year and end year must be specified.', 1;
    END;

    IF @StartYear > @EndYear
    BEGIN
        THROW 50003, 'Start year cannot be greater than end year.', 1;
    END;

    /* Validate the Execute parameter */
    IF @Execute IS NULL
    BEGIN
        THROW 50004, 'Execute parameter must be specified.', 1;
    END;

    /*
        Build the archive database and procedure names.

        Examples:
        WA -> WA_ARCHIVE.dbo.WA_CopyData
        WI -> WI_ARCHIVE.dbo.WI_CopyData
        VA -> VA_ARCHIVE.dbo.VA_CopyData
    */
    SET @ArchiveDatabase = @DatabaseName + N'_ARCHIVE';
    SET @ProcedureName = @DatabaseName + N'_CopyData';

    SET @FullProcedureName =
        QUOTENAME(@ArchiveDatabase)
        + N'.'
        + QUOTENAME(N'dbo')
        + N'.'
        + QUOTENAME(@ProcedureName);

    /* Validate that the archive database exists */
    IF DB_ID(@ArchiveDatabase) IS NULL
    BEGIN
        SET @ErrorMessage =
            N'Archive database does not exist: '
            + QUOTENAME(@ArchiveDatabase)
            + N'.';

        THROW 50005, @ErrorMessage, 1;
    END;

    /* Validate that the archive database is online */
    IF DATABASEPROPERTYEX(@ArchiveDatabase, 'Status') <> 'ONLINE'
    BEGIN
        SET @ErrorMessage =
            N'Archive database is not online: '
            + QUOTENAME(@ArchiveDatabase)
            + N'.';

        THROW 50006, @ErrorMessage, 1;
    END;

    /* Check whether the target procedure exists */
    SET @ProcedureExists = 0;

    SET @Sql = N'
        SELECT @Exists =
            CASE
                WHEN EXISTS
                (
                    SELECT 1
                    FROM ' + QUOTENAME(@ArchiveDatabase) + N'.sys.procedures AS p
                    INNER JOIN ' + QUOTENAME(@ArchiveDatabase) + N'.sys.schemas AS s
                        ON s.schema_id = p.schema_id
                    WHERE s.name = N''dbo''
                      AND p.name = @TargetProcedureName
                )
                THEN 1
                ELSE 0
            END;
    ';

    EXEC sys.sp_executesql
        @Sql,
        N'@TargetProcedureName SYSNAME, @Exists BIT OUTPUT',
        @TargetProcedureName = @ProcedureName,
        @Exists = @ProcedureExists OUTPUT;

    IF @ProcedureExists = 0
    BEGIN
        SET @ErrorMessage =
            N'Target procedure does not exist: '
            + @FullProcedureName
            + N'.';

        THROW 50007, @ErrorMessage, 1;
    END;

    /* Start processing the requested year range */
    SET @CurrentYear = @StartYear;

    WHILE @CurrentYear <= @EndYear
    BEGIN
        BEGIN TRY
            RAISERROR(
                'Starting %s. Year: %d. Execute: %d.',
                10,
                1,
                @FullProcedureName,
                @CurrentYear,
                @Execute
            ) WITH NOWAIT;

            /*
                Execute the target procedure.

                Expected target procedure signature:

                CREATE PROCEDURE dbo.WA_CopyData
                    @Year    INT,
                    @Execute BIT
            */
            SET @Sql = N'
                EXEC ' + @FullProcedureName + N'
                    @Year = @ExecutionYear,
                    @Execute = @ExecutionMode;
            ';

            EXEC sys.sp_executesql
                @Sql,
                N'@ExecutionYear INT, @ExecutionMode BIT',
                @ExecutionYear = @CurrentYear,
                @ExecutionMode = @Execute;

            RAISERROR(
                'Year %d completed successfully.',
                10,
                1,
                @CurrentYear
            ) WITH NOWAIT;
        END TRY
        BEGIN CATCH
            /* Stop immediately when an error occurs */
            SET @ErrorMessage =
                N'Execution stopped. Database: '
                + QUOTENAME(@ArchiveDatabase)
                + N'; Procedure: '
                + @FullProcedureName
                + N'; Year: '
                + CONVERT(NVARCHAR(10), @CurrentYear)
                + N'; Execute: '
                + CONVERT(NVARCHAR(1), @Execute)
                + N'; Original error number: '
                + CONVERT(NVARCHAR(10), ERROR_NUMBER())
                + N'; Original error message: '
                + ERROR_MESSAGE();

            THROW 50010, @ErrorMessage, 1;
        END CATCH;

        SET @CurrentYear += 1;
    END;

    RAISERROR(
        'All years from %d through %d were processed successfully. Execute: %d.',
        10,
        1,
        @StartYear,
        @EndYear,
        @Execute
    ) WITH NOWAIT;
END;
GO
