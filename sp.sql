CREATE OR ALTER PROCEDURE dbo.RunCopyDataByYear
(
    @DatabaseName SYSNAME,
    @StartYear    INT,
    @EndYear      INT
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @CurrentYear      INT,
        @ProcedureName    SYSNAME,
        @FullProcedureName NVARCHAR(776),
        @Sql              NVARCHAR(MAX),
        @ProcedureExists  BIT,
        @ErrorMessage     NVARCHAR(2048);

    /* Validate the database name */
    IF NULLIF(LTRIM(RTRIM(@DatabaseName)), N'') IS NULL
    BEGIN
        THROW 50001, 'Database name cannot be empty.', 1;
    END;

    /* Remove leading and trailing spaces */
    SET @DatabaseName = LTRIM(RTRIM(@DatabaseName));

    /* Validate the year range */
    IF @StartYear IS NULL OR @EndYear IS NULL
    BEGIN
        THROW 50002, 'Start year and end year must be specified.', 1;
    END;

    IF @StartYear > @EndYear
    BEGIN
        THROW 50003, 'Start year cannot be greater than end year.', 1;
    END;

    /* Validate that the database exists */
    IF DB_ID(@DatabaseName) IS NULL
    BEGIN
        SET @ErrorMessage =
            N'Database does not exist: ' + QUOTENAME(@DatabaseName) + N'.';

        THROW 50004, @ErrorMessage, 1;
    END;

    /* Validate that the database is online */
    IF DATABASEPROPERTYEX(@DatabaseName, 'Status') <> 'ONLINE'
    BEGIN
        SET @ErrorMessage =
            N'Database is not online: ' + QUOTENAME(@DatabaseName) + N'.';

        THROW 50005, @ErrorMessage, 1;
    END;

    /*
        Build the procedure name from the database name.

        Examples:
        WA -> WA_CopyData
        WI -> WI_CopyData
        VA -> VA_CopyData
    */
    SET @ProcedureName = @DatabaseName + N'_CopyData';

    SET @FullProcedureName =
        QUOTENAME(@DatabaseName)
        + N'.'
        + QUOTENAME(N'dbo')
        + N'.'
        + QUOTENAME(@ProcedureName);

    /* Check whether the target procedure exists */
    SET @ProcedureExists = 0;

    SET @Sql = N'
        USE ' + QUOTENAME(@DatabaseName) + N';

        IF OBJECT_ID(
            N''dbo.' + REPLACE(@ProcedureName, N'''', N'''''') + N''',
            N''P''
        ) IS NOT NULL
        BEGIN
            SET @Exists = 1;
        END;
    ';

    EXEC sys.sp_executesql
        @Sql,
        N'@Exists BIT OUTPUT',
        @Exists = @ProcedureExists OUTPUT;

    IF @ProcedureExists = 0
    BEGIN
        SET @ErrorMessage =
            N'Target procedure does not exist: '
            + @FullProcedureName
            + N'.';

        THROW 50006, @ErrorMessage, 1;
    END;

    /* Start processing from the first requested year */
    SET @CurrentYear = @StartYear;

    WHILE @CurrentYear <= @EndYear
    BEGIN
        BEGIN TRY
            RAISERROR(
                'Starting procedure %s for year %d.',
                10,
                1,
                @FullProcedureName,
                @CurrentYear
            ) WITH NOWAIT;

            /*
                Execute the target procedure.

                Expected target procedure signature:
                CREATE PROCEDURE dbo.WA_CopyData
                    @Year INT
            */
            SET @Sql = N'
                EXEC ' + @FullProcedureName + N'
                    @Year = @ExecutionYear;
            ';

            EXEC sys.sp_executesql
                @Sql,
                N'@ExecutionYear INT',
                @ExecutionYear = @CurrentYear;

            RAISERROR(
                'Procedure completed successfully for year %d.',
                10,
                1,
                @CurrentYear
            ) WITH NOWAIT;
        END TRY
        BEGIN CATCH
            /* Stop processing immediately and return detailed error information */
            SET @ErrorMessage =
                N'Execution stopped. Database: '
                + QUOTENAME(@DatabaseName)
                + N'; Procedure: '
                + @FullProcedureName
                + N'; Year: '
                + CONVERT(NVARCHAR(10), @CurrentYear)
                + N'; Error number: '
                + CONVERT(NVARCHAR(10), ERROR_NUMBER())
                + N'; Error message: '
                + ERROR_MESSAGE();

            THROW 50010, @ErrorMessage, 1;
        END CATCH;

        SET @CurrentYear += 1;
    END;

    RAISERROR(
        'All years from %d through %d were processed successfully.',
        10,
        1,
        @StartYear,
        @EndYear
    ) WITH NOWAIT;
END;
GO
