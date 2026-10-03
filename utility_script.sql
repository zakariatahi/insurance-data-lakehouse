/**
 * SQL Server Utility Objects Script for Databricks Lakeflow Connect
 * Version 1.7
 *
 * This script creates versioned utility stored procedures that can be used
 * to automatically remediate common SQL Server setup issues for ingestion.
 *
 * FEATURES:
 * - Automatic table discovery with @Tables = 'ALL', 'SCHEMAS:Sales,HR', wildcards
 * - Smart CT/CDC selection based on primary key presence
 * - Table-level CT/CDC enablement and DDL support objects
 * - Multi-platform detection and optimization
 * - Idempotent
 *
 * Platform Support:
 * - On-premises SQL Server
 * - Azure SQL Database
 * - Azure SQL Managed Instance
 * - Amazon RDS for SQL Server
 *
 * CHANGE HISTORY:
 * Version 1.7
 *   - Added support for detecting primary key and unique key constraint changes (ADD/DROP)
 *   - Added @AllowDisablePreExistingCaptureInstances parameter to lakeflowSetupChangeDataCapture to allow Lakeflow to take ownership of pre-existing CDC capture instances
 *   - Added pendingAddColumn flag to queue consecutive ADD COLUMNs when both CI slots are occupied, avoiding a full refresh
 *   - Upgrades now carry the DDL audit and capture instance tracking tables forward with sp_rename instead of dropping and recreating them, preserving their recorded state and avoiding a full refresh
 *   - Wrapped the DDL trigger and tracking-table cutover in a transaction (SET XACT_ABORT ON with rollback) so a failed upgrade rolls back cleanly instead of leaving objects half-migrated
 *   - Fixed reinit loop when a pre-existing capture instance is present and schema change handling calls the refresh procedure with @reinit=1
 *   - Fixed DDL triggers to explicitly SET ANSI_PADDING/ANSI_NULLS/QUOTED_IDENTIFIER ON to prevent failures from sessions with non-default SET options
 *   - Fixed collation-conflict errors (Msg 468) on databases with a non-default server collation
 *   - Added the @CreateDdlSupportingObjects parameter to lakeflowSetupChangeTracking (default 0), making the DDL audit table and trigger opt-in
 *   - Fixed CLEANUP mode to prevent orphaned DDL triggers from causing ALTER TABLE failures after uninstall
 *   - Fixed a schema change on a table with both a pre-existing and a Lakeflow capture instance to request a full refresh instead of interrupting ingestion
 *   - Fixed silent data loss where, after adding a column to a table that also has a pre-existing (non-Lakeflow) capture instance, the new column could be ingested as NULL; a full refresh now recreates a complete Lakeflow capture instance that includes the new column
 *   - Fixed ADD COLUMN schema evolution failing (Msg 5333/102) when dynamic data masking is applied to the capture instance tracking table
 *   - Fixed a variable case mismatch in the ALTER TABLE trigger ADD COLUMN and ADD/DROP CONSTRAINT handling that caused Msg 137 (Must declare the scalar variable) on case-sensitive or binary collations such as Latin1_General_BIN
 *   - Fixed lakeflowFixPermissions to grant server-scoped permissions (SELECT on sys.change_tracking_databases, EXECUTE on sp_tables/sp_columns_100/sp_pkeys/sp_statistics_100) from the master database; previously these were attempted in the user database and always failed. Grants that cannot be made are reported as per-object warnings, as before
 *   - Fixed the utility script failing to install (Msg 102) in SSMS due to truncation of a generated stored procedure
 *
 * Version 1.5
 *   - Fixed DDL audit trigger failure on Azure SQL Database by removing three-part name (db.dbo.table) from INSERT; Azure SQL does not support cross-database references inside triggers
 *   - Fixed Azure SQL Database compatibility issue using dynamic SQL for RDS-specific operations
 *   - Fixed backward compatibility with older DDL support objects that used different capture instance naming
 *   - Fixed data loss during schema changes when a pre-existing (non-Lakeflow) capture instance is present: unread change data is now correctly merged into the new Lakeflow capture instance
 *
 * Version 1.4
 *   - Fixed issue with back-to-back schema changes causing replication loops
 *   - Fixed Amazon RDS SQL Server platform support for CDC enablement
 *   - Improved handling of capture instance management during schema evolution
 *   - Capture instances are now created with lakeflow naming (lakeflow_schema_table_1) for better identification and management
 *   - Fixed variable case mismatch in ALTER TABLE trigger
 *
 * Version 1.3
 *   - DDL support objects for inline schema evolution
 *   - Pre-existing capture instance preservation logic
 *   - Lakeflow capture instance management
 *
 * Version 1.2
 *   - Multi-platform support enhancements
 *
 * Version 1.1
 *   - Initial versioned release
 *
 */

SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;

BEGIN
    PRINT N'Starting Lakeflow Connect Utility Objects installation...';
    PRINT N'Version: 1.7';
    PRINT N'Catalog: ' + DB_NAME();
    PRINT N'Executed by: ' + SUSER_NAME();
    PRINT N'Date/Time: ' + CONVERT(VARCHAR, GETDATE(), 120);
    PRINT N'';

    -- Detect platform
    DECLARE @engineEdition INT = CAST(SERVERPROPERTY('EngineEdition') AS INT);
    DECLARE @serverName NVARCHAR(255) = @@SERVERNAME;
    DECLARE @currentPlatform NVARCHAR(50);

    IF @engineEdition = 5
        SET @currentPlatform = 'AZURE_SQL_DATABASE';
    ELSE
        IF @engineEdition = 8
            SET @currentPlatform = 'AZURE_SQL_MANAGED_INSTANCE';
        ELSE
            IF @serverName LIKE '%.rds.amazonaws.com'
                SET @currentPlatform = 'AMAZON_RDS';
            ELSE
                IF @engineEdition IN (1, 2, 3, 4)
                    SET @currentPlatform = 'ON_PREMISES';
                ELSE
                    SET @currentPlatform = 'UNKNOWN';

    PRINT N'Detected platform: ' + @currentPlatform;

    -- Validate that current user has sufficient privileges
    IF (IS_ROLEMEMBER('db_owner') = 0)
        BEGIN
            RAISERROR ('User executing this script is not a ''db_owner'' role member. To execute this script, please use a user that is a member of the db_owner role.', 16, 1);
            RETURN;
        END

    -- Cleanup existing objects
    PRINT N'Cleaning up existing utility objects (all versions)...';

    DECLARE @dropSql NVARCHAR(MAX) = '';

    -- Drop lakeflowFixPermissions procedures
    SELECT @dropSql = @dropSql + 'DROP PROCEDURE dbo.[' + name + '];' + CHAR(13)
    FROM sys.procedures
    WHERE name = 'lakeflowFixPermissions';

    IF LEN(@dropSql) > 0
        BEGIN
            EXEC sp_executesql @dropSql;
            PRINT N'Dropped existing lakeflowFixPermissions procedures';
        END

    -- Drop lakeflowSetupChangeTracking procedures
    SET @dropSql = '';
    SELECT @dropSql = @dropSql + 'DROP PROCEDURE dbo.[' + name + '];' + CHAR(13)
    FROM sys.procedures
    WHERE name = 'lakeflowSetupChangeTracking';

    IF LEN(@dropSql) > 0
        BEGIN
            EXEC sp_executesql @dropSql;
            PRINT N'Dropped existing lakeflowSetupChangeTracking procedures';
        END

    -- Drop lakeflowSetupChangeDataCapture procedures
    SET @dropSql = '';
    SELECT @dropSql = @dropSql + 'DROP PROCEDURE dbo.[' + name + '];' + CHAR(13)
    FROM sys.procedures
    WHERE name = 'lakeflowSetupChangeDataCapture';

    IF LEN(@dropSql) > 0
        BEGIN
            EXEC sp_executesql @dropSql;
            PRINT N'Dropped existing lakeflowSetupChangeDataCapture procedures';
        END

    -- Drop lakeflowDetectPlatform functions
    SET @dropSql = '';
    SELECT @dropSql = @dropSql + 'DROP FUNCTION dbo.[' + name + '];' + CHAR(13)
    FROM sys.objects
    WHERE name = 'lakeflowDetectPlatform'
      AND type = 'FN';

    IF LEN(@dropSql) > 0
        BEGIN
            EXEC sp_executesql @dropSql;
            PRINT N'Dropped existing lakeflowDetectPlatform functions';
        END

    -- Drop lakeflowVersionComponent function
    SET @dropSql = '';
    SELECT @dropSql = @dropSql + 'DROP FUNCTION dbo.[' + name + '];' + CHAR(13)
    FROM sys.objects
    WHERE name = 'lakeflowVersionComponent'
      AND type = 'FN';

    IF LEN(@dropSql) > 0
        BEGIN
            EXEC sp_executesql @dropSql;
            PRINT N'Dropped existing lakeflowVersionComponent functions';
        END

    -- Drop lakeflowUtilityVersion functions
    SET @dropSql = '';
    SELECT @dropSql = @dropSql + 'DROP FUNCTION dbo.[' + name + '];' + CHAR(13)
    FROM sys.objects
    WHERE (name LIKE 'lakeflowUtilityVersion_%_%' OR name = 'lakeflowUtilityVersion')
      AND type = 'FN';

    IF LEN(@dropSql) > 0
        BEGIN
            EXEC sp_executesql @dropSql;
            PRINT N'Dropped existing lakeflowUtilityVersion functions';
        END

    -- Objects for the current version are NOT dropped here: the setup procedures carry them forward, so
    -- the old trigger keeps capturing until then. Only legacy (replicant-prefixed) objects are dropped
    -- here; triggers first so a failed table drop leaves no orphaned trigger.
    SET @dropSql = '';
    SELECT @dropSql = @dropSql + 'DROP TRIGGER [' + name + '] ON DATABASE;' + CHAR(13)
    FROM sys.triggers
    WHERE (name LIKE 'replicantDdlAuditTrigger_%_%' OR name LIKE 'replicantAlterTableTrigger_%_%')
      AND parent_class = 0;

    IF LEN(@dropSql) > 0
        BEGIN
            EXEC sp_executesql @dropSql;
            PRINT N'Dropped legacy (replicant-prefixed) DDL support triggers';
        END

    SET @dropSql = '';
    SELECT @dropSql = @dropSql + 'DROP PROCEDURE dbo.[' + name + '];' + CHAR(13)
    FROM sys.procedures
    WHERE name LIKE 'replicantDisableOldCaptureInstance_%_%'
       OR name LIKE 'replicantMergeCaptureInstances_%_%'
       OR name LIKE 'replicantRefreshCaptureInstance_%_%';

    IF LEN(@dropSql) > 0
        BEGIN
            EXEC sp_executesql @dropSql;
            PRINT N'Dropped legacy (replicant-prefixed) CDC procedures';
        END

    SET @dropSql = '';
    SELECT @dropSql = @dropSql + 'DROP TABLE [dbo].[' + name + '];' + CHAR(13)
    FROM sys.tables
    WHERE name LIKE 'replicantDdlAudit_%_%';

    IF LEN(@dropSql) > 0
        BEGIN
            EXEC sp_executesql @dropSql;
            PRINT N'Dropped legacy (replicant-prefixed) DDL audit tables';
        END

    SET @dropSql = '';
    SELECT @dropSql = @dropSql + 'DROP TABLE [dbo].[' + name + '];' + CHAR(13)
    FROM sys.tables
    WHERE name LIKE 'replicantCaptureInstanceInfo_%_%';

    IF LEN(@dropSql) > 0
        BEGIN
            EXEC sp_executesql @dropSql;
            PRINT N'Dropped legacy (replicant-prefixed) capture instance tables';
        END

    PRINT N'Cleanup completed.';
    PRINT N'';
END

-- Create versioned functions first (dependencies)
PRINT N'Creating lakeflowDetectPlatform function...';
EXEC sp_executesql N'
CREATE FUNCTION dbo.lakeflowDetectPlatform()
RETURNS NVARCHAR(50)
AS
BEGIN
    DECLARE @engineEdition INT = CAST(SERVERPROPERTY(''EngineEdition'') AS INT);
    DECLARE @serverName NVARCHAR(255) = @@SERVERNAME;
    DECLARE @platform NVARCHAR(50);

    IF @engineEdition = 5
        SET @platform = ''AZURE_SQL_DATABASE'';
    ELSE IF @engineEdition = 8
        SET @platform = ''AZURE_SQL_MANAGED_INSTANCE'';
    ELSE IF @serverName LIKE ''%.rds.amazonaws.com''
        SET @platform = ''AMAZON_RDS'';
    ELSE IF DB_ID(''msdb'') IS NOT NULL AND OBJECT_ID(''msdb.dbo.rds_cdc_enable_db'', ''P'') IS NOT NULL
        SET @platform = ''AMAZON_RDS'';
    ELSE IF @engineEdition IN (1, 2, 3, 4)
        SET @platform = ''ON_PREMISES'';
    ELSE
        SET @platform = ''UNKNOWN'';

    RETURN @platform;
END';
PRINT N'Created lakeflowDetectPlatform function';

PRINT N'Creating lakeflowUtilityVersion function...';
EXEC sp_executesql N'
CREATE FUNCTION dbo.lakeflowUtilityVersion()
RETURNS NVARCHAR(10)
AS
BEGIN
    RETURN ''1.7'';
END';
PRINT N'Created lakeflowUtilityVersion function';

PRINT N'Creating lakeflowVersionComponent function...';
EXEC sp_executesql N'
CREATE FUNCTION dbo.lakeflowVersionComponent(@objectName SYSNAME, @part INT)
RETURNS INT
AS
BEGIN
    -- Numeric major (@part=2) or minor (@part=1) of a versioned name like lakeflowDdlAudit_1_6;
    -- one source for the version-ordering predicate reused by the setup procs.
    RETURN TRY_CAST(PARSENAME(REPLACE(@objectName, ''_'', ''.''), @part) AS INT);
END';
PRINT N'Created lakeflowVersionComponent function';

-- Create lakeflowFixPermissions
PRINT N'Creating lakeflowFixPermissions procedure...';
EXEC sp_executesql N'
CREATE PROCEDURE dbo.lakeflowFixPermissions
    @User NVARCHAR(128),
    @Tables NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @DatabaseUser NVARCHAR(128) = @User;
    DECLARE @Platform NVARCHAR(50) = dbo.lakeflowDetectPlatform();
    DECLARE @CatalogName NVARCHAR(128) = DB_NAME();
    DECLARE @ErrorMessage NVARCHAR(4000);
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @CurrentObject NVARCHAR(255);
    -- Set to 1 if any server-scoped grant fails, so a distinct terminal marker can flag pending work.
    DECLARE @ServerScopedFailed BIT = 0;

    -- Error codes and messages
    DECLARE @invalidModeErrorCode INT = 100000;
    DECLARE @insufficientUserPrivilegesCode INT = 100400;
    DECLARE @insufficientUserPrivilegesErrorMessage NVARCHAR(200);

    SET @insufficientUserPrivilegesErrorMessage = ''User executing this script is not a ''''db_owner'''' role member. To execute this script, please use a user that is.'';

    PRINT N''Starting permission fixes for: '' + @CatalogName;
    PRINT N''Platform: '' + @Platform;
    PRINT N''User: '' + @User;
    IF @Tables IS NOT NULL
        PRINT N''Tables parameter: '' + @Tables;

    BEGIN TRY
        -- Validate that current user is db_owner
        IF (IS_ROLEMEMBER(''db_owner'') = 0)
        BEGIN
            THROW @insufficientUserPrivilegesCode, @insufficientUserPrivilegesErrorMessage, 1;
        END

        -- User resolution
        IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE principal_id = DATABASE_PRINCIPAL_ID(@User))
        BEGIN
            -- Check if user exists as database user
            SELECT @DatabaseUser = dp.name
            FROM sys.database_principals dp
            INNER JOIN sys.server_principals sp ON dp.sid = sp.sid
            WHERE sp.name = CAST(@User AS sysname)
                AND dp.type IN (''S'', ''U'', ''G'')
                AND dp.name NOT IN (''guest'');

            -- If still no database user found, warn and skip
            IF @DatabaseUser IS NULL OR @DatabaseUser = @User
            BEGIN
                PRINT N''⚠ Warning: User/Login ['' + @User + ''] not found as database user. Skipping permission grants.'';
                PRINT N''  To fix: CREATE USER ['' + @User + ''] FOR LOGIN ['' + @User + ''];'';
                RETURN;
            END
            ELSE
            BEGIN
                PRINT N''Server login ['' + @User + ''] maps to database user ['' + @DatabaseUser + ''].'';
            END
        END

        IF @DatabaseUser = ''dbo''
        BEGIN
            PRINT N''Skipping permission grants (dbo already has all permissions).'';
            PRINT N''Permission setup completed for user: '' + @User;
            RETURN;
        END

        -- Database-scoped system views/tables, granted in the user database below. Server-scoped
        -- objects are not listed here; they live in @ServerScopedGrants and are granted from master.
        DECLARE @SystemObjects TABLE (ObjectName NVARCHAR(255));
        INSERT INTO @SystemObjects VALUES
            (''sys.objects''), (''sys.schemas''), (''sys.tables''), (''sys.columns''),
            (''sys.key_constraints''), (''sys.foreign_keys''), (''sys.check_constraints''),
            (''sys.default_constraints''), (''sys.triggers''), (''sys.indexes''),
            (''sys.index_columns''), (''sys.fulltext_index_columns''), (''sys.fulltext_indexes''),
            (''sys.change_tracking_tables''),
            (''cdc.change_tables''), (''cdc.captured_columns''), (''cdc.index_columns'');

        -- Server-scoped permissions, listed once here and granted individually from master below
        -- (single source of truth for the objects that can only be granted in the master database).
        DECLARE @ServerScopedGrants TABLE (Permission NVARCHAR(20), ObjectName NVARCHAR(128));
        INSERT INTO @ServerScopedGrants VALUES
            (''SELECT'', ''sys.change_tracking_databases''),
            (''EXECUTE'', ''sys.sp_tables''), (''EXECUTE'', ''sys.sp_columns_100''),
            (''EXECUTE'', ''sys.sp_pkeys''), (''EXECUTE'', ''sys.sp_statistics_100'');

        PRINT N'''';
        PRINT N''=== System Object Permissions ==='';

        DECLARE sys_cursor CURSOR FOR
            SELECT ObjectName FROM @SystemObjects;

        OPEN sys_cursor;
        FETCH NEXT FROM sys_cursor INTO @CurrentObject;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            BEGIN TRY
                -- Check if object exists before trying to grant (helps with CDC objects)
                IF @CurrentObject LIKE ''cdc.%''
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N''cdc'')
                    BEGIN
                        PRINT N''ℹ Skipping '' + @CurrentObject + '' (CDC not enabled)'';
                        FETCH NEXT FROM sys_cursor INTO @CurrentObject;
                        CONTINUE;
                    END
                END

                SET @SQL = ''GRANT SELECT ON '' + @CurrentObject + '' TO ['' + @DatabaseUser + '']'';
                EXEC sp_executesql @SQL;
                PRINT N''✓ Granted SELECT on '' + @CurrentObject;
            END TRY
            BEGIN CATCH
                PRINT N''⚠ Could not grant SELECT on '' + @CurrentObject + '': '' + ERROR_MESSAGE();
            END CATCH

            FETCH NEXT FROM sys_cursor INTO @CurrentObject;
        END

        CLOSE sys_cursor;
        DEALLOCATE sys_cursor;

        -- Grant server-scoped permissions: SELECT on sys.change_tracking_databases and EXECUTE on
        -- the required system stored procedures. SQL Server only allows these grants when the current
        -- database is master, so each is granted through its own dynamic ''USE master; GRANT ...''
        -- batch (USE only changes context within that batch and reverts afterward). Like the
        -- database-level grants above, this grants to an already-existing user and needs grant
        -- authority in master (sysadmin or equivalent); any grant it cannot make is reported as a
        -- per-object warning and the procedure continues.
        PRINT N'''';
        PRINT N''=== Server-Scoped Object Permissions ==='';

        IF @Platform = ''AZURE_SQL_DATABASE''
        BEGIN
            PRINT N''ℹ Skipping server-scoped permissions on Azure SQL Database'';
            PRINT N''  Database users have implicit EXECUTE access to system stored procedures,'';
            PRINT N''  and server-scoped catalog views cannot be granted on Azure SQL Database.'';
        END
        ELSE
        BEGIN
            DECLARE @serverScopedPerm NVARCHAR(20), @serverScopedObject NVARCHAR(128);
            DECLARE server_scoped_cursor CURSOR FOR
                SELECT Permission, ObjectName FROM @ServerScopedGrants;

            OPEN server_scoped_cursor;
            FETCH NEXT FROM server_scoped_cursor INTO @serverScopedPerm, @serverScopedObject;

            WHILE @@FETCH_STATUS = 0
            BEGIN
                BEGIN TRY
                    SET @SQL = N''USE master; GRANT '' + @serverScopedPerm + N'' ON '' + @serverScopedObject + N'' TO '' + QUOTENAME(@User) + N'';'';
                    EXEC sp_executesql @SQL;
                    PRINT N''✓ Granted '' + @serverScopedPerm + N'' on '' + @serverScopedObject + N'' in master'';
                END TRY
                BEGIN CATCH
                    PRINT N''⚠ Could not grant '' + @serverScopedPerm + N'' on '' + @serverScopedObject + N'' in master: '' + ERROR_MESSAGE();
                    SET @ServerScopedFailed = 1;
                END CATCH

                FETCH NEXT FROM server_scoped_cursor INTO @serverScopedPerm, @serverScopedObject;
            END

            CLOSE server_scoped_cursor;
            DEALLOCATE server_scoped_cursor;
        END

        -- Grant EXECUTE permissions on lakeflow utility functions required by setup validation.
        -- The ingestion user calls lakeflowDetectPlatform and lakeflowUtilityVersion
        -- directly during validation, so these grants are needed regardless of platform.
        PRINT N'''';
        PRINT N''=== Lakeflow Utility Function Permissions ==='';

        BEGIN TRY
            SET @SQL = ''GRANT EXECUTE ON [dbo].[lakeflowDetectPlatform] TO ['' + @DatabaseUser + '']'';
            EXEC sp_executesql @SQL;
            PRINT N''✓ Granted EXECUTE on lakeflowDetectPlatform'';
        END TRY
        BEGIN CATCH
            PRINT N''⚠ Could not grant EXECUTE on lakeflowDetectPlatform: '' + ERROR_MESSAGE();
        END CATCH

        BEGIN TRY
            SET @SQL = ''GRANT EXECUTE ON [dbo].[lakeflowUtilityVersion] TO ['' + @DatabaseUser + '']'';
            EXEC sp_executesql @SQL;
            PRINT N''✓ Granted EXECUTE on lakeflowUtilityVersion'';
        END TRY
        BEGIN CATCH
            PRINT N''⚠ Could not grant EXECUTE on lakeflowUtilityVersion: '' + ERROR_MESSAGE();
        END CATCH

        -- Handle table-specific permissions if @Tables parameter is provided
        IF @Tables IS NOT NULL
        BEGIN
            PRINT N'''';
            PRINT N''=== Table-Level SELECT Permissions ==='';

            DECLARE @TargetTables TABLE (
                SchemaName NVARCHAR(128),
                TableName NVARCHAR(128),
                FullName NVARCHAR(261),
                ObjectId INT
            );

            -- Table discovery logic
            IF @Tables = ''ALL''
            BEGIN
                PRINT N''Discovering all user tables in database...'';
                INSERT INTO @TargetTables (SchemaName, TableName, FullName, ObjectId)
                SELECT
                    s.name, t.name,
                    QUOTENAME(s.name) + ''.'' + QUOTENAME(t.name),
                    t.object_id
                FROM sys.tables t
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE t.type = ''U''
                    AND s.name COLLATE DATABASE_DEFAULT NOT IN (''sys'', ''information_schema'', ''cdc'', ''INFORMATION_SCHEMA'', ''guest'');
            END
            ELSE IF @Tables LIKE ''SCHEMAS:%''
            BEGIN
                DECLARE @SchemaList NVARCHAR(MAX) = SUBSTRING(@Tables, 9, LEN(@Tables));
                -- Accept bracket-quoted schema names such as [dbo]; the catalog is matched unbracketed.
                SET @SchemaList = REPLACE(REPLACE(@SchemaList, ''['', ''''), '']'', '''');
                PRINT N''Discovering tables in schemas: '' + @SchemaList;
                DECLARE @SchemaXML XML;
                SET @SchemaXML = CAST(''<schema>'' + REPLACE(@SchemaList, '','', ''</schema><schema>'') + ''</schema>'' AS XML);
                INSERT INTO @TargetTables (SchemaName, TableName, FullName, ObjectId)
                SELECT
                    s.name, t.name,
                    QUOTENAME(s.name) + ''.'' + QUOTENAME(t.name),
                    t.object_id
                FROM sys.tables t
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE t.type = ''U''
                    AND s.name COLLATE DATABASE_DEFAULT IN (
                        SELECT LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(x.value(''(./text())[1]'', ''NVARCHAR(MAX)''), CHAR(10), ''''), CHAR(13), ''''), CHAR(9), '''')))
                        FROM @SchemaXML.nodes(''/schema'') AS T(x)
                        WHERE LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(x.value(''(./text())[1]'', ''NVARCHAR(MAX)''), CHAR(10), ''''), CHAR(13), ''''), CHAR(9), ''''))) != ''''
                    );
            END
            ELSE
            BEGIN
                PRINT N''Processing specified tables: '' + @Tables;
                DECLARE @TableList TABLE (FullTableName NVARCHAR(261));
                INSERT INTO @TableList (FullTableName)
                SELECT LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(Split.a.value(''.'', ''NVARCHAR(MAX)''), CHAR(10), ''''), CHAR(13), ''''), CHAR(9), ''''))) AS value
                FROM (
                    SELECT CAST(''<M>'' + REPLACE(@Tables, '','', ''</M><M>'') + ''</M>'' AS XML) AS Data
                ) AS A
                CROSS APPLY Data.nodes(''/M'') AS Split(a)
                WHERE LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(Split.a.value(''.'', ''NVARCHAR(MAX)''), CHAR(10), ''''), CHAR(13), ''''), CHAR(9), ''''))) != '''';

                -- Accept bracket-quoted identifiers such as [dbo].[employees]; the catalog names matched
                -- below are unbracketed. Strip only when brackets are balanced and any star is a trailing
                -- .* wildcard, so a comma-split fragment (from a bracketed name containing a comma), a name
                -- containing a lone ], or a bracket-quoted star [dbo].[*] is left as-is and stays a no-op
                -- rather than mis-matching a different table or expanding to the dbo.* wildcard.
                UPDATE @TableList
                SET FullTableName = REPLACE(REPLACE(FullTableName, ''['', ''''), '']'', '''')
                WHERE DATALENGTH(REPLACE(FullTableName, ''['', '''')) = DATALENGTH(REPLACE(FullTableName, '']'', ''''))
                    AND (CHARINDEX(''*'', FullTableName) = 0 OR FullTableName LIKE ''%.*'');

                INSERT INTO @TargetTables (SchemaName, TableName, FullName, ObjectId)
                SELECT
                    s.name, t.name,
                    QUOTENAME(s.name) + ''.'' + QUOTENAME(t.name),
                    t.object_id
                FROM sys.tables t
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                INNER JOIN @TableList tl ON
                    (tl.FullTableName = s.name COLLATE DATABASE_DEFAULT + ''.*'' OR
                     tl.FullTableName = s.name COLLATE DATABASE_DEFAULT + ''.'' + t.name COLLATE DATABASE_DEFAULT OR
                     (CHARINDEX(''.'', tl.FullTableName) = 0 AND tl.FullTableName = t.name COLLATE DATABASE_DEFAULT AND s.name COLLATE DATABASE_DEFAULT = ''dbo''))
                WHERE t.type = ''U'';
            END

            -- Grant SELECT permissions on discovered tables
            DECLARE @ProcessedCount INT = 0, @ErrorCount INT = 0;
            DECLARE @CurrentSchema NVARCHAR(128), @CurrentTable NVARCHAR(128), @CurrentFullName NVARCHAR(261);

            DECLARE table_cursor CURSOR FOR
                SELECT SchemaName, TableName, FullName FROM @TargetTables ORDER BY SchemaName, TableName;

            OPEN table_cursor;
            FETCH NEXT FROM table_cursor INTO @CurrentSchema, @CurrentTable, @CurrentFullName;

            WHILE @@FETCH_STATUS = 0
            BEGIN
                BEGIN TRY
                    SET @SQL = N''GRANT SELECT ON '' + @CurrentFullName + '' TO ['' + @DatabaseUser + '']'';
                    EXEC sp_executesql @SQL;
                    PRINT N''✓ Granted SELECT on '' + @CurrentFullName;
                    SET @ProcessedCount = @ProcessedCount + 1;
                END TRY
                BEGIN CATCH
                    PRINT N''✗ Error granting SELECT on '' + @CurrentFullName + '': '' + ERROR_MESSAGE();
                    SET @ErrorCount = @ErrorCount + 1;
                END CATCH

                FETCH NEXT FROM table_cursor INTO @CurrentSchema, @CurrentTable, @CurrentFullName;
            END

            CLOSE table_cursor;
            DEALLOCATE table_cursor;

            -- Summary for table permissions
            PRINT N'''';
            PRINT N''Table permission summary:'';
            PRINT N''  - Tables processed: '' + CAST(@ProcessedCount AS NVARCHAR(10));
            PRINT N''  - Tables with errors: '' + CAST(@ErrorCount AS NVARCHAR(10));
        END

        PRINT N'''';
        PRINT N''Permission fixes completed for user: '' + @User;

        -- Platform-specific guidance
        IF @Platform = ''AZURE_SQL_DATABASE''
        BEGIN
            PRINT N'''';
            PRINT N''=== Azure SQL Database Platform Notes ==='';
            PRINT N''• System stored procedures: Accessible by default to database users (no grants needed)'';
            PRINT N''• Server-scoped catalog views: Limited access in Azure SQL Database'';
            PRINT N''• Consider granting db_datareader role for broader access'';
            PRINT N''• CDC objects are only available when CDC is enabled on the database'';
            PRINT N'''';
            PRINT N''=== Recommended Additional Access ==='';
            PRINT N''-- Grant broader database-level access for comprehensive permissions:'';
            PRINT N''USE ['' + @CatalogName + ''];'';
            PRINT N''ALTER ROLE db_datareader ADD MEMBER ['' + @DatabaseUser + ''];'';
            PRINT N'''';
            PRINT N''=== Server-Scoped Limitations ==='';
            PRINT N''• sys.change_tracking_databases: Requires server-level access (typically not available)'';
            PRINT N''• Most Azure SQL Database deployments cannot grant server-level permissions'';
            PRINT N''• Contact your Azure administrator if server-level access is specifically required'';
        END
        ELSE IF @Platform = ''AZURE_SQL_MANAGED_INSTANCE''
        BEGIN
            PRINT N'''';
            PRINT N''=== Azure SQL Managed Instance Platform Notes ==='';
            PRINT N''• Most permissions granted successfully at database level'';
        END
        ELSE
        BEGIN
            PRINT N'''';
            PRINT N''=== Platform Notes ==='';
            PRINT N''• All permissions granted successfully at database level'';
            PRINT N''• No additional server-level configuration required'';
        END

        -- Distinct, greppable terminal marker printed only when a server-scoped grant failed, so a
        -- human reading the log can spot pending work despite the success-shaped notes above. This is
        -- a log signal only; no caller parses the procedure''s output.
        IF @ServerScopedFailed = 1
            PRINT N''RESULT: SERVER-SCOPED PERMISSIONS NOT GRANTED - a sysadmin must grant them in master (see the ⚠ warnings above)'';

    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ''Error in lakeflowFixPermissions: '' + ERROR_MESSAGE();
        PRINT @ErrorMessage;
        THROW;
    END CATCH
END';
PRINT N'Created lakeflowFixPermissions procedure';

-- Create lakeflowSetupChangeTracking
PRINT N'Creating lakeflowSetupChangeTracking procedure...';
EXEC sp_executesql N'
CREATE PROCEDURE dbo.lakeflowSetupChangeTracking
    @Tables NVARCHAR(MAX) = NULL,
    @User NVARCHAR(128) = NULL,
    @Retention NVARCHAR(50) = ''2 DAYS'',
    @Mode NVARCHAR(10) = ''INSTALL'',
    @CreateDdlSupportingObjects BIT = 0
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @DatabaseUser NVARCHAR(128) = @User;
    DECLARE @Platform NVARCHAR(50) = dbo.lakeflowDetectPlatform();
    DECLARE @CatalogName NVARCHAR(128) = DB_NAME();
    DECLARE @ErrorMessage NVARCHAR(4000);
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @versionSuffix NVARCHAR(10) = ''_1_7'';
    DECLARE @ddlAuditTableName NVARCHAR(100) = ''lakeflowDdlAudit'' + @versionSuffix;
    DECLARE @ddlAuditTriggerName NVARCHAR(100) = ''lakeflowDdlAuditTrigger'' + @versionSuffix;

    -- Error codes and messages
    DECLARE @invalidModeErrorCode INT = 100000;
    DECLARE @invalidModeErrorMessage NVARCHAR(200);
    DECLARE @insufficientUserPrivilegesCode INT = 100400;
    DECLARE @insufficientUserPrivilegesErrorMessage NVARCHAR(200);

    SET @invalidModeErrorMessage = CONCAT(''Provided execution mode: '', @Mode, '', is not recognized. Allowed values are: INSTALL, CLEANUP'');
    SET @insufficientUserPrivilegesErrorMessage = ''User executing this script is not a ''''db_owner'''' role member. To execute this script, please use a user that is.'';

    PRINT N''Starting change tracking setup for: '' + @CatalogName;
    PRINT N''Platform: '' + @Platform;
    PRINT N''Mode: '' + @Mode;
    IF @Tables IS NOT NULL
        PRINT N''Tables: '' + @Tables;

    BEGIN TRY
        -- Validate execution mode
        IF (@Mode != ''INSTALL'' AND @Mode != ''CLEANUP'')
        BEGIN
            THROW @invalidModeErrorCode, @invalidModeErrorMessage, 1;
        END

        -- Validate that current user is db_owner
        IF (IS_ROLEMEMBER(''db_owner'') = 0)
        BEGIN
            THROW @insufficientUserPrivilegesCode, @insufficientUserPrivilegesErrorMessage, 1;
        END

        -- Cleanup legacy DDL support objects
        IF EXISTS (SELECT 1 FROM sys.triggers WHERE name = ''replicate_io_audit_ddl_trigger_1'' AND parent_class = 0)
            OR OBJECT_ID(''dbo.replicate_io_audit_ddl_1'', ''U'') IS NOT NULL
            OR OBJECT_ID(''dbo.replicate_io_audit_tbl_cons_1'', ''U'') IS NOT NULL
            OR OBJECT_ID(''dbo.replicate_io_audit_tbl_schema_1'', ''U'') IS NOT NULL
            OR EXISTS (SELECT 1 FROM sys.triggers WHERE name = ''alterTableTrigger_1'' AND parent_class = 0)
            OR OBJECT_ID(''dbo.disableOldCaptureInstance_1'', ''P'') IS NOT NULL
            OR OBJECT_ID(''dbo.refreshCaptureInstance_1'', ''P'') IS NOT NULL
            OR OBJECT_ID(''dbo.mergeCaptureInstance_1'', ''P'') IS NOT NULL
            OR OBJECT_ID(''dbo.captureInstanceTracker_1'', ''U'') IS NOT NULL
        BEGIN
            PRINT N''Cleaning up legacy DDL support objects...'';

            IF EXISTS (SELECT 1 FROM sys.triggers WHERE name = ''replicate_io_audit_ddl_trigger_1'' AND parent_class = 0)
            BEGIN
                EXEC(''DROP TRIGGER replicate_io_audit_ddl_trigger_1 ON DATABASE'');
                PRINT N''✓ Dropped legacy trigger: replicate_io_audit_ddl_trigger_1'';
            END

            IF OBJECT_ID(''dbo.replicate_io_audit_ddl_1'', ''U'') IS NOT NULL
            BEGIN
                EXEC(''DROP TABLE dbo.replicate_io_audit_ddl_1'');
                PRINT N''✓ Dropped legacy table: replicate_io_audit_ddl_1'';
            END

            IF OBJECT_ID(''dbo.replicate_io_audit_tbl_cons_1'', ''U'') IS NOT NULL
            BEGIN
                EXEC(''DROP TABLE dbo.replicate_io_audit_tbl_cons_1'');
                PRINT N''✓ Dropped legacy table: replicate_io_audit_tbl_cons_1'';
            END

            IF OBJECT_ID(''dbo.replicate_io_audit_tbl_schema_1'', ''U'') IS NOT NULL
            BEGIN
                EXEC(''DROP TABLE dbo.replicate_io_audit_tbl_schema_1'');
                PRINT N''✓ Dropped legacy table: replicate_io_audit_tbl_schema_1'';
            END

            IF EXISTS (SELECT name FROM sys.triggers WHERE name = ''alterTableTrigger_1'' AND type = ''TR'')
            BEGIN
                EXEC(''DROP TRIGGER alterTableTrigger_1 ON DATABASE'');
                PRINT N''✓ Dropped legacy trigger: alterTableTrigger_1'';
            END

            IF OBJECT_ID(''dbo.disableOldCaptureInstance_1'', ''P'') IS NOT NULL
            BEGIN
                EXEC(''DROP PROCEDURE dbo.disableOldCaptureInstance_1'');
                PRINT N''✓ Dropped legacy procedure: disableOldCaptureInstance_1'';
            END

            IF OBJECT_ID(''dbo.refreshCaptureInstance_1'', ''P'') IS NOT NULL
            BEGIN
                EXEC(''DROP PROCEDURE dbo.refreshCaptureInstance_1'');
                PRINT N''✓ Dropped legacy procedure: refreshCaptureInstance_1'';
            END

            IF OBJECT_ID(''dbo.mergeCaptureInstance_1'', ''P'') IS NOT NULL
            BEGIN
                EXEC(''DROP PROCEDURE dbo.mergeCaptureInstance_1'');
                PRINT N''✓ Dropped legacy procedure: mergeCaptureInstance_1'';
            END

            IF OBJECT_ID(''dbo.captureInstanceTracker_1'', ''U'') IS NOT NULL
            BEGIN
                EXEC(''DROP TABLE dbo.captureInstanceTracker_1'');
                PRINT N''✓ Dropped legacy table: captureInstanceTracker_1'';
            END

            PRINT N''Legacy DDL support objects cleanup completed'';
        END

        -- Cleanup mode: Remove DDL support objects
        IF @Mode = ''CLEANUP''
        BEGIN
            PRINT N''Cleaning up CT DDL support objects...'';

            -- Drop DDL audit trigger
            IF EXISTS (SELECT 1 FROM sys.triggers WHERE name = CAST(@ddlAuditTriggerName AS sysname) AND parent_class = 0)
            BEGIN
                SET @SQL = N''DROP TRIGGER ['' + @ddlAuditTriggerName + ''] ON DATABASE'';
                EXEC sp_executesql @SQL;
                PRINT N''✓ Dropped trigger: '' + @ddlAuditTriggerName;
            END

            -- Drop DDL audit table
            IF OBJECT_ID(''dbo.'' + @ddlAuditTableName, ''U'') IS NOT NULL
            BEGIN
                SET @SQL = N''DROP TABLE [dbo].['' + @ddlAuditTableName + '']'';
                EXEC sp_executesql @SQL;
                PRINT N''✓ Dropped table: '' + @ddlAuditTableName;
            END

            -- Pattern-based cleanup for any remaining CT objects across versions.
            DECLARE @ctCleanupSql NVARCHAR(MAX) = '''';

            SELECT @ctCleanupSql = @ctCleanupSql + ''DROP TRIGGER ['' + name + ''] ON DATABASE;'' + CHAR(13)
            FROM sys.triggers
            WHERE ((name LIKE ''lakeflowDdlAuditTrigger_%_%'' AND name != CAST(@ddlAuditTriggerName AS sysname))
                OR name LIKE ''replicantDdlAuditTrigger_%_%'')
              AND parent_class = 0;

            IF LEN(@ctCleanupSql) > 0
            BEGIN
                EXEC sp_executesql @ctCleanupSql;
                PRINT N''✓ Cleaned up remaining DDL audit triggers across versions'';
            END

            SET @ctCleanupSql = '''';
            SELECT @ctCleanupSql = @ctCleanupSql + ''DROP TABLE [dbo].['' + name + ''];'' + CHAR(13)
            FROM sys.tables
            WHERE (name LIKE ''lakeflowDdlAudit_%_%'' AND name != CAST(@ddlAuditTableName AS sysname))
               OR name LIKE ''replicantDdlAudit_%_%'';

            IF LEN(@ctCleanupSql) > 0
            BEGIN
                EXEC sp_executesql @ctCleanupSql;
                PRINT N''✓ Cleaned up remaining DDL audit tables across versions'';
            END

            PRINT N''CT DDL support objects cleanup completed'';
            RETURN;
        END

        -- Install mode continues here
        PRINT N''Setting up change tracking infrastructure...'';

        -- Check if change tracking is enabled at database level
        IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_databases ctd
                       INNER JOIN sys.databases d ON ctd.database_id = d.database_id
                       WHERE d.name COLLATE DATABASE_DEFAULT = DB_NAME())
        BEGIN
            PRINT N''Enabling change tracking at database level...'';
            SET @SQL = N''ALTER DATABASE '' + QUOTENAME(@CatalogName) + '' SET CHANGE_TRACKING = ON (CHANGE_RETENTION = '' + @Retention + '', AUTO_CLEANUP = ON)'';
            EXEC sp_executesql @SQL;
            PRINT N''✓ Change tracking enabled at database level'';
        END
        ELSE
        BEGIN
            PRINT N''ℹ Change tracking already enabled at database level'';
        END

        -- Optionally create the CT DDL supporting objects (audit table + audit trigger)
        IF @CreateDdlSupportingObjects = 1
        BEGIN
        -- Current version, parsed numerically; bounds the table carry-forward below to strictly-older, so a
        -- newer table (a downgrade) is never renamed onto the current name and downgraded.
        DECLARE @auditCurMajor INT = dbo.lakeflowVersionComponent(@ddlAuditTableName, 2);
        DECLARE @auditCurMinor INT = dbo.lakeflowVersionComponent(@ddlAuditTableName, 1);
        -- The versioned PK constraint name, built once so the carry-forward rename target and the fresh
        -- CREATE below cannot drift apart.
        DECLARE @auditPkName SYSNAME = ''replicantDdlAuditPrimaryKey_'' + @versionSuffix;
        -- Audit column char widths, declared once so the gate below and the CREATE TABLE stay in sync
        -- (sys.columns max_length is bytes = 2x chars).
        DECLARE @wAuditName INT = 128;
        DECLARE @wAuditType INT = 30;
        DECLARE @wAuditSql INT = 2000;

        -- Select a compatible prior-version audit table to carry forward, before the cutover transaction so
        -- the transaction holds only the swap. Compatible = the full current audit column contract (name,
        -- type, length, nullability, identity) and no extras, with Change Tracking; if more than one exists,
        -- keep the highest version and drop the rest. Validating the full contract (not just names or count)
        -- stops a same-named but wrong-typed table from being renamed onto the current name and then breaking
        -- the recreated audit trigger INSERT.
        DECLARE @auditPrior SYSNAME;
        SELECT TOP 1 @auditPrior = t.name
        FROM sys.tables t
        WHERE t.name LIKE ''lakeflowDdlAudit_%_%''
          AND t.name <> CAST(@ddlAuditTableName AS sysname)
          AND EXISTS (SELECT 1 FROM sys.change_tracking_tables ct WHERE ct.object_id = t.object_id)
          -- All 8 columns present with the exact type/length/nullability/identity the current audit trigger
          -- writes into (matched rows = 8), and no extras (total columns = 8).
          AND (SELECT COUNT(*) FROM sys.columns c JOIN sys.types ty ON c.user_type_id = ty.user_type_id
               WHERE c.object_id = t.object_id
                 AND ((c.name = ''SERIAL_NUMBER'' AND ty.name = ''int'' AND c.is_nullable = 0 AND c.is_identity = 1)
                   OR (c.name = ''CURRENT_USER'' AND ty.name = ''nvarchar'' AND c.max_length = @wAuditName * 2 AND c.is_nullable = 1 AND c.is_identity = 0)
                   OR (c.name = ''SCHEMA_NAME'' AND ty.name = ''nvarchar'' AND c.max_length = @wAuditName * 2 AND c.is_nullable = 1 AND c.is_identity = 0)
                   OR (c.name = ''TABLE_NAME'' AND ty.name = ''nvarchar'' AND c.max_length = @wAuditName * 2 AND c.is_nullable = 1 AND c.is_identity = 0)
                   OR (c.name = ''TYPE'' AND ty.name = ''nvarchar'' AND c.max_length = @wAuditType * 2 AND c.is_nullable = 1 AND c.is_identity = 0)
                   OR (c.name = ''OPERATION_TYPE'' AND ty.name = ''nvarchar'' AND c.max_length = @wAuditType * 2 AND c.is_nullable = 1 AND c.is_identity = 0)
                   OR (c.name = ''SQL_TXT'' AND ty.name = ''nvarchar'' AND c.max_length = @wAuditSql * 2 AND c.is_nullable = 1 AND c.is_identity = 0)
                   OR (c.name = ''LOGICAL_POSITION'' AND ty.name = ''bigint'' AND c.is_nullable = 0 AND c.is_identity = 0))) = 8
          AND (SELECT COUNT(*) FROM sys.columns c WHERE c.object_id = t.object_id) = 8
          -- Only carry forward an older version, never one newer than this script (a downgrade).
          AND (dbo.lakeflowVersionComponent(t.name, 2) < @auditCurMajor
               OR (dbo.lakeflowVersionComponent(t.name, 2) = @auditCurMajor
                   AND dbo.lakeflowVersionComponent(t.name, 1) < @auditCurMinor))
        ORDER BY dbo.lakeflowVersionComponent(t.name, 2) DESC,
                 dbo.lakeflowVersionComponent(t.name, 1) DESC;

        -- Announce an incompatible prior (its DDL stream restarts on a fresh table) before the cutover.
        IF @auditPrior IS NULL AND OBJECT_ID(''dbo.'' + @ddlAuditTableName, ''U'') IS NULL
              AND EXISTS (SELECT 1 FROM sys.tables
                          WHERE name LIKE ''lakeflowDdlAudit_%_%''
                            AND name <> CAST(@ddlAuditTableName AS sysname))
        BEGIN
            -- A newer-version prior is left in place (version bound above); report it as a downgrade,
            -- not a contract mismatch, and name the table.
            DECLARE @newerAudit SYSNAME = (
                SELECT TOP 1 name FROM sys.tables
                WHERE name LIKE ''lakeflowDdlAudit_%_%'' AND name <> CAST(@ddlAuditTableName AS sysname)
                  AND (dbo.lakeflowVersionComponent(name, 2) > @auditCurMajor
                       OR (dbo.lakeflowVersionComponent(name, 2) = @auditCurMajor
                           AND dbo.lakeflowVersionComponent(name, 1) > @auditCurMinor))
                ORDER BY dbo.lakeflowVersionComponent(name, 2) DESC, dbo.lakeflowVersionComponent(name, 1) DESC);
            DECLARE @otherAudit SYSNAME = (
                SELECT TOP 1 name FROM sys.tables
                WHERE name LIKE ''lakeflowDdlAudit_%_%'' AND name <> CAST(@ddlAuditTableName AS sysname)
                ORDER BY name);
            IF @newerAudit IS NOT NULL
                PRINT N''ℹ Newer DDL audit table '' + @newerAudit + '' found; its rows are preserved, but this older version is now active for capture. You may be running an older setup script.'';
            ELSE
                PRINT N''⚠ Existing DDL audit table '' + ISNULL(@otherAudit, N''(unknown)'') + '' does not match this version; creating a fresh one (DDL audit history will restart).'';
        END

        -- Atomic cutover in ONE transaction: drop the non-current audit trigger FIRST (taking the DDL-trigger
        -- lock up front), then rename the prior table forward (or create a fresh one), then create the current
        -- trigger. A concurrent ALTER TABLE blocks on that lock until commit and then fires the current trigger,
        -- so it can never fire the old trigger against a table this cutover has already renamed away (which
        -- would deadlock and lose the change). A mid-cutover failure rolls back cleanly.
        -- Trade-off: holding the DDL-trigger lock for the whole cutover blocks any concurrent customer ALTER
        -- TABLE in this database until commit. The cutover is metadata-only and normally sub-second, so the
        -- window is small; a customer ALTER whose own timeout is shorter fails fast (retryable) rather than
        -- silently losing the change.
        -- NOTE: the capture-instance cutover in lakeflowSetupChangeDataCapture mirrors this transaction
        -- scaffolding (lock timeout, XACT_ABORT, rollback, version-bounded stale-drop); keep the two in sync.
        BEGIN TRY
            -- Fail fast rather than freeze all customer DDL if a lock is contended, and ensure a client
            -- cancel aborts and rolls back the transaction instead of leaving it open holding the
            -- database-scoped DDL lock.
            SET LOCK_TIMEOUT 30000;
            SET XACT_ABORT ON;
            BEGIN TRANSACTION;

            -- Drop the non-current audit trigger before the rename so the DDL-trigger lock is held from the
            -- start of the transaction (see the ordering note above). The current trigger is created below,
            -- after the rename.
            DECLARE @dropOldAuditTrig NVARCHAR(MAX) = '''';
            SELECT @dropOldAuditTrig = @dropOldAuditTrig + ''DROP TRIGGER '' + QUOTENAME(name) + '' ON DATABASE;'' + CHAR(13)
            FROM sys.triggers
            WHERE name LIKE ''lakeflowDdlAuditTrigger_%_%''
              AND name <> CAST(@ddlAuditTriggerName AS sysname) AND parent_class = 0;
            IF LEN(@dropOldAuditTrig) > 0
            BEGIN
                PRINT N''  Dropping prior-version DDL audit trigger(s):'' + CHAR(13) + @dropOldAuditTrig;
                EXEC sp_executesql @dropOldAuditTrig;
            END

            IF @auditPrior IS NOT NULL AND OBJECT_ID(''dbo.'' + @ddlAuditTableName, ''U'') IS NULL
            BEGIN
                DECLARE @auditPriorQualified NVARCHAR(300) = ''dbo.'' + @auditPrior;
                EXEC sp_rename @objname = @auditPriorQualified, @newname = @ddlAuditTableName;

                -- Keep the versioned PK constraint name in sync with the table version.
                DECLARE @auditPk SYSNAME = (SELECT name FROM sys.key_constraints
                                            WHERE parent_object_id = OBJECT_ID(''dbo.'' + @ddlAuditTableName) AND type = ''PK'');
                IF @auditPk IS NOT NULL AND @auditPk <> @auditPkName AND OBJECT_ID(@auditPkName) IS NULL
                    EXEC sp_rename @objname = @auditPk, @newname = @auditPkName, @objtype = ''OBJECT'';

                -- Drop any other prior-version audit tables (stale duplicates; the live one just moved).
                DECLARE @dropExtraAudit NVARCHAR(MAX) = '''';
                SELECT @dropExtraAudit = @dropExtraAudit + ''DROP TABLE [dbo].'' + QUOTENAME(name) + '';'' + CHAR(13)
                FROM sys.tables
                WHERE name LIKE ''lakeflowDdlAudit_%_%'' AND name <> CAST(@ddlAuditTableName AS sysname)
                  -- Only drop strictly-older duplicates; never a newer table (matches the selection bound).
                  AND (dbo.lakeflowVersionComponent(name, 2) < @auditCurMajor
                       OR (dbo.lakeflowVersionComponent(name, 2) = @auditCurMajor
                           AND dbo.lakeflowVersionComponent(name, 1) < @auditCurMinor));
                IF LEN(@dropExtraAudit) > 0
                BEGIN
                    PRINT N''  Dropping stale DDL audit tables:'' + CHAR(13) + @dropExtraAudit;
                    EXEC sp_executesql @dropExtraAudit;
                END

                PRINT N''✓ Carried DDL audit table forward: renamed '' + @auditPrior + '' to '' + @ddlAuditTableName;
            END

            -- No compatible prior: create a fresh current audit table. This column set is also encoded in
            -- the carry-forward compatibility gate above (per-column predicates + the two = 8 counts): any
            -- column name/count/nullability/identity change here must update the gate too, or the next
            -- upgrade silently fails the gate and recreates a fresh table (audit history restarts).
            IF OBJECT_ID(''dbo.'' + @ddlAuditTableName, ''U'') IS NULL
            BEGIN
                SET @SQL = N''CREATE TABLE [dbo].['' + @ddlAuditTableName + ''](
                    [SERIAL_NUMBER] INT IDENTITY NOT NULL,
                    [CURRENT_USER] NVARCHAR('' + CAST(@wAuditName AS VARCHAR(10)) + '') NULL,
                    [SCHEMA_NAME] NVARCHAR('' + CAST(@wAuditName AS VARCHAR(10)) + '') NULL,
                    [TABLE_NAME] NVARCHAR('' + CAST(@wAuditName AS VARCHAR(10)) + '') NULL,
                    [TYPE] NVARCHAR('' + CAST(@wAuditType AS VARCHAR(10)) + '') NULL,
                    [OPERATION_TYPE] NVARCHAR('' + CAST(@wAuditType AS VARCHAR(10)) + '') NULL,
                    [SQL_TXT] NVARCHAR('' + CAST(@wAuditSql AS VARCHAR(10)) + '') NULL,
                    [LOGICAL_POSITION] BIGINT NOT NULL,
                    CONSTRAINT ['' + @auditPkName + ''] PRIMARY KEY ([SERIAL_NUMBER], [LOGICAL_POSITION]))'';
                EXEC sp_executesql @SQL;
                PRINT N''✓ Created DDL audit table: '' + @ddlAuditTableName;
            END

            -- Enable change tracking BEFORE arming the trigger (a fresh table needs it; a carried-forward
            -- table already has it via sp_rename, so this is skipped). Doing it before the trigger, inside the
            -- transaction, stops the trigger from writing rows to a not-yet-tracked table -- rows that the
            -- change-tracking reader would never see.
            IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_tables
                           WHERE object_id = OBJECT_ID(''dbo.'' + @ddlAuditTableName))
            BEGIN
                SET @SQL = N''ALTER TABLE [dbo].['' + @ddlAuditTableName + ''] ENABLE CHANGE_TRACKING'';
                EXEC sp_executesql @SQL;
                PRINT N''✓ Enabled change tracking on DDL audit table'';
            END

            -- Create DDL audit trigger (the non-current trigger was already dropped at the top of the
            -- transaction, before the rename).
            IF NOT EXISTS (SELECT 1 FROM sys.triggers WHERE name = CAST(@ddlAuditTriggerName AS sysname))
            BEGIN
                DECLARE @QuotedDbName NVARCHAR(255) = QUOTENAME(DB_NAME());
                SET @SQL = CAST(N''CREATE TRIGGER ['' AS NVARCHAR(MAX)) + @ddlAuditTriggerName + ''] ON DATABASE
                    FOR ALTER_TABLE
                    AS
                    SET NOCOUNT ON;
                    SET ANSI_PADDING ON;
                    SET ANSI_NULLS ON;
                    SET QUOTED_IDENTIFIER ON;
                    DECLARE @DbName NVARCHAR(255),
                            @SchemaName NVARCHAR(max),
                            @TableName NVARCHAR(255),
                            @QuotedFullName NVARCHAR(max),
                            @objectType NVARCHAR(30),
                            @data XML,
                            @changeVersion NVARCHAR(30),
                            @operation NVARCHAR(30),
                            @capturedSql NVARCHAR(2000),
                            @isCTEnabledDBLevel bit,
                            @isCTEnabledTableLevel bit,
                            @isColumnAdd nvarchar(255),
                            @isAlterColumn nvarchar(255),
                            @isDropColumn nvarchar(255);
    
                        SET @data = EVENTDATA();
                        SET @changeVersion = CHANGE_TRACKING_CURRENT_VERSION();
                        SET @DbName = DB_NAME();
                        SET @SchemaName = @data.value(''''(/EVENT_INSTANCE/SchemaName)[1]'''',  ''''NVARCHAR(MAX)'''');
                        SET @TableName = @data.value(''''(/EVENT_INSTANCE/ObjectName)[1]'''',  ''''NVARCHAR(255)'''');
                        SET @objectType = @data.value(''''(/EVENT_INSTANCE/ObjectType)[1]'''', ''''NVARCHAR(30)'''');
                        SET @QuotedFullName = QUOTENAME(@SchemaName) + ''''.'''' + QUOTENAME(@TableName);
                        SET @operation = @data.value(''''(/EVENT_INSTANCE/EventType)[1]'''', ''''NVARCHAR(30)'''');
                        SET @capturedSql = @data.value(''''(/EVENT_INSTANCE/TSQLCommand/CommandText)[1]'''', ''''NVARCHAR(2000)'''');
                        SET @isCTEnabledDBLevel = (SELECT COUNT(*) FROM sys.change_tracking_databases ctd
                                                    INNER JOIN sys.databases d ON ctd.database_id = d.database_id
                                                    WHERE d.name = CAST(@DbName AS sysname));
                        SET @isCTEnabledTableLevel = (SELECT COUNT(*) FROM sys.change_tracking_tables WHERE object_id = object_id(@QuotedFullName));
                        SET @isColumnAdd = @data.value(''''(/EVENT_INSTANCE/AlterTableActionList/Create)[1]'''', ''''NVARCHAR(255)'''');
                        SET @isAlterColumn = @data.value(''''(/EVENT_INSTANCE/AlterTableActionList/Alter)[1]'''', ''''NVARCHAR(255)'''');
                        SET @isDropColumn = @data.value(''''(/EVENT_INSTANCE/AlterTableActionList/Drop)[1]'''', ''''NVARCHAR(255)'''');
    
                    IF ((@isCTEnabledDBLevel = 1 AND @isCTEnabledTableLevel = 1) AND ((@isColumnAdd IS NOT NULL) OR (@isAlterColumn IS NOT NULL) OR (@isDropColumn IS NOT NULL)))
                    BEGIN
                        INSERT INTO dbo.['' + @ddlAuditTableName + ''] (
                            [CURRENT_USER],
                            [SCHEMA_NAME],
                            [TABLE_NAME],
                            [TYPE],
                            [OPERATION_TYPE],
                            [SQL_TXT],
                            [LOGICAL_POSITION]
                        )
                        VALUES (
                            SUSER_NAME(),
                            @SchemaName,
                            @TableName,
                            @objectType,
                            @operation,
                            @capturedSql,
                            @changeVersion
                        );
                    END'';
                EXEC sp_executesql @SQL;
                PRINT N''✓ Created DDL audit trigger: '' + @ddlAuditTriggerName;
            END

            COMMIT TRANSACTION;
            SET XACT_ABORT OFF;
            SET LOCK_TIMEOUT -1;
        END TRY
        BEGIN CATCH
            IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
            SET XACT_ABORT OFF;
            SET LOCK_TIMEOUT -1;
            -- Error 1222 (lock request timeout) means a concurrent ALTER held the DDL lock past the
            -- cutover timeout: transient, and safe to re-run once the concurrent change completes.
            IF ERROR_NUMBER() = 1222
                PRINT N''✗ Cutover blocked by a concurrent ALTER TABLE (lock timeout); rolled back with no changes. This is transient -- re-run setup once the concurrent change completes.'';
            ELSE
                PRINT N''✗ Cutover rolled back; no changes were applied.'';
            THROW;
        END CATCH
        END

        -- User resolution
        IF @User IS NOT NULL AND @User != ''''
        BEGIN
            -- Check if user exists as database user
            IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE principal_id = DATABASE_PRINCIPAL_ID(@User))
            BEGIN
                -- Check if it is a server login and find its mapped database user
                SELECT @DatabaseUser = dp.name
                FROM sys.database_principals dp
                INNER JOIN sys.server_principals sp ON dp.sid = sp.sid
                WHERE sp.name = CAST(@User AS sysname)
                    AND dp.type IN (''S'', ''U'', ''G'')
                    AND dp.name NOT IN (''guest'');

                -- If still no database user found, warn
                IF @DatabaseUser IS NULL OR @DatabaseUser = @User
                BEGIN
                    PRINT N''⚠ Warning: User/Login ['' + @User + ''] not found as database user. Skipping permission grants.'';
                    PRINT N''  To fix: CREATE USER ['' + @User + ''] FOR LOGIN ['' + @User + ''];'';
                    SET @DatabaseUser = NULL;
                END
                ELSE
                BEGIN
                    PRINT N''Server login ['' + @User + ''] maps to database user ['' + @DatabaseUser + ''].'';
                END
            END

            IF @DatabaseUser = ''dbo''
            BEGIN
                PRINT N''Skipping permission grants (dbo already has all permissions).'';
                SET @DatabaseUser = NULL;
            END
        END

        -- Grant permissions to user if specified
        IF @DatabaseUser IS NOT NULL
        BEGIN
            PRINT N''Granting permissions to user: '' + @DatabaseUser;

            -- Grant permissions on the DDL audit table only when those objects were created
            IF @CreateDdlSupportingObjects = 1
            BEGIN
                -- Grant SELECT on DDL audit table
                SET @SQL = N''GRANT SELECT ON [dbo].['' + @ddlAuditTableName + ''] TO '' + QUOTENAME(@DatabaseUser);
                EXEC sp_executesql @SQL;
                PRINT N''✓ Granted SELECT on '' + @ddlAuditTableName + '' to '' + @DatabaseUser;

                -- Grant VIEW CHANGE TRACKING on DDL audit table
                SET @SQL = N''GRANT VIEW CHANGE TRACKING ON [dbo].['' + @ddlAuditTableName + ''] TO '' + QUOTENAME(@DatabaseUser);
                EXEC sp_executesql @SQL;
                PRINT N''✓ Granted VIEW CHANGE TRACKING on '' + @ddlAuditTableName + '' to '' + @DatabaseUser;
            END

            -- Grant VIEW DEFINITION to see database-level triggers
            SET @SQL = N''GRANT VIEW DEFINITION TO '' + QUOTENAME(@DatabaseUser);
            EXEC sp_executesql @SQL;
            PRINT N''✓ Granted VIEW DEFINITION to '' + @DatabaseUser;
        END

        -- Process tables if specified
        IF @Tables IS NOT NULL
        BEGIN
            PRINT N''Processing tables for change tracking enablement...'';

            -- Declare variables for table processing
            DECLARE @TargetTables TABLE (
                SchemaName NVARCHAR(128),
                TableName NVARCHAR(128),
                HasPrimaryKey BIT
            );

            DECLARE @SkippedTables NVARCHAR(MAX) = '''';
            DECLARE @SkippedTablesCount INT = 0;
            DECLARE @CurrentSchema NVARCHAR(128);
            DECLARE @CurrentTable NVARCHAR(128);
            DECLARE @ProcessedCount INT = 0;
            DECLARE @SkippedCount INT = 0;
            DECLARE @ErrorCount INT = 0;

            -- Parse table list and populate target tables
            IF @Tables = ''ALL''
            BEGIN
                INSERT INTO @TargetTables (SchemaName, TableName, HasPrimaryKey)
                SELECT
                    s.name,
                    t.name,
                    CASE WHEN EXISTS (
                        SELECT 1 FROM sys.key_constraints kc
                        WHERE kc.parent_object_id = t.object_id
                        AND kc.type = ''PK''
                    ) THEN 1 ELSE 0 END
                FROM sys.tables t
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE t.is_ms_shipped = 0;
            END
            ELSE IF @Tables LIKE ''SCHEMAS:%''
            BEGIN
                DECLARE @SchemaList NVARCHAR(MAX) = SUBSTRING(@Tables, 9, LEN(@Tables));
                -- Accept bracket-quoted schema names such as [dbo]; the catalog is matched unbracketed.
                SET @SchemaList = REPLACE(REPLACE(@SchemaList, ''['', ''''), '']'', '''');
                INSERT INTO @TargetTables (SchemaName, TableName, HasPrimaryKey)
                SELECT
                    s.name, t.name,
                    CASE WHEN pk.CONSTRAINT_NAME IS NOT NULL THEN 1 ELSE 0 END
                FROM sys.tables t
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS pk ON
                    pk.TABLE_SCHEMA COLLATE DATABASE_DEFAULT = s.name COLLATE DATABASE_DEFAULT AND pk.TABLE_NAME COLLATE DATABASE_DEFAULT = t.name COLLATE DATABASE_DEFAULT AND pk.CONSTRAINT_TYPE COLLATE DATABASE_DEFAULT = ''PRIMARY KEY''
                WHERE t.type = ''U''
                    AND s.name COLLATE DATABASE_DEFAULT IN (SELECT LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(Split.a.value(''.'', ''NVARCHAR(MAX)''), CHAR(10), ''''), CHAR(13), ''''), CHAR(9), ''''))) AS value
                FROM (
                    SELECT CAST(''<M>'' + REPLACE(@SchemaList, '','', ''</M><M>'') + ''</M>'' AS XML) AS Data
                ) AS A
                CROSS APPLY Data.nodes(''/M'') AS Split(a)
                WHERE LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(Split.a.value(''.'', ''NVARCHAR(MAX)''), CHAR(10), ''''), CHAR(13), ''''), CHAR(9), ''''))) != '''');
            END
            ELSE
            BEGIN
                PRINT N''Processing specified tables: '' + @Tables;
                DECLARE @TableList TABLE (FullTableName NVARCHAR(261));
                INSERT INTO @TableList (FullTableName)
                SELECT LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(Split.a.value(''.'', ''NVARCHAR(MAX)''), CHAR(10), ''''), CHAR(13), ''''), CHAR(9), ''''))) AS value
                FROM (
                    SELECT CAST(''<M>'' + REPLACE(@Tables, '','', ''</M><M>'') + ''</M>'' AS XML) AS Data
                ) AS A
                CROSS APPLY Data.nodes(''/M'') AS Split(a)
                WHERE LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(Split.a.value(''.'', ''NVARCHAR(MAX)''), CHAR(10), ''''), CHAR(13), ''''), CHAR(9), ''''))) != '''';

                -- Accept bracket-quoted identifiers such as [dbo].[employees]; the catalog names matched
                -- below are unbracketed. Strip only when brackets are balanced and any star is a trailing
                -- .* wildcard, so a comma-split fragment (from a bracketed name containing a comma), a name
                -- containing a lone ], or a bracket-quoted star [dbo].[*] is left as-is and stays a no-op
                -- rather than mis-matching a different table or expanding to the dbo.* wildcard.
                UPDATE @TableList
                SET FullTableName = REPLACE(REPLACE(FullTableName, ''['', ''''), '']'', '''')
                WHERE DATALENGTH(REPLACE(FullTableName, ''['', '''')) = DATALENGTH(REPLACE(FullTableName, '']'', ''''))
                    AND (CHARINDEX(''*'', FullTableName) = 0 OR FullTableName LIKE ''%.*'');

                INSERT INTO @TargetTables (SchemaName, TableName, HasPrimaryKey)
                SELECT
                    s.name, t.name,
                    CASE WHEN pk.CONSTRAINT_NAME IS NOT NULL THEN 1 ELSE 0 END
                FROM sys.tables t
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                INNER JOIN @TableList tl ON
                    (tl.FullTableName = s.name COLLATE DATABASE_DEFAULT + ''.*'' OR
                     tl.FullTableName = s.name COLLATE DATABASE_DEFAULT + ''.'' + t.name COLLATE DATABASE_DEFAULT OR
                     (CHARINDEX(''.'', tl.FullTableName) = 0 AND tl.FullTableName = t.name COLLATE DATABASE_DEFAULT AND s.name COLLATE DATABASE_DEFAULT = ''dbo''))
                LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS pk ON
                    pk.TABLE_SCHEMA COLLATE DATABASE_DEFAULT = s.name COLLATE DATABASE_DEFAULT AND pk.TABLE_NAME COLLATE DATABASE_DEFAULT = t.name COLLATE DATABASE_DEFAULT AND pk.CONSTRAINT_TYPE COLLATE DATABASE_DEFAULT = ''PRIMARY KEY''
                WHERE t.type = ''U'';
            END

            -- Check for tables without primary keys
            SELECT @SkippedTables = COALESCE(@SkippedTables + '','', '''') + QUOTENAME(SchemaName) + ''.'' + QUOTENAME(TableName)
            FROM @TargetTables
            WHERE HasPrimaryKey = 0;

            SELECT @SkippedTablesCount = COUNT(*)
            FROM @TargetTables
            WHERE HasPrimaryKey = 0;

            IF @SkippedTablesCount > 0
            BEGIN
                DECLARE @SkippedTableWord NVARCHAR(10) = CASE WHEN @SkippedTablesCount = 1 THEN ''table'' ELSE ''tables'' END;
                PRINT N''⚠ WARNING: Skipping '' + CAST(@SkippedTablesCount AS NVARCHAR(10)) + '' '' + @SkippedTableWord + '' without primary keys:'';
                PRINT N''   '' + @SkippedTables;
                PRINT N''   Consider using lakeflowSetupChangeDataCapture for these tables.'';

                DELETE FROM @TargetTables WHERE HasPrimaryKey = 0;
            END

            -- Process each table for change tracking enablement
            DECLARE table_cursor CURSOR FOR
                SELECT SchemaName, TableName FROM @TargetTables ORDER BY SchemaName, TableName;

            OPEN table_cursor;
            FETCH NEXT FROM table_cursor INTO @CurrentSchema, @CurrentTable;

            WHILE @@FETCH_STATUS = 0
            BEGIN
                BEGIN TRY
                    IF NOT EXISTS (
                        SELECT 1 FROM sys.change_tracking_tables ct
                        INNER JOIN sys.tables t ON ct.object_id = t.object_id
                        WHERE t.schema_id = SCHEMA_ID(@CurrentSchema) AND t.name = CAST(@CurrentTable AS sysname)
                    )
                    BEGIN
                        SET @SQL = N''ALTER TABLE '' + QUOTENAME(@CurrentSchema) + ''.'' + QUOTENAME(@CurrentTable) + '' ENABLE CHANGE_TRACKING'';
                        EXEC sp_executesql @SQL;
                        PRINT N''✓ Enabled change tracking on ['' + @CurrentSchema + ''].['' + @CurrentTable + '']'';
                        SET @ProcessedCount = @ProcessedCount + 1;
                    END
                    ELSE
                    BEGIN
                        PRINT N''ℹ Change tracking already enabled on ['' + @CurrentSchema + ''].['' + @CurrentTable + '']'';
                        SET @SkippedCount = @SkippedCount + 1;
                    END
                END TRY
                BEGIN CATCH
                    PRINT N''✗ Error enabling change tracking on ['' + @CurrentSchema + ''].['' + @CurrentTable + '']: '' + ERROR_MESSAGE();
                    SET @ErrorCount = @ErrorCount + 1;
                END CATCH

                FETCH NEXT FROM table_cursor INTO @CurrentSchema, @CurrentTable;
            END

            CLOSE table_cursor;
            DEALLOCATE table_cursor;

            -- Grant VIEW CHANGE TRACKING permissions to user (if @User is specified)
            IF @DatabaseUser IS NOT NULL
            BEGIN
                PRINT N'''';
                PRINT N''=== Granting VIEW CHANGE TRACKING Permissions ==='';

                DECLARE @PermissionGrantCount INT = 0, @PermissionErrorCount INT = 0;

                -- Strategy based on @Tables parameter
                IF @Tables = ''ALL''
                BEGIN
                    -- Grant on all user tables with change tracking enabled
                    PRINT N''Granting VIEW CHANGE TRACKING on all change tracking enabled tables...'';

                    DECLARE @CTSchema NVARCHAR(128), @CTTable NVARCHAR(128);
                    DECLARE ct_cursor CURSOR FOR
                        SELECT s.name, t.name
                        FROM sys.change_tracking_tables ct
                        INNER JOIN sys.tables t ON ct.object_id = t.object_id
                        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                        WHERE s.name COLLATE DATABASE_DEFAULT NOT IN (''sys'', ''information_schema'', ''cdc'', ''INFORMATION_SCHEMA'', ''guest'')
                        ORDER BY s.name, t.name;

                    OPEN ct_cursor;
                    FETCH NEXT FROM ct_cursor INTO @CTSchema, @CTTable;

                    WHILE @@FETCH_STATUS = 0
                    BEGIN
                        BEGIN TRY
                            SET @SQL = N''GRANT VIEW CHANGE TRACKING ON ['' + @CTSchema + ''].['' + @CTTable + ''] TO '' + QUOTENAME(@DatabaseUser);
                            EXEC sp_executesql @SQL;
                            PRINT N''  ✓ Granted VIEW CHANGE TRACKING on ['' + @CTSchema + ''].['' + @CTTable + '']'';
                            SET @PermissionGrantCount = @PermissionGrantCount + 1;
                        END TRY
                        BEGIN CATCH
                            PRINT N''  ⚠ Could not grant VIEW CHANGE TRACKING on ['' + @CTSchema + ''].['' + @CTTable + '']: '' + ERROR_MESSAGE();
                            SET @PermissionErrorCount = @PermissionErrorCount + 1;
                        END CATCH

                        FETCH NEXT FROM ct_cursor INTO @CTSchema, @CTTable;
                    END

                    CLOSE ct_cursor;
                    DEALLOCATE ct_cursor;
                END
                ELSE IF @Tables LIKE ''SCHEMAS:%''
                BEGIN
                    -- Grant on schema level for specified schemas
                    DECLARE @SchemaListForPerms NVARCHAR(MAX) = SUBSTRING(@Tables, 9, LEN(@Tables));
                    -- Accept bracket-quoted schema names such as [dbo]; the catalog is matched unbracketed.
                    SET @SchemaListForPerms = REPLACE(REPLACE(@SchemaListForPerms, ''['', ''''), '']'', '''');
                    PRINT N''Granting VIEW CHANGE TRACKING on schemas: '' + @SchemaListForPerms;

                    -- Parse schema list and grant on each schema''''s CT-enabled tables
                    DECLARE @Schema NVARCHAR(128);
                    DECLARE @TempSchemasForPerms NVARCHAR(MAX) = @SchemaListForPerms;

                    WHILE LEN(@TempSchemasForPerms) > 0
                    BEGIN
                        DECLARE @SchemaPosPerm INT = CHARINDEX('','', @TempSchemasForPerms);
                        IF @SchemaPosPerm = 0
                        BEGIN
                            SET @Schema = LTRIM(RTRIM(@TempSchemasForPerms));
                            SET @TempSchemasForPerms = N'''';
                        END
                        ELSE
                        BEGIN
                            SET @Schema = LTRIM(RTRIM(LEFT(@TempSchemasForPerms, @SchemaPosPerm - 1)));
                            SET @TempSchemasForPerms = SUBSTRING(@TempSchemasForPerms, @SchemaPosPerm + 1, LEN(@TempSchemasForPerms));
                        END

                        IF LEN(@Schema) > 0
                        BEGIN
                            -- Grant on all CT-enabled tables in this schema
                            DECLARE schema_ct_cursor CURSOR FOR
                                SELECT t.name
                                FROM sys.change_tracking_tables ct
                                INNER JOIN sys.tables t ON ct.object_id = t.object_id
                                WHERE t.schema_id = SCHEMA_ID(@Schema)
                                ORDER BY t.name;

                            OPEN schema_ct_cursor;
                            FETCH NEXT FROM schema_ct_cursor INTO @CTTable;

                            WHILE @@FETCH_STATUS = 0
                            BEGIN
                                BEGIN TRY
                                    SET @SQL = N''GRANT VIEW CHANGE TRACKING ON ['' + @Schema + ''].['' + @CTTable + ''] TO '' + QUOTENAME(@DatabaseUser);
                                    EXEC sp_executesql @SQL;
                                    PRINT N''  ✓ Granted VIEW CHANGE TRACKING on ['' + @Schema + ''].['' + @CTTable + '']'';
                                    SET @PermissionGrantCount = @PermissionGrantCount + 1;
                                END TRY
                                BEGIN CATCH
                                    PRINT N''  ⚠ Could not grant VIEW CHANGE TRACKING on ['' + @Schema + ''].['' + @CTTable + '']: '' + ERROR_MESSAGE();
                                    SET @PermissionErrorCount = @PermissionErrorCount + 1;
                                END CATCH

                                FETCH NEXT FROM schema_ct_cursor INTO @CTTable;
                            END

                            CLOSE schema_ct_cursor;
                            DEALLOCATE schema_ct_cursor;
                        END
                    END
                END
                ELSE
                BEGIN
                    -- Grant on specific tables listed in @Tables
                    PRINT N''Granting VIEW CHANGE TRACKING on specified tables...'';

                    -- Use the same @TargetTables that were processed for CT enablement
                    DECLARE specific_ct_cursor CURSOR FOR
                        SELECT SchemaName, TableName FROM @TargetTables
                        WHERE EXISTS (
                            SELECT 1 FROM sys.change_tracking_tables ct
                            INNER JOIN sys.tables t ON ct.object_id = t.object_id
                            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                            WHERE s.name COLLATE DATABASE_DEFAULT = SchemaName AND t.name COLLATE DATABASE_DEFAULT = TableName
                        )
                        ORDER BY SchemaName, TableName;

                    OPEN specific_ct_cursor;
                    FETCH NEXT FROM specific_ct_cursor INTO @CTSchema, @CTTable;

                    WHILE @@FETCH_STATUS = 0
                    BEGIN
                        BEGIN TRY
                            SET @SQL = N''GRANT VIEW CHANGE TRACKING ON ['' + @CTSchema + ''].['' + @CTTable + ''] TO '' + QUOTENAME(@DatabaseUser);
                            EXEC sp_executesql @SQL;
                            PRINT N''  ✓ Granted VIEW CHANGE TRACKING on ['' + @CTSchema + ''].['' + @CTTable + '']'';
                            SET @PermissionGrantCount = @PermissionGrantCount + 1;
                        END TRY
                        BEGIN CATCH
                            PRINT N''  ⚠ Could not grant VIEW CHANGE TRACKING on ['' + @CTSchema + ''].['' + @CTTable + '']: '' + ERROR_MESSAGE();
                            SET @PermissionErrorCount = @PermissionErrorCount + 1;
                        END CATCH

                        FETCH NEXT FROM specific_ct_cursor INTO @CTSchema, @CTTable;
                    END

                    CLOSE specific_ct_cursor;
                    DEALLOCATE specific_ct_cursor;
                END

                -- Permission grant summary report
                PRINT N'''';
                PRINT N''VIEW CHANGE TRACKING permission summary:'';
                PRINT N''  - Tables granted: '' + CAST(@PermissionGrantCount AS NVARCHAR(10));
                PRINT N''  - Tables with permission errors: '' + CAST(@PermissionErrorCount AS NVARCHAR(10));

                IF @PermissionGrantCount > 0
                    PRINT N''✓ VIEW CHANGE TRACKING permissions granted to user: '' + @DatabaseUser;
            END

            -- Final summary report
            PRINT N'''';
            PRINT N''CT setup summary:'';
            PRINT N''  - Tables processed: '' + CAST(@ProcessedCount AS NVARCHAR(10));
            PRINT N''  - Tables already enabled: '' + CAST(@SkippedCount AS NVARCHAR(10));
            PRINT N''  - Tables with processing errors: '' + CAST(@ErrorCount AS NVARCHAR(10));
            PRINT N''  - Tables skipped (no PK): '' + CAST(@SkippedTablesCount AS NVARCHAR(10));
        END

        -- The audit trigger must exist on exit; a mid-cutover failure auto-commits with none in place,
        -- silently dropping DDL. Fail loudly with remediation instead.
        IF @CreateDdlSupportingObjects = 1
           AND NOT EXISTS (SELECT 1 FROM sys.triggers
                           WHERE name = CAST(@ddlAuditTriggerName AS sysname) AND parent_class = 0)
            THROW 51000, N''DDL audit trigger is missing after setup. Please re-run lakeflowSetupChangeTracking. Schema changes (ALTER TABLE) made since a failed run may have been missed, and those tables may need a full refresh.'', 1;

        PRINT N''Change tracking setup completed successfully'';

    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ''Error in lakeflowSetupChangeTracking: '' + ERROR_MESSAGE();
        PRINT @ErrorMessage;
        THROW;
    END CATCH
END';
PRINT N'Created lakeflowSetupChangeTracking procedure';

-- Create lakeflowSetupChangeDataCapture
PRINT N'Creating lakeflowSetupChangeDataCapture procedure...';
EXEC sp_executesql N'
CREATE PROCEDURE dbo.lakeflowSetupChangeDataCapture
    @Tables NVARCHAR(MAX) = NULL,
    @User NVARCHAR(128) = NULL,
    @Mode NVARCHAR(10) = ''INSTALL'',
    @AllowDisablePreExistingCaptureInstances BIT = 0
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @DatabaseUser NVARCHAR(128) = @User;
    DECLARE @Platform NVARCHAR(50) = dbo.lakeflowDetectPlatform();
    DECLARE @CatalogName NVARCHAR(128) = DB_NAME();
    DECLARE @ErrorMessage NVARCHAR(4000);
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @versionSuffix NVARCHAR(10) = ''_1_7'';
    DECLARE @captureInstanceTableName NVARCHAR(100) = ''lakeflowCaptureInstanceInfo'' + @versionSuffix;
    DECLARE @alterTableTriggerName NVARCHAR(100) = ''lakeflowAlterTableTrigger'' + @versionSuffix;
    DECLARE @disableOldCaptureInstanceProcName NVARCHAR(100) = ''lakeflowDisableOldCaptureInstance'' + @versionSuffix;
    DECLARE @mergeCaptureInstancesProcName NVARCHAR(100) = ''lakeflowMergeCaptureInstances'' + @versionSuffix;
    DECLARE @refreshCaptureInstanceProcName NVARCHAR(100) = ''lakeflowRefreshCaptureInstance'' + @versionSuffix;

    -- Error codes and messages
    DECLARE @invalidModeErrorCode INT = 100000;
    DECLARE @invalidModeErrorMessage NVARCHAR(200);
    DECLARE @insufficientUserPrivilegesCode INT = 100400;
    DECLARE @insufficientUserPrivilegesErrorMessage NVARCHAR(200);

    SET @invalidModeErrorMessage = CONCAT(''Provided execution mode: '', @Mode, '', is not recognized. Allowed values are: INSTALL, CLEANUP'');
    SET @insufficientUserPrivilegesErrorMessage = ''User executing this script is not a ''''db_owner'''' role member. To execute this script, please use a user that is.'';

    PRINT N''Starting Change Data Capture setup for: '' + @CatalogName;
    PRINT N''Platform: '' + @Platform;
    PRINT N''Mode: '' + @Mode;
    IF @Tables IS NOT NULL
        PRINT N''Tables: '' + @Tables;

    BEGIN TRY
        -- Validate execution mode
        IF (@Mode != ''INSTALL'' AND @Mode != ''CLEANUP'')
        BEGIN
            THROW @invalidModeErrorCode, @invalidModeErrorMessage, 1;
        END

        -- Validate that current user is db_owner
        IF (IS_ROLEMEMBER(''db_owner'') = 0)
        BEGIN
            THROW @insufficientUserPrivilegesCode, @insufficientUserPrivilegesErrorMessage, 1;
        END

        -- Cleanup legacy DDL support objects
        IF EXISTS (SELECT 1 FROM sys.triggers WHERE name = ''replicate_io_audit_ddl_trigger_1'' AND parent_class = 0)
            OR OBJECT_ID(''dbo.replicate_io_audit_ddl_1'', ''U'') IS NOT NULL
            OR OBJECT_ID(''dbo.replicate_io_audit_tbl_cons_1'', ''U'') IS NOT NULL
            OR OBJECT_ID(''dbo.replicate_io_audit_tbl_schema_1'', ''U'') IS NOT NULL
            OR EXISTS (SELECT 1 FROM sys.triggers WHERE name = ''alterTableTrigger_1'' AND parent_class = 0)
            OR OBJECT_ID(''dbo.disableOldCaptureInstance_1'', ''P'') IS NOT NULL
            OR OBJECT_ID(''dbo.refreshCaptureInstance_1'', ''P'') IS NOT NULL
            OR OBJECT_ID(''dbo.mergeCaptureInstance_1'', ''P'') IS NOT NULL
            OR OBJECT_ID(''dbo.captureInstanceTracker_1'', ''U'') IS NOT NULL
        BEGIN
            PRINT N''Cleaning up legacy DDL support objects...'';

            IF EXISTS (SELECT 1 FROM sys.triggers WHERE name = ''replicate_io_audit_ddl_trigger_1'' AND parent_class = 0)
            BEGIN
                EXEC(''DROP TRIGGER replicate_io_audit_ddl_trigger_1 ON DATABASE'');
                PRINT N''✓ Dropped legacy trigger: replicate_io_audit_ddl_trigger_1'';
            END

            IF OBJECT_ID(''dbo.replicate_io_audit_ddl_1'', ''U'') IS NOT NULL
            BEGIN
                EXEC(''DROP TABLE dbo.replicate_io_audit_ddl_1'');
                PRINT N''✓ Dropped legacy table: replicate_io_audit_ddl_1'';
            END

            IF OBJECT_ID(''dbo.replicate_io_audit_tbl_cons_1'', ''U'') IS NOT NULL
            BEGIN
                EXEC(''DROP TABLE dbo.replicate_io_audit_tbl_cons_1'');
                PRINT N''✓ Dropped legacy table: replicate_io_audit_tbl_cons_1'';
            END

            IF OBJECT_ID(''dbo.replicate_io_audit_tbl_schema_1'', ''U'') IS NOT NULL
            BEGIN
                EXEC(''DROP TABLE dbo.replicate_io_audit_tbl_schema_1'');
                PRINT N''✓ Dropped legacy table: replicate_io_audit_tbl_schema_1'';
            END

            IF EXISTS (SELECT name FROM sys.triggers WHERE name = ''alterTableTrigger_1'' AND type = ''TR'')
            BEGIN
                EXEC(''DROP TRIGGER alterTableTrigger_1 ON DATABASE'');
                PRINT N''✓ Dropped legacy trigger: alterTableTrigger_1'';
            END

            IF OBJECT_ID(''dbo.disableOldCaptureInstance_1'', ''P'') IS NOT NULL
            BEGIN
                EXEC(''DROP PROCEDURE dbo.disableOldCaptureInstance_1'');
                PRINT N''✓ Dropped legacy procedure: disableOldCaptureInstance_1'';
            END

            IF OBJECT_ID(''dbo.refreshCaptureInstance_1'', ''P'') IS NOT NULL
            BEGIN
                EXEC(''DROP PROCEDURE dbo.refreshCaptureInstance_1'');
                PRINT N''✓ Dropped legacy procedure: refreshCaptureInstance_1'';
            END

            IF OBJECT_ID(''dbo.mergeCaptureInstance_1'', ''P'') IS NOT NULL
            BEGIN
                EXEC(''DROP PROCEDURE dbo.mergeCaptureInstance_1'');
                PRINT N''✓ Dropped legacy procedure: mergeCaptureInstance_1'';
            END

            IF OBJECT_ID(''dbo.captureInstanceTracker_1'', ''U'') IS NOT NULL
            BEGIN
                EXEC(''DROP TABLE dbo.captureInstanceTracker_1'');
                PRINT N''✓ Dropped legacy table: captureInstanceTracker_1'';
            END

            PRINT N''Legacy DDL support objects cleanup completed'';
        END

        -- Cleanup mode: Remove DDL support objects
        IF @Mode = ''CLEANUP''
        BEGIN
            PRINT N''Cleaning up CDC DDL support objects...'';

            -- Drop ALTER TABLE trigger first to avoid orphaned triggers if cleanup fails mid-sweep
            IF EXISTS (SELECT 1 FROM sys.triggers WHERE name = CAST(@alterTableTriggerName AS sysname) AND parent_class = 0)
            BEGIN
                SET @SQL = N''DROP TRIGGER ['' + @alterTableTriggerName + ''] ON DATABASE'';
                EXEC sp_executesql @SQL;
                PRINT N''✓ Dropped ALTER TABLE trigger: '' + @alterTableTriggerName;
            END

            -- Drop procedures
            IF OBJECT_ID(''dbo.'' + @refreshCaptureInstanceProcName, ''P'') IS NOT NULL
            BEGIN
                SET @SQL = N''DROP PROCEDURE [dbo].['' + @refreshCaptureInstanceProcName + '']'';
                EXEC sp_executesql @SQL;
                PRINT N''✓ Dropped procedure: '' + @refreshCaptureInstanceProcName;
            END

            IF OBJECT_ID(''dbo.'' + @mergeCaptureInstancesProcName, ''P'') IS NOT NULL
            BEGIN
                SET @SQL = N''DROP PROCEDURE [dbo].['' + @mergeCaptureInstancesProcName + '']'';
                EXEC sp_executesql @SQL;
                PRINT N''✓ Dropped procedure: '' + @mergeCaptureInstancesProcName;
            END

            IF OBJECT_ID(''dbo.'' + @disableOldCaptureInstanceProcName, ''P'') IS NOT NULL
            BEGIN
                SET @SQL = N''DROP PROCEDURE [dbo].['' + @disableOldCaptureInstanceProcName + '']'';
                EXEC sp_executesql @SQL;
                PRINT N''✓ Dropped procedure: '' + @disableOldCaptureInstanceProcName;
            END

            -- Drop capture instance table
            IF OBJECT_ID(''dbo.'' + @captureInstanceTableName, ''U'') IS NOT NULL
            BEGIN
                SET @SQL = N''DROP TABLE [dbo].['' + @captureInstanceTableName + '']'';
                EXEC sp_executesql @SQL;
                PRINT N''✓ Dropped capture instance table: '' + @captureInstanceTableName;
            END

            -- Pattern-based cleanup for any remaining CDC objects across versions.
            DECLARE @cdcCleanupSql NVARCHAR(MAX) = '''';

            SELECT @cdcCleanupSql = @cdcCleanupSql + ''DROP TRIGGER ['' + name + ''] ON DATABASE;'' + CHAR(13)
            FROM sys.triggers
            WHERE ((name LIKE ''lakeflowAlterTableTrigger_%_%'' AND name != CAST(@alterTableTriggerName AS sysname))
                OR name LIKE ''replicantAlterTableTrigger_%_%'') AND parent_class = 0;

            IF LEN(@cdcCleanupSql) > 0
            BEGIN
                EXEC sp_executesql @cdcCleanupSql;
                PRINT N''✓ Cleaned up remaining ALTER TABLE triggers across versions'';
            END

            -- Clean up any remaining capture instance tables across versions
            SET @cdcCleanupSql = '''';
            SELECT @cdcCleanupSql = @cdcCleanupSql + ''DROP TABLE [dbo].['' + name + ''];'' + CHAR(13)
            FROM sys.tables
            WHERE (name LIKE ''lakeflowCaptureInstanceInfo_%_%'' AND name != CAST(@captureInstanceTableName AS sysname))
               OR name LIKE ''replicantCaptureInstanceInfo_%_%'';

            IF LEN(@cdcCleanupSql) > 0
            BEGIN
                EXEC sp_executesql @cdcCleanupSql;
                PRINT N''✓ Cleaned up remaining capture instance tables across versions'';
            END

            -- Clean up any remaining CDC procedures across versions
            SET @cdcCleanupSql = '''';
            SELECT @cdcCleanupSql = @cdcCleanupSql + ''DROP PROCEDURE [dbo].['' + name + ''];'' + CHAR(13)
            FROM sys.procedures
            WHERE (name LIKE ''lakeflowDisableOldCaptureInstance_%_%'' AND name != CAST(@disableOldCaptureInstanceProcName AS sysname))
               OR (name LIKE ''lakeflowMergeCaptureInstances_%_%'' AND name != CAST(@mergeCaptureInstancesProcName AS sysname))
               OR (name LIKE ''lakeflowRefreshCaptureInstance_%_%'' AND name != CAST(@refreshCaptureInstanceProcName AS sysname))
               OR name LIKE ''replicantDisableOldCaptureInstance_%_%''
               OR name LIKE ''replicantMergeCaptureInstances_%_%''
               OR name LIKE ''replicantRefreshCaptureInstance_%_%'';

            IF LEN(@cdcCleanupSql) > 0
            BEGIN
                EXEC sp_executesql @cdcCleanupSql;
                PRINT N''✓ Cleaned up remaining CDC procedures across versions'';
            END

            PRINT N''CDC DDL support objects cleanup completed'';
            RETURN;
        END

        -- Install mode: Create/upgrade DDL support objects
        PRINT N''Installing/upgrading CDC DDL support objects...'';

        -- Enable CDC at database level if not already enabled
        IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name COLLATE DATABASE_DEFAULT = DB_NAME() AND is_cdc_enabled = 1)
        BEGIN
            PRINT N''Enabling Change Data Capture at database level...'';

            -- Use platform-specific CDC enablement procedure
            IF @Platform = ''AMAZON_RDS''
            BEGIN
                DECLARE @currentDbName NVARCHAR(128) = DB_NAME();
                DECLARE @rdsSql NVARCHAR(MAX);
                DECLARE @rdsCheckSql NVARCHAR(MAX);
                DECLARE @rdsExists INT;

                SET @rdsCheckSql = N''SELECT @rdsExists = CASE WHEN OBJECT_ID(''''msdb.dbo.rds_cdc_enable_db'''', ''''P'''') IS NOT NULL THEN 1 ELSE 0 END'';
                EXEC sp_executesql @rdsCheckSql, N''@rdsExists INT OUTPUT'', @rdsExists OUTPUT;

                IF @rdsExists = 0
                BEGIN
                    RAISERROR(''Platform detected as Amazon RDS but required procedure msdb.dbo.rds_cdc_enable_db does not exist'', 16, 1);
                    ROLLBACK TRAN;
                    RETURN;
                END

                SET @rdsSql = N''EXEC msdb.dbo.rds_cdc_enable_db @db_name = @dbName'';
                EXEC sp_executesql @rdsSql, N''@dbName NVARCHAR(128)'', @dbName = @currentDbName;
            END
            ELSE
            BEGIN
                EXEC sys.sp_cdc_enable_db;
            END

            PRINT N''✓ Change Data Capture enabled at database level'';
        END
        ELSE
        BEGIN
            PRINT N''ℹ Change Data Capture already enabled at database level'';
        END

        -- Current version, parsed numerically; bounds the table carry-forward below to strictly-older, so a
        -- newer table (a downgrade) is never renamed onto the current name and downgraded.
        DECLARE @ciCurMajor INT = dbo.lakeflowVersionComponent(@captureInstanceTableName, 2);
        DECLARE @ciCurMinor INT = dbo.lakeflowVersionComponent(@captureInstanceTableName, 1);
        -- The versioned PK constraint name, built once so the carry-forward rename target and the fresh
        -- CREATE below cannot drift apart.
        DECLARE @ciPkName SYSNAME = ''replicantCaptureInstanceInfoPrimaryKey_'' + @versionSuffix;

        -- Select a compatible prior-version capture-instance table to carry forward, before the cutover
        -- transaction so the transaction holds only the swap.
        -- sp_rename keeps its recorded state (the 1.6 columns are added below). Compatible = the six pre-1.6
        -- columns present with their exact type/length/nullability (extras are allowed; the 1.6 columns are
        -- added by the heal). Validating types, not just names, stops a same-named but wrong-typed table
        -- from being carried forward. If more than one exists, keep the highest version and drop the rest.
        -- Capture-instance key char widths, declared once so the gate below and the CREATE TABLE stay in
        -- sync (schemaName/tableName are varchar, so sys.columns max_length is bytes = chars).
        DECLARE @wCiSchema INT = 100;
        DECLARE @wCiTable INT = 255;
        DECLARE @ciPrior SYSNAME;
        SELECT TOP 1 @ciPrior = t.name
        FROM sys.tables t
        WHERE t.name LIKE ''lakeflowCaptureInstanceInfo_%_%''
          AND t.name <> CAST(@captureInstanceTableName AS sysname)
          AND (SELECT COUNT(*) FROM sys.columns c JOIN sys.types ty ON c.user_type_id = ty.user_type_id
               WHERE c.object_id = t.object_id
                 AND ((c.name = ''oldCaptureInstance'' AND ty.name = ''varchar'' AND c.max_length = -1 AND c.is_nullable = 1)
                   OR (c.name = ''newCaptureInstance'' AND ty.name = ''varchar'' AND c.max_length = -1 AND c.is_nullable = 1)
                   OR (c.name = ''schemaName'' AND ty.name = ''varchar'' AND c.max_length = @wCiSchema AND c.is_nullable = 0)
                   OR (c.name = ''tableName'' AND ty.name = ''varchar'' AND c.max_length = @wCiTable AND c.is_nullable = 0)
                   OR (c.name = ''committedCursor'' AND ty.name = ''varchar'' AND c.max_length = -1 AND c.is_nullable = 1)
                   OR (c.name = ''triggerReinit'' AND ty.name = ''bit'' AND c.is_nullable = 1))) = 6
          -- Only carry forward an older version, never one newer than this script (a downgrade).
          AND (dbo.lakeflowVersionComponent(t.name, 2) < @ciCurMajor
               OR (dbo.lakeflowVersionComponent(t.name, 2) = @ciCurMajor
                   AND dbo.lakeflowVersionComponent(t.name, 1) < @ciCurMinor))
        ORDER BY dbo.lakeflowVersionComponent(t.name, 2) DESC,
                 dbo.lakeflowVersionComponent(t.name, 1) DESC;

        -- Announce an incompatible prior (its tables reinitialize on a fresh table) before the cutover.
        IF @ciPrior IS NULL AND OBJECT_ID(''dbo.'' + @captureInstanceTableName, ''U'') IS NULL
              AND EXISTS (SELECT 1 FROM sys.tables
                          WHERE name LIKE ''lakeflowCaptureInstanceInfo_%_%''
                            AND name <> CAST(@captureInstanceTableName AS sysname))
        BEGIN
            -- A newer-version prior is left in place (version bound above); report it as a downgrade,
            -- not a contract mismatch, and name the table.
            DECLARE @newerCi SYSNAME = (
                SELECT TOP 1 name FROM sys.tables
                WHERE name LIKE ''lakeflowCaptureInstanceInfo_%_%'' AND name <> CAST(@captureInstanceTableName AS sysname)
                  AND (dbo.lakeflowVersionComponent(name, 2) > @ciCurMajor
                       OR (dbo.lakeflowVersionComponent(name, 2) = @ciCurMajor
                           AND dbo.lakeflowVersionComponent(name, 1) > @ciCurMinor))
                ORDER BY dbo.lakeflowVersionComponent(name, 2) DESC, dbo.lakeflowVersionComponent(name, 1) DESC);
            DECLARE @otherCi SYSNAME = (
                SELECT TOP 1 name FROM sys.tables
                WHERE name LIKE ''lakeflowCaptureInstanceInfo_%_%'' AND name <> CAST(@captureInstanceTableName AS sysname)
                ORDER BY name);
            IF @newerCi IS NOT NULL
                PRINT N''ℹ Newer capture instance table '' + @newerCi + '' found; its rows are preserved, but this older version is now active for capture. You may be running an older setup script.'';
            ELSE
                PRINT N''⚠ Existing capture instance table '' + ISNULL(@otherCi, N''(unknown)'') + '' does not match this version; creating a fresh one (affected tables will be reinitialized).'';
        END

        -- The non-current lifecycle procs are dropped inside the cutover transaction below, after the old
        -- alter-table trigger is dropped -- never before it, or the still-armed old trigger would EXEC a
        -- proc that no longer exists (a concurrent ADD COLUMN would then fail).

        -- If a current-named capture-instance table already exists (from an interrupted run), heal its 1.6
        -- columns now, before creating the lifecycle procs below. Deferred name resolution lets a proc be
        -- created before its table exists, but a proc that binds against an existing table with a missing
        -- column fails to create; the carry-forward/fresh paths create the table inside the cutover
        -- transaction, so at proc-create time it is either absent (deferred) or complete. CHAR(39) is a quote.
        IF OBJECT_ID(''dbo.'' + @captureInstanceTableName, ''U'') IS NOT NULL
        BEGIN
            DECLARE @ciAlterPre NVARCHAR(MAX);
            IF COL_LENGTH(''dbo.'' + @captureInstanceTableName, ''ddlStatement'') IS NULL
            BEGIN
                SET @ciAlterPre = ''ALTER TABLE [dbo].['' + @captureInstanceTableName + ''] ADD ddlStatement NVARCHAR(MAX) NOT NULL DEFAULT '' + CHAR(39) + CHAR(39);
                EXEC sp_executesql @ciAlterPre;
            END
            IF COL_LENGTH(''dbo.'' + @captureInstanceTableName, ''pendingAddColumn'') IS NULL
            BEGIN
                SET @ciAlterPre = ''ALTER TABLE [dbo].['' + @captureInstanceTableName + ''] ADD pendingAddColumn BIT NOT NULL DEFAULT 0'';
                EXEC sp_executesql @ciAlterPre;
            END
        END

        -- Create lakeflowDisableOldCaptureInstance procedure.
        BEGIN
            -- CREATE OR ALTER (not drop then create) so this procedure is never momentarily absent
            -- while the current alter-table trigger, which calls it, stays armed across setup re-runs.
            SET @SQL = CAST(N''CREATE OR ALTER PROCEDURE [dbo].['' AS NVARCHAR(MAX)) + @disableOldCaptureInstanceProcName + '']
                @schemaName VARCHAR(MAX), @tableName VARCHAR(MAX)
            WITH EXECUTE AS OWNER
            AS
            SET NOCOUNT ON

            DECLARE @oldCaptureInstance NVARCHAR(MAX);

            BEGIN TRAN
                SET @oldCaptureInstance = (SELECT oldCaptureInstance FROM dbo.['' + @captureInstanceTableName + ''] WHERE schemaName=@schemaName AND tableName=@tableName);

                -- Only disable capture instances that we own (lakeflow naming or old New_ naming),
                -- unless AllowDisablePreExistingCaptureInstances was set when installing DDL support objects
                IF @oldCaptureInstance IS NOT NULL AND (@oldCaptureInstance LIKE ''''lakeflow[_]%[_][1-2]'''' OR @oldCaptureInstance LIKE ''''New[_]%[_]%'''' OR '' + CAST(@AllowDisablePreExistingCaptureInstances AS NVARCHAR(1)) + N'' = 1)
                BEGIN
                    EXEC sys.sp_cdc_disable_table
                        @source_schema = @schemaName,
                        @source_name = @tableName,
                        @capture_instance = @oldCaptureInstance;
                END
                IF @oldCaptureInstance IS NOT NULL
                    UPDATE dbo.['' + @captureInstanceTableName + ''] SET oldCaptureInstance=NULL WHERE schemaName=@schemaName AND tableName=@tableName;
            COMMIT TRAN'';
            EXEC sp_executesql @SQL;
            PRINT N''✓ Created procedure: '' + @disableOldCaptureInstanceProcName;
        END

        -- Create lakeflowMergeCaptureInstances procedure.
        BEGIN
            -- CREATE OR ALTER (not drop then create) so this procedure is never momentarily absent
            -- while the current alter-table trigger, which calls it, stays armed across setup re-runs.
            SET @SQL = CAST(N''CREATE OR ALTER PROCEDURE [dbo].['' AS NVARCHAR(MAX)) + @mergeCaptureInstancesProcName + '']
                @schemaName VARCHAR(MAX), @tableName VARCHAR(MAX)
            WITH EXECUTE AS OWNER
            AS
            SET NOCOUNT ON
            BEGIN TRAN
                DECLARE @newCaptureInstanceFullPath NVARCHAR(MAX),
                    @oldCaptureInstanceFullPath NVARCHAR(MAX),
                    @columnList NVARCHAR(MAX),
                    @columnListValues NVARCHAR(MAX),
                    @oldCaptureInstanceName NVARCHAR(MAX),
                    @newCaptureInstanceName NVARCHAR(MAX),
                    @captureInstanceCount INT,
                    @minLSN VARCHAR(MAX),
                    @quotedFullTableName nvarchar(max),
                    @mergeSQL NVARCHAR(MAX);

                SET @quotedFullTableName = QUOTENAME(@schemaName) + ''''.'''' + QUOTENAME(@tableName);
                SET @captureInstanceCount = (SELECT COUNT(*) FROM cdc.change_tables WHERE source_object_id = OBJECT_ID(@quotedFullTableName));
                IF (@captureInstanceCount = 2)
                BEGIN
                    SET @oldCaptureInstanceName = (SELECT oldCaptureInstance
                                           FROM dbo.['' + @captureInstanceTableName + '']
                                           WHERE schemaName = @schemaName and tableName = @tableName) + ''''_CT'''';
                    SET @newCaptureInstanceName = (SELECT newCaptureInstance
                                           FROM dbo.['' + @captureInstanceTableName + '']
                                           WHERE schemaName = @schemaName and tableName = @tableName) + ''''_CT'''';
                    SET @newCaptureInstanceFullPath = ''''[cdc].'''' + QUOTENAME(@newCaptureInstanceName);
	                SET @oldCaptureInstanceFullPath = ''''[cdc].'''' + QUOTENAME(@oldCaptureInstanceName);
                    SET @minLSN = (SELECT committedCursor FROM dbo.['' + @captureInstanceTableName + ''] WHERE schemaName=@schemaName and tableName=@tableName);

                    IF @minLSN is NULL OR @minLSN = ''''''''
                    BEGIN
                        SET @minLSN = ''''0x00000000000000000000''''
                    END

						SET @columnList = (SELECT STUFF((SELECT '''','''' + QUOTENAME(A.COLUMN_NAME)
												   FROM INFORMATION_SCHEMA.COLUMNS A
													   JOIN INFORMATION_SCHEMA.COLUMNS B ON
														   A.COLUMN_NAME=B.COLUMN_NAME AND
														   A.DATA_TYPE=B.DATA_TYPE
													   WHERE A.TABLE_NAME COLLATE DATABASE_DEFAULT=@newCaptureInstanceName AND
														   A.TABLE_SCHEMA COLLATE DATABASE_DEFAULT=''''cdc'''' AND
														   B.TABLE_NAME COLLATE DATABASE_DEFAULT=@oldCaptureInstanceName AND
														   B.TABLE_SCHEMA COLLATE DATABASE_DEFAULT=''''cdc'''' FOR XML PATH(''''''''), TYPE).value(''''.'''', ''''nvarchar(max)''''), 1, 1, ''''''''));

						SET @columnListValues = (SELECT STUFF((SELECT '''',source.'''' + QUOTENAME(A.COLUMN_NAME)
														 FROM INFORMATION_SCHEMA.COLUMNS A
															 JOIN INFORMATION_SCHEMA.COLUMNS B ON
																 A.COLUMN_NAME=B.COLUMN_NAME AND
																 A.DATA_TYPE=B.DATA_TYPE
														 WHERE
															 A.TABLE_NAME COLLATE DATABASE_DEFAULT=@newCaptureInstanceName AND
															 A.TABLE_SCHEMA COLLATE DATABASE_DEFAULT=''''cdc'''' AND
															 B.TABLE_NAME COLLATE DATABASE_DEFAULT=@oldCaptureInstanceName AND
															 B.TABLE_SCHEMA COLLATE DATABASE_DEFAULT=''''cdc'''' FOR XML PATH(''''''''), TYPE).value(''''.'''', ''''nvarchar(max)''''), 1, 1, ''''''''));

                    SET @mergeSQL = ''''MERGE '''' + @newCaptureInstanceFullPath + '''' AS target USING '''' + @oldCaptureInstanceFullPath + '''' AS source ON source.__$start_lsn = target.__$start_lsn AND source.__$seqval = target.__$seqval AND source.__$operation = target.__$operation WHEN NOT MATCHED AND source.__$start_lsn > '''' + @minLSN + '''' THEN INSERT ('''' + @columnList + '''') VALUES ('''' + @columnListValues + '''');'''';
                    EXEC sp_executesql @mergeSQL;
                END
            COMMIT TRAN'';
            EXEC sp_executesql @SQL;
            PRINT N''✓ Created procedure: '' + @mergeCaptureInstancesProcName;
        END

        -- Create lakeflowRefreshCaptureInstance procedure.
        BEGIN
            -- CREATE OR ALTER (not drop then create) so this procedure is never momentarily absent
            -- while the current alter-table trigger, which calls it, stays armed across setup re-runs.
            SET @SQL = CAST(N''CREATE OR ALTER PROCEDURE [dbo].['' AS NVARCHAR(MAX)) + @refreshCaptureInstanceProcName + '']
                @schemaName NVARCHAR(MAX),
                @tableName NVARCHAR(MAX),
                @reinit INT = 0
            WITH EXECUTE AS OWNER
            AS
            SET NOCOUNT ON
            -- Abort and roll back on any error so a failed recreate cannot commit a
            -- tracker row for an instance that was never created.
            SET XACT_ABORT ON

            BEGIN TRAN
                DECLARE @OldCaptureInstance NVARCHAR(MAX),
                    @NewCaptureInstance NVARCHAR(MAX),
                    @FileGroupName NVARCHAR(255),
                    @SupportNetChanges BIT,
                    @RoleName VARCHAR(255),
                    @CaptureInstanceCount INT,
                    @TriggerReinit INT,
                    @SkipCaptureInstanceCreation INT,
                    @LakeflowInstanceCount INT,
                    @OldestLakeflowInstanceForReinit NVARCHAR(MAX),
                    @BothLakeflowErrorMsg NVARCHAR(500),
                    @LakeflowInstanceToDrop NVARCHAR(MAX),
                    @CommittedCursor VARCHAR(MAX),
                    @OldInstanceToTrack nvarchar(max),
                    @QuotedFullName nvarchar(max),
                    @ExistingDdlStatement NVARCHAR(MAX),
                    @PendingAddColumn INT;

                SET @QuotedFullName = QUOTENAME(@schemaName) + ''''.'''' + QUOTENAME(@tableName);
                SET @SkipCaptureInstanceCreation = 0;
                SET @TriggerReinit = 0;
                SET @PendingAddColumn = 0;

                SET @CaptureInstanceCount = (SELECT COUNT(capture_instance) FROM cdc.change_tables WHERE source_object_id = object_id(@QuotedFullName));

                IF (@CaptureInstanceCount = 2)
                BEGIN
                    -- Genuine new DDL with 2 instances already: signal reinit
                    SET @TriggerReinit = 1;

                    -- Lakeflow-owned instance predicate (lakeflow_..._1/_2 or legacy New_), repeated
                    -- in several queries here; dynamic SQL blocks factoring out, so keep copies in sync.
                    -- Check if we have a lakeflow instance to drop
                    SET @LakeflowInstanceCount = (SELECT COUNT(capture_instance) FROM cdc.change_tables WHERE source_object_id = object_id(@QuotedFullName) AND (capture_instance LIKE ''''lakeflow[_]%[_][1-2]'''' OR capture_instance LIKE ''''New[_]%[_]%''''));

                    IF (@LakeflowInstanceCount = 2)
                    BEGIN
                        IF (@reinit = 1)
                        BEGIN
                            SET @TriggerReinit = 0;

                            -- During reinit, if we have 2 lakeflow instances, drop the oldest one
                            SET @OldestLakeflowInstanceForReinit = (
                                SELECT TOP 1 capture_instance
                                FROM cdc.change_tables
                                WHERE source_object_id = object_id(@QuotedFullName)
                                    AND (capture_instance LIKE ''''lakeflow[_]%[_][1-2]'''' OR capture_instance LIKE ''''New[_]%[_]%'''')
                                ORDER BY create_date ASC
                            );

                            IF @OldestLakeflowInstanceForReinit IS NOT NULL
                            BEGIN
                                PRINT ''''Reinit recovery: Dropping oldest lakeflow instance '''''''''''' + @OldestLakeflowInstanceForReinit + '''''''''''' to free up a slot for table '''' + @QuotedFullName;
                                EXEC sys.sp_cdc_disable_table
                                    @source_schema = @schemaName,
                                    @source_name = @tableName,
                                    @capture_instance = @OldestLakeflowInstanceForReinit;
                            END
                        END
                        ELSE
                        BEGIN
                            -- Both slots are occupied during an active schema change.
                            -- Queue this ADD COLUMN so the next rotation picks it up instead of triggering a full refresh.
                            SET @PendingAddColumn = 1;
                            SET @SkipCaptureInstanceCreation = 1;

                            PRINT ''''Both capture instance slots occupied by lakeflow instances. Queuing ADD COLUMN for table '''' + @QuotedFullName;
                        END
                    END
                    ELSE IF (@LakeflowInstanceCount = 1)
                    BEGIN
                        -- One lakeflow and one pre-existing instance fill both CDC slots, so a full
                        -- refresh is needed to apply this schema change.

                        -- Get the lakeflow instance (oldest one).
                        SET @LakeflowInstanceToDrop = (
                            SELECT TOP 1 capture_instance
                            FROM cdc.change_tables
                            WHERE source_object_id = object_id(@QuotedFullName)
                                AND (capture_instance LIKE ''''lakeflow[_]%[_][1-2]'''' OR capture_instance LIKE ''''New[_]%[_]%'''')
                            ORDER BY create_date ASC
                        );

                        IF (@reinit = 1)
                        BEGIN
                            -- On a full refresh, drop the lakeflow instance and let a complete one be
                            -- created below; skipping would strand the pipeline on the stale pre-existing
                            -- instance, which lacks any newly-added column.
                            SET @TriggerReinit = 0;
                            IF @LakeflowInstanceToDrop IS NOT NULL
                                EXEC sys.sp_cdc_disable_table
                                    @source_schema = @schemaName,
                                    @source_name = @tableName,
                                    @capture_instance = @LakeflowInstanceToDrop;
                        END
                        ELSE
                        BEGIN
                            -- Ingestion is still reading this lakeflow instance, so do not drop it here.
                            -- Signal a full refresh and record the instance so it is replaced safely then;
                            -- defer creating the new instance until that reinit.
                            SET @OldInstanceToTrack = @LakeflowInstanceToDrop;
                            SET @SkipCaptureInstanceCreation = 1;
                        END
                    END
                    ELSE
                    BEGIN
                        -- Both slots are taken by non-lakeflow instances
                        IF (@reinit = 1)
                        BEGIN
                            -- During reinit, we cannot proceed - raise clear error
                            DECLARE @BothNonLakeflowErrorMsg NVARCHAR(500);
                            SET @BothNonLakeflowErrorMsg = ''''Cannot create lakeflow capture instance for table '''' + @QuotedFullName +
                                '''': both CDC slots are occupied by non-lakeflow instances. '''' +
                                ''''Lakeflow requires at least one available slot. '''' +
                                ''''Please drop one of the existing capture instances and retry the pipeline.'''';
                            RAISERROR(@BothNonLakeflowErrorMsg, 16, 1);
                            ROLLBACK TRAN;
                            RETURN;
                        END
                        ELSE
                        BEGIN
                            -- During DDL refresh, trigger reinit to attempt recovery
                            SET @TriggerReinit = 1;
                            SET @SkipCaptureInstanceCreation = 1;
                            PRINT ''''Both CDC slots occupied by non-lakeflow instances. Triggering reinit for table '''' + @QuotedFullName;
                        END
                    END
                END

                -- Get existing capture instance, preferring lakeflow instances that we own
                SET @OldCaptureInstance = (
                    select top 1 capture_instance
                    from cdc.change_tables
                    where source_object_id=OBJECT_ID(@QuotedFullName)
                        AND (capture_instance LIKE ''''lakeflow[_]%[_][1-2]'''' OR capture_instance LIKE ''''New[_]%[_]%'''')
                    order by create_date ASC
                );

                -- If no lakeflow instance exists, get the oldest instance to use its settings
                -- (but we will not drop it since it does not have lakeflow prefix)
                IF @OldCaptureInstance IS NULL
                BEGIN
                    SET @OldCaptureInstance = (
                        select top 1 capture_instance
                        from cdc.change_tables
                        where source_object_id=OBJECT_ID(@QuotedFullName)
                        order by create_date ASC
                    );

                    -- Warn about pre-existing non-lakeflow instance
                    IF @OldCaptureInstance IS NOT NULL
                    BEGIN
                        DECLARE @PreExistingWarningMsg NVARCHAR(500);
                        SET @PreExistingWarningMsg = ''''WARNING: Table '''' + @QuotedFullName + '''' has a pre-existing capture instance named '''''''''''' + @OldCaptureInstance + '''''''''''' that was not created by lakeflow. Lakeflow will preserve this instance and create its own instance alongside it. Settings (filegroup, role, supports_net_changes) will be copied from the pre-existing instance.'''';
                        PRINT @PreExistingWarningMsg;
                    END
                END
                SET @SupportNetChanges = (select top 1 supports_net_changes from cdc.change_tables where source_object_id=OBJECT_ID(@QuotedFullName) order by create_date ASC);
                SET @FileGroupName = (select top 1 filegroup_name from cdc.change_tables where source_object_id=OBJECT_ID(@QuotedFullName) order by create_date ASC);
                SET @RoleName = (select top 1 role_name from cdc.change_tables where source_object_id=OBJECT_ID(@QuotedFullName) order by create_date ASC);

                IF @LakeflowInstanceToDrop IS NOT NULL
                BEGIN
                    -- Recreate under the opposite suffix from the dropped instance; reusing the same
                    -- change-table name after a drop in one transaction fails on Azure SQL DB / MI.
                    IF @LakeflowInstanceToDrop LIKE ''''%[_]1''''
                    BEGIN
                        SET @NewCaptureInstance = ''''lakeflow_'''' + @schemaName + ''''_'''' + @tableName + ''''_2''''
                    END
                    ELSE
                    BEGIN
                        SET @NewCaptureInstance = ''''lakeflow_'''' + @schemaName + ''''_'''' + @tableName + ''''_1''''
                    END
                END
                ELSE IF @OldCaptureInstance LIKE ''''lakeflow[_]%''''
                BEGIN
                    -- Toggle between _1 and _2 suffixes
                    IF @OldCaptureInstance LIKE ''''%[_]1''''
                    BEGIN
                        SET @NewCaptureInstance = ''''lakeflow_'''' + @schemaName + ''''_'''' + @tableName + ''''_2''''
                    END
                    ELSE
                    BEGIN
                        SET @NewCaptureInstance = ''''lakeflow_'''' + @schemaName + ''''_'''' + @tableName + ''''_1''''
                    END
                END
                ELSE
                BEGIN
                    -- First time or non-lakeflow instance: use lakeflow_schemaName_tableName_1
                    SET @NewCaptureInstance = ''''lakeflow_'''' + @schemaName + ''''_'''' + @tableName + ''''_1''''
                END

                -- Skip capture instance creation ONLY if we cannot create one (e.g., 2 non-lakeflow instances)
                IF @SkipCaptureInstanceCreation = 0
                BEGIN
                    BEGIN TRAN
                        EXEC sys.sp_cdc_enable_table
                            @source_schema = @schemaName,
                            @source_name   = @tableName,
                            @role_name     = @RoleName,
                            @capture_instance = @NewCaptureInstance,
                            @filegroup_name = @FileGroupName,
                            @supports_net_changes = @SupportNetChanges

                        SET @CommittedCursor = (SELECT committedCursor FROM dbo.['' + @captureInstanceTableName + ''] WHERE schemaName=@schemaName AND tableName=@tableName);
                        -- Preserve DDL statements during trigger calls; clear during reinit recovery
                        IF @reinit = 0
                            SET @ExistingDdlStatement = ISNULL((SELECT ddlStatement FROM dbo.['' + @captureInstanceTableName + ''] WHERE schemaName=@schemaName AND tableName=@tableName), '''''''');
                        ELSE
                            SET @ExistingDdlStatement = '''''''';
                        DELETE FROM dbo.['' + @captureInstanceTableName + ''] WHERE schemaName=@schemaName AND tableName=@tableName;
                        -- On reinit, never track a pre-existing (non-lakeflow) instance as the old
                        -- instance; it must never be dropped by a full refresh, even when
                        -- AllowDisablePreExistingCaptureInstances is set.
                        IF @reinit = 1
                            AND NOT (@OldCaptureInstance LIKE ''''lakeflow[_]%[_][1-2]'''' OR @OldCaptureInstance LIKE ''''New[_]%[_]%'''')
                            SET @OldInstanceToTrack = NULL;
                        ELSE
                            SET @OldInstanceToTrack = @OldCaptureInstance;

                        INSERT INTO dbo.['' + @captureInstanceTableName + ''] (oldCaptureInstance, newCaptureInstance, schemaName, tableName, committedCursor, triggerReinit, ddlStatement, pendingAddColumn) VALUES (@OldInstanceToTrack, @NewCaptureInstance, @schemaName, @tableName, @CommittedCursor, @TriggerReinit, @ExistingDdlStatement, 0);
                        IF (@reinit = 0 AND @OldInstanceToTrack IS NOT NULL)
                            EXEC dbo.'' + @mergeCaptureInstancesProcName + '' @schemaName, @tableName;
                    COMMIT TRAN
                END
                ELSE
                BEGIN
                    BEGIN TRAN
                        IF @PendingAddColumn = 1
                        BEGIN
                            -- Both lakeflow CI slots are occupied by an in-progress rotation. Queue the ADD COLUMN
                            -- for processing after the current rotation completes instead of triggering reinit.
                            UPDATE dbo.['' + @captureInstanceTableName + ''] SET pendingAddColumn = 1 WHERE schemaName=@schemaName AND tableName=@tableName;
                        END
                        ELSE
                        BEGIN
                            -- No free slot to create a capture instance. Record the lakeflow instance
                            -- (if any) so the full refresh can replace it; leave NULL when both
                            -- instances are pre-existing.
                            SET @CommittedCursor = (SELECT committedCursor FROM dbo.['' + @captureInstanceTableName + ''] WHERE schemaName=@schemaName AND tableName=@tableName);
                            IF @reinit = 0
                                SET @ExistingDdlStatement = ISNULL((SELECT ddlStatement FROM dbo.['' + @captureInstanceTableName + ''] WHERE schemaName=@schemaName AND tableName=@tableName), '''''''');
                            ELSE
                                SET @ExistingDdlStatement = '''''''';
                            DELETE FROM dbo.['' + @captureInstanceTableName + ''] WHERE schemaName=@schemaName AND tableName=@tableName;
                            INSERT INTO dbo.['' + @captureInstanceTableName + ''] (oldCaptureInstance, newCaptureInstance, schemaName, tableName, committedCursor, triggerReinit, ddlStatement, pendingAddColumn) VALUES (@OldInstanceToTrack, NULL, @schemaName, @tableName, @CommittedCursor, @TriggerReinit, @ExistingDdlStatement, 0);
                        END
                    COMMIT TRAN
                END
            COMMIT TRAN'';
            EXEC sp_executesql @SQL;
            PRINT N''✓ Created procedure: '' + @refreshCaptureInstanceProcName;
        END

        -- Atomic cutover in ONE transaction: drop the non-current alter-table trigger FIRST (taking the
        -- DDL-trigger lock up front), then rename the prior capture-instance table forward (or create a fresh
        -- one), add the 1.6 columns, drop the non-current lifecycle procs, and create the current trigger. The
        -- lifecycle procs are created above, so the committed trigger never calls a proc that does not exist. A
        -- concurrent ALTER TABLE blocks on that lock until commit and then fires the current trigger, so it can
        -- never fire the old trigger against a table this cutover has already renamed away; a mid-cutover
        -- failure rolls back cleanly.
        -- Trade-off: holding the DDL-trigger lock for the whole cutover blocks any concurrent customer ALTER
        -- TABLE in this database until commit. The cutover is metadata-only and normally sub-second, so the
        -- window is small; a customer ALTER whose own timeout is shorter fails fast (retryable) rather than
        -- silently losing the change.
        -- NOTE: the DDL-audit cutover in lakeflowSetupChangeTracking mirrors this transaction scaffolding
        -- (lock timeout, XACT_ABORT, rollback, version-bounded stale-drop); keep the two in sync.
        BEGIN TRY
            -- Fail fast rather than freeze all customer DDL if a lock is contended, and ensure a client
            -- cancel aborts and rolls back the transaction instead of leaving it open holding the
            -- database-scoped DDL lock.
            SET LOCK_TIMEOUT 30000;
            SET XACT_ABORT ON;
            BEGIN TRANSACTION;

            -- Drop the non-current alter-table trigger before the rename so the DDL-trigger lock is held from
            -- the start of the transaction (see the ordering note above). The current trigger is created below,
            -- after the rename; the non-current lifecycle procs are dropped after this, so no still-armed old
            -- trigger references a dropped proc.
            DECLARE @dropOldCdcTrig NVARCHAR(MAX) = '''';
            SELECT @dropOldCdcTrig = @dropOldCdcTrig + ''DROP TRIGGER '' + QUOTENAME(name) + '' ON DATABASE;'' + CHAR(13)
            FROM sys.triggers
            WHERE name LIKE ''lakeflowAlterTableTrigger_%_%''
              AND name <> CAST(@alterTableTriggerName AS sysname) AND parent_class = 0;
            IF LEN(@dropOldCdcTrig) > 0
            BEGIN
                PRINT N''  Dropping prior-version alter-table trigger(s):'' + CHAR(13) + @dropOldCdcTrig;
                EXEC sp_executesql @dropOldCdcTrig;
            END

            IF @ciPrior IS NOT NULL AND OBJECT_ID(''dbo.'' + @captureInstanceTableName, ''U'') IS NULL
            BEGIN
                DECLARE @ciPriorQualified NVARCHAR(300) = ''dbo.'' + @ciPrior;
                EXEC sp_rename @objname = @ciPriorQualified, @newname = @captureInstanceTableName;

                -- Keep the versioned PK constraint name in sync with the table version.
                DECLARE @ciPk SYSNAME = (SELECT name FROM sys.key_constraints
                                         WHERE parent_object_id = OBJECT_ID(''dbo.'' + @captureInstanceTableName) AND type = ''PK'');
                IF @ciPk IS NOT NULL AND @ciPk <> @ciPkName AND OBJECT_ID(@ciPkName) IS NULL
                    EXEC sp_rename @objname = @ciPk, @newname = @ciPkName, @objtype = ''OBJECT'';

                -- Drop any other prior-version trackers (stale duplicates; the live one just moved).
                DECLARE @dropExtraCi NVARCHAR(MAX) = '''';
                SELECT @dropExtraCi = @dropExtraCi + ''DROP TABLE [dbo].'' + QUOTENAME(name) + '';'' + CHAR(13)
                FROM sys.tables
                WHERE name LIKE ''lakeflowCaptureInstanceInfo_%_%'' AND name <> CAST(@captureInstanceTableName AS sysname)
                  -- Only drop strictly-older duplicates; never a newer table (matches the selection bound).
                  AND (dbo.lakeflowVersionComponent(name, 2) < @ciCurMajor
                       OR (dbo.lakeflowVersionComponent(name, 2) = @ciCurMajor
                           AND dbo.lakeflowVersionComponent(name, 1) < @ciCurMinor));
                IF LEN(@dropExtraCi) > 0
                BEGIN
                    PRINT N''  Dropping stale capture instance tables:'' + CHAR(13) + @dropExtraCi;
                    EXEC sp_executesql @dropExtraCi;
                END

                PRINT N''✓ Carried capture instance table forward: renamed '' + @ciPrior + '' to '' + @captureInstanceTableName;
            END

            -- No compatible prior: create a fresh current capture-instance table (with the 1.6 columns).
            -- The 1.6 column set (ddlStatement, pendingAddColumn) is also encoded in the two COL_LENGTH-guarded
            -- heal blocks (the pre-transaction heal above and the in-transaction heal below); a future column
            -- change must update all three, or a carried-forward or interrupted-run table silently lacks it.
            IF OBJECT_ID(''dbo.'' + @captureInstanceTableName, ''U'') IS NULL
            BEGIN
                SET @SQL = N''CREATE TABLE [dbo].['' + @captureInstanceTableName + ''](
                    [oldCaptureInstance] VARCHAR(MAX) NULL,
                    [newCaptureInstance] VARCHAR(MAX) NULL,
                    [schemaName] VARCHAR('' + CAST(@wCiSchema AS VARCHAR(10)) + '') NOT NULL,
                    [tableName] VARCHAR('' + CAST(@wCiTable AS VARCHAR(10)) + '') NOT NULL,
                    [committedCursor] VARCHAR(MAX) NULL,
                    [triggerReinit] BIT NULL,
                    [ddlStatement] NVARCHAR(MAX) NOT NULL DEFAULT '''''''',
                    [pendingAddColumn] BIT NOT NULL DEFAULT 0,
                    CONSTRAINT ['' + @ciPkName + ''] PRIMARY KEY (schemaName, tableName)
                )'';
                EXEC sp_executesql @SQL;
                PRINT N''✓ Created capture instance table: '' + @captureInstanceTableName;
            END

            -- Ensure the 1.6 columns exist on a carried-forward table (idempotent, COL_LENGTH-guarded; heals a
            -- half-migrated table on a re-run). CHAR(39) is a single quote.
            DECLARE @ciAlter NVARCHAR(MAX);
            IF COL_LENGTH(''dbo.'' + @captureInstanceTableName, ''ddlStatement'') IS NULL
            BEGIN
                SET @ciAlter = ''ALTER TABLE [dbo].['' + @captureInstanceTableName + ''] ADD ddlStatement NVARCHAR(MAX) NOT NULL DEFAULT '' + CHAR(39) + CHAR(39);
                EXEC sp_executesql @ciAlter;
            END
            IF COL_LENGTH(''dbo.'' + @captureInstanceTableName, ''pendingAddColumn'') IS NULL
            BEGIN
                SET @ciAlter = ''ALTER TABLE [dbo].['' + @captureInstanceTableName + ''] ADD pendingAddColumn BIT NOT NULL DEFAULT 0'';
                EXEC sp_executesql @ciAlter;
            END

            -- Drop every non-current lifecycle proc (the old alter-table trigger was already dropped at the top
            -- of the transaction, so no still-armed old trigger ever references a dropped proc; a rollback
            -- restores the old trigger and its procs together). The current procs were created before the
            -- transaction, so the current trigger created below still binds.
            DECLARE @dropOldCdcProcs NVARCHAR(MAX) = '''';
            SELECT @dropOldCdcProcs = @dropOldCdcProcs + ''DROP PROCEDURE [dbo].'' + QUOTENAME(name) + '';'' + CHAR(13)
            FROM sys.procedures
            WHERE (name LIKE ''lakeflowDisableOldCaptureInstance_%_%'' AND name <> CAST(@disableOldCaptureInstanceProcName AS sysname))
               OR (name LIKE ''lakeflowMergeCaptureInstances_%_%'' AND name <> CAST(@mergeCaptureInstancesProcName AS sysname))
               OR (name LIKE ''lakeflowRefreshCaptureInstance_%_%'' AND name <> CAST(@refreshCaptureInstanceProcName AS sysname));
            IF LEN(@dropOldCdcProcs) > 0
            BEGIN
                PRINT N''  Dropping prior-version lifecycle procedure(s):'' + CHAR(13) + @dropOldCdcProcs;
                EXEC sp_executesql @dropOldCdcProcs;
            END

            -- Create ALTER TABLE trigger
            IF NOT EXISTS (SELECT 1 FROM sys.triggers WHERE name = CAST(@alterTableTriggerName AS sysname) AND parent_class = 0)
            BEGIN
                SET @SQL = CAST(N''CREATE TRIGGER ['' AS NVARCHAR(MAX)) + @alterTableTriggerName + '']
                    ON DATABASE FOR ALTER_TABLE
                    AS
                    BEGIN
                        SET NOCOUNT ON;
                        SET ANSI_PADDING ON;
                        SET ANSI_NULLS ON;
                        SET QUOTED_IDENTIFIER ON;
    
                        DECLARE @data XML = EVENTDATA();
                        DECLARE @DbName NVARCHAR(255) = DB_NAME();
                        DECLARE @schemaName NVARCHAR(MAX) = @data.value(''''(/EVENT_INSTANCE/SchemaName)[1]'''', ''''NVARCHAR(MAX)'''');
                        DECLARE @tableName NVARCHAR(255) = @data.value(''''(/EVENT_INSTANCE/ObjectName)[1]'''', ''''NVARCHAR(255)'''');
                        DECLARE @isCreateColumn NVARCHAR(255) = @data.value(''''(/EVENT_INSTANCE/AlterTableActionList/Create/Columns)[1]'''', ''''NVARCHAR(255)'''');
                        DECLARE @isCreateConstraint NVARCHAR(255) = @data.value(''''(/EVENT_INSTANCE/AlterTableActionList/Create/Constraints)[1]'''', ''''NVARCHAR(255)'''');
                        DECLARE @isDropConstraint NVARCHAR(255) = @data.value(''''(/EVENT_INSTANCE/AlterTableActionList/Drop/Constraints)[1]'''', ''''NVARCHAR(255)'''');
                        DECLARE @ddlStatement NVARCHAR(MAX) = @data.value(''''(/EVENT_INSTANCE/TSQLCommand/CommandText)[1]'''', ''''NVARCHAR(MAX)'''');
                        DECLARE @IsCdcEnabledDBLevel BIT = (SELECT is_cdc_enabled FROM sys.databases WHERE name = CAST(@DbName AS sysname));
                        DECLARE @IsCdcEnabledTableLevel BIT = (SELECT is_tracked_by_cdc FROM sys.tables WHERE schema_id = SCHEMA_ID(@schemaName) AND name = CAST(@tableName AS sysname));
    
                        -- ADD COLUMN: refresh capture instance via procedure
                        IF (@IsCdcEnabledDBLevel = 1 AND @IsCdcEnabledTableLevel = 1 AND @isCreateColumn IS NOT NULL)
                        BEGIN
                            EXEC [dbo].['' + @refreshCaptureInstanceProcName + ''] @schemaName, @tableName;
                        END
                        -- ADD/DROP CONSTRAINT: append DDL statement for Java-side parsing
                        -- The Java extractor will parse each DDL to determine if reinit is needed (e.g. PK/UK changes).
                        ELSE IF (@IsCdcEnabledDBLevel = 1 AND @IsCdcEnabledTableLevel = 1 AND (@isCreateConstraint IS NOT NULL OR @isDropConstraint IS NOT NULL))
                        BEGIN
                            IF EXISTS (SELECT 1 FROM dbo.['' + @captureInstanceTableName + ''] WHERE schemaName = @schemaName AND tableName = @tableName)
                                UPDATE dbo.['' + @captureInstanceTableName + ''] SET ddlStatement = CASE WHEN ddlStatement = '''''''' THEN @ddlStatement ELSE ddlStatement + ''''LAKEFLOW_CONNECT_DDL_SEPARATOR'''' + @ddlStatement END WHERE schemaName = @schemaName AND tableName = @tableName;
                            ELSE
                                INSERT INTO dbo.['' + @captureInstanceTableName + ''] (oldCaptureInstance, newCaptureInstance, schemaName, tableName, committedCursor, triggerReinit, ddlStatement)
                                VALUES (NULL, NULL, @schemaName, @tableName, NULL, 0, @ddlStatement);
                        END
                    END'';
                EXEC sp_executesql @SQL;
                PRINT N''✓ Created ALTER TABLE trigger: '' + @alterTableTriggerName;
            END

            COMMIT TRANSACTION;
            SET XACT_ABORT OFF;
            SET LOCK_TIMEOUT -1;
        END TRY
        BEGIN CATCH
            IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
            SET XACT_ABORT OFF;
            SET LOCK_TIMEOUT -1;
            -- Error 1222 (lock request timeout) means a concurrent ALTER held the DDL lock past the
            -- cutover timeout: transient, and safe to re-run once the concurrent change completes.
            IF ERROR_NUMBER() = 1222
                PRINT N''✗ Cutover blocked by a concurrent ALTER TABLE (lock timeout); the capture table and trigger swap were rolled back. This is transient -- re-run setup once the concurrent change completes.'';
            ELSE
                PRINT N''✗ Cutover rolled back; the capture table and trigger swap were undone.'';
            THROW;
        END CATCH

        -- User resolution
        IF @User IS NOT NULL AND @User != ''''
        BEGIN
            -- Check if user exists as database user
            IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE principal_id = DATABASE_PRINCIPAL_ID(@User))
            BEGIN
                -- Check if it is a server login and find its mapped database user
                SELECT @DatabaseUser = dp.name
                FROM sys.database_principals dp
                INNER JOIN sys.server_principals sp ON dp.sid = sp.sid
                WHERE sp.name = CAST(@User AS sysname)
                    AND dp.type IN (''S'', ''U'', ''G'')
                    AND dp.name NOT IN (''guest'');

                -- If still no database user found, warn and exit
                IF @DatabaseUser IS NULL OR @DatabaseUser = @User
                BEGIN
                    PRINT N''⚠ Warning: User/Login ['' + @User + ''] not found as database user. Skipping permission grants.'';
                    PRINT N''  To fix: CREATE USER ['' + @User + ''] FOR LOGIN ['' + @User + ''];'';
                    SET @DatabaseUser = NULL;
                END
                ELSE
                BEGIN
                    PRINT N''Server login ['' + @User + ''] maps to database user ['' + @DatabaseUser + ''].'';
                END
            END

            -- Special handling for dbo user - cannot grant permissions to dbo
            IF @DatabaseUser = ''dbo''
            BEGIN
                PRINT N''Skipping permission grants (dbo already has all permissions).'';
                SET @DatabaseUser = NULL;
            END
        END

        -- Grant permissions to user if specified
        IF @DatabaseUser IS NOT NULL
        BEGIN
            PRINT N''Granting CDC DDL support object permissions to user: '' + @DatabaseUser;
            BEGIN TRY
                SET @SQL = N''GRANT SELECT, UPDATE ON [dbo].['' + @captureInstanceTableName + ''] TO ['' + @DatabaseUser + '']'';
                EXEC sp_executesql @SQL;
                SET @SQL = N''GRANT VIEW DEFINITION TO ['' + @DatabaseUser + '']'';
                EXEC sp_executesql @SQL;
                SET @SQL = N''GRANT VIEW DATABASE STATE TO ['' + @DatabaseUser + '']'';
                EXEC sp_executesql @SQL;
                SET @SQL = N''GRANT SELECT ON SCHEMA::dbo TO ['' + @DatabaseUser + '']'';
                EXEC sp_executesql @SQL;
                SET @SQL = N''GRANT SELECT, INSERT ON SCHEMA::cdc TO ['' + @DatabaseUser + '']'';
                EXEC sp_executesql @SQL;
                SET @SQL = N''GRANT EXECUTE ON [dbo].['' + @disableOldCaptureInstanceProcName + ''] TO ['' + @DatabaseUser + '']'';
                EXEC sp_executesql @SQL;
                SET @SQL = N''GRANT EXECUTE ON [dbo].['' + @mergeCaptureInstancesProcName + ''] TO ['' + @DatabaseUser + '']'';
                EXEC sp_executesql @SQL;
                SET @SQL = N''GRANT EXECUTE ON [dbo].['' + @refreshCaptureInstanceProcName + ''] TO ['' + @DatabaseUser + '']'';
                EXEC sp_executesql @SQL;
                PRINT N''✓ Granted CDC permissions to '' + @DatabaseUser;
            END TRY
            BEGIN CATCH
                PRINT N''⚠ Could not grant CDC permissions to '' + @DatabaseUser + '': '' + ERROR_MESSAGE();
            END CATCH
        END

        -- Process tables if specified
        IF @Tables IS NOT NULL
        BEGIN
            PRINT N''Processing tables for Change Data Capture enablement...'';

            DECLARE @TargetTables TABLE (
                SchemaName NVARCHAR(128),
                TableName NVARCHAR(128),
                HasPrimaryKey BIT
            );

            -- Parse table list and populate target tables
            IF @Tables = ''ALL''
            BEGIN
                INSERT INTO @TargetTables (SchemaName, TableName, HasPrimaryKey)
                SELECT
                    s.name,
                    t.name,
                    CASE WHEN EXISTS (
                        SELECT 1 FROM sys.key_constraints kc
                        WHERE kc.parent_object_id = t.object_id
                        AND kc.type = ''PK''
                    ) THEN 1 ELSE 0 END
                FROM sys.tables t
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE t.is_ms_shipped = 0;
            END
            ELSE IF @Tables LIKE ''SCHEMAS:%''
            BEGIN
                DECLARE @SchemaList NVARCHAR(MAX) = SUBSTRING(@Tables, 9, LEN(@Tables));
                -- Accept bracket-quoted schema names such as [dbo]; the catalog is matched unbracketed.
                SET @SchemaList = REPLACE(REPLACE(@SchemaList, ''['', ''''), '']'', '''');
                INSERT INTO @TargetTables (SchemaName, TableName, HasPrimaryKey)
                SELECT
                    s.name, t.name,
                    CASE WHEN pk.CONSTRAINT_NAME IS NOT NULL THEN 1 ELSE 0 END
                FROM sys.tables t
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS pk ON
                    pk.TABLE_SCHEMA COLLATE DATABASE_DEFAULT = s.name COLLATE DATABASE_DEFAULT AND pk.TABLE_NAME COLLATE DATABASE_DEFAULT = t.name COLLATE DATABASE_DEFAULT AND pk.CONSTRAINT_TYPE COLLATE DATABASE_DEFAULT = ''PRIMARY KEY''
                WHERE t.type = ''U''
                    AND s.name COLLATE DATABASE_DEFAULT IN (SELECT LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(Split.a.value(''.'', ''NVARCHAR(MAX)''), CHAR(10), ''''), CHAR(13), ''''), CHAR(9), ''''))) AS value
                FROM (
                    SELECT CAST(''<M>'' + REPLACE(@SchemaList, '','', ''</M><M>'') + ''</M>'' AS XML) AS Data
                ) AS A
                CROSS APPLY Data.nodes(''/M'') AS Split(a)
                WHERE LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(Split.a.value(''.'', ''NVARCHAR(MAX)''), CHAR(10), ''''), CHAR(13), ''''), CHAR(9), ''''))) != '''');
            END
            ELSE
            BEGIN
                DECLARE @TableList TABLE (FullTableName NVARCHAR(261));
                INSERT INTO @TableList (FullTableName)
                SELECT LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(Split.a.value(''.'', ''NVARCHAR(MAX)''), CHAR(10), ''''), CHAR(13), ''''), CHAR(9), ''''))) AS value
                FROM (
                    SELECT CAST(''<M>'' + REPLACE(@Tables, '','', ''</M><M>'') + ''</M>'' AS XML) AS Data
                ) AS A
                CROSS APPLY Data.nodes(''/M'') AS Split(a)
                WHERE LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(Split.a.value(''.'', ''NVARCHAR(MAX)''), CHAR(10), ''''), CHAR(13), ''''), CHAR(9), ''''))) != '''';

                -- Accept bracket-quoted identifiers such as [dbo].[employees]; the catalog names matched
                -- below are unbracketed. Strip only when brackets are balanced and any star is a trailing
                -- .* wildcard, so a comma-split fragment (from a bracketed name containing a comma), a name
                -- containing a lone ], or a bracket-quoted star [dbo].[*] is left as-is and stays a no-op
                -- rather than mis-matching a different table or expanding to the dbo.* wildcard.
                UPDATE @TableList
                SET FullTableName = REPLACE(REPLACE(FullTableName, ''['', ''''), '']'', '''')
                WHERE DATALENGTH(REPLACE(FullTableName, ''['', '''')) = DATALENGTH(REPLACE(FullTableName, '']'', ''''))
                    AND (CHARINDEX(''*'', FullTableName) = 0 OR FullTableName LIKE ''%.*'');

                INSERT INTO @TargetTables (SchemaName, TableName, HasPrimaryKey)
                SELECT
                    s.name, t.name,
                    CASE WHEN pk.CONSTRAINT_NAME IS NOT NULL THEN 1 ELSE 0 END
                FROM sys.tables t
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                INNER JOIN @TableList tl ON
                    (tl.FullTableName = s.name COLLATE DATABASE_DEFAULT + ''.*'' OR
                     tl.FullTableName = s.name COLLATE DATABASE_DEFAULT + ''.'' + t.name COLLATE DATABASE_DEFAULT OR
                     (CHARINDEX(''.'', tl.FullTableName) = 0 AND tl.FullTableName = t.name COLLATE DATABASE_DEFAULT AND s.name COLLATE DATABASE_DEFAULT = ''dbo''))
                LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS pk ON
                    pk.TABLE_SCHEMA COLLATE DATABASE_DEFAULT = s.name COLLATE DATABASE_DEFAULT AND pk.TABLE_NAME COLLATE DATABASE_DEFAULT = t.name COLLATE DATABASE_DEFAULT AND pk.CONSTRAINT_TYPE COLLATE DATABASE_DEFAULT = ''PRIMARY KEY''
                WHERE t.type = ''U'';
            END

            -- Process each table for CDC enablement
            DECLARE @CurrentSchema NVARCHAR(128), @CurrentTable NVARCHAR(128);
            DECLARE @ProcessedCount INT = 0, @SkippedCount INT = 0, @ErrorCount INT = 0;

            DECLARE table_cursor CURSOR FOR
                SELECT SchemaName, TableName FROM @TargetTables ORDER BY SchemaName, TableName;

            OPEN table_cursor;
            FETCH NEXT FROM table_cursor INTO @CurrentSchema, @CurrentTable;

            WHILE @@FETCH_STATUS = 0
            BEGIN
                BEGIN TRY
                    IF NOT EXISTS (
                        SELECT 1 FROM cdc.change_tables ct
                        INNER JOIN sys.tables t ON ct.source_object_id = t.object_id
                        WHERE t.schema_id = SCHEMA_ID(@CurrentSchema) AND t.name = CAST(@CurrentTable AS sysname)
                    )
                    BEGIN
                        DECLARE @LakeflowCaptureInstance NVARCHAR(255) = ''lakeflow_'' + @CurrentSchema + ''_'' + @CurrentTable + ''_1'';
                        EXEC sys.sp_cdc_enable_table
                            @source_schema = @CurrentSchema,
                            @source_name = @CurrentTable,
                            @role_name = NULL,
                            @capture_instance = @LakeflowCaptureInstance;
                        PRINT N''✓ Enabled CDC on ['' + @CurrentSchema + ''].['' + @CurrentTable + ''] with capture instance '' + @LakeflowCaptureInstance;
                        SET @ProcessedCount = @ProcessedCount + 1;
                    END
                    ELSE
                    BEGIN
                        PRINT N''ℹ CDC already enabled on ['' + @CurrentSchema + ''].['' + @CurrentTable + '']'';
                        SET @SkippedCount = @SkippedCount + 1;
                    END
                END TRY
                BEGIN CATCH
                    PRINT N''✗ Error enabling CDC on ['' + @CurrentSchema + ''].['' + @CurrentTable + '']: '' + ERROR_MESSAGE();
                    SET @ErrorCount = @ErrorCount + 1;
                END CATCH

                FETCH NEXT FROM table_cursor INTO @CurrentSchema, @CurrentTable;
            END

            CLOSE table_cursor;
            DEALLOCATE table_cursor;

            -- Summary
            PRINT N'''';
            PRINT N''CDC setup summary:'';
            PRINT N''  - Tables processed: '' + CAST(@ProcessedCount AS NVARCHAR(10));
            PRINT N''  - Tables already enabled: '' + CAST(@SkippedCount AS NVARCHAR(10));
            PRINT N''  - Tables with errors: '' + CAST(@ErrorCount AS NVARCHAR(10));
        END

        -- The alter-table trigger must exist on exit; a mid-cutover failure auto-commits with none in
        -- place, silently dropping ALTER TABLEs. Fail loudly with remediation instead.
        IF NOT EXISTS (SELECT 1 FROM sys.triggers
                       WHERE name = CAST(@alterTableTriggerName AS sysname) AND parent_class = 0)
            THROW 51000, N''Alter-table trigger is missing after setup. Please re-run lakeflowSetupChangeDataCapture. Schema changes (ALTER TABLE) made since a failed run may have been missed, and those tables may need a full refresh.'', 1;

        PRINT N''Change Data Capture setup completed successfully'';

    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ''Error in lakeflowSetupChangeDataCapture: '' + ERROR_MESSAGE();
        PRINT @ErrorMessage;
        THROW;
    END CATCH
END';
PRINT N'Created lakeflowSetupChangeDataCapture procedure';

-- Final validation and summary
BEGIN
    PRINT N'';
    PRINT N'=== Installation Summary ===';
    DECLARE @Platform NVARCHAR(50) = dbo.lakeflowDetectPlatform();
    PRINT N'Platform: ' + @Platform;
    PRINT N'Version: ' + dbo.lakeflowUtilityVersion();

    -- Verify created objects
    IF OBJECT_ID('dbo.lakeflowDetectPlatform', 'FN') IS NOT NULL
        PRINT N'✓ lakeflowDetectPlatform function created successfully'
    ELSE
        PRINT N'✗ lakeflowDetectPlatform function creation failed';

    IF OBJECT_ID('dbo.lakeflowUtilityVersion', 'FN') IS NOT NULL
        PRINT N'✓ lakeflowUtilityVersion function created successfully'
    ELSE
        PRINT N'✗ lakeflowUtilityVersion function creation failed';

    IF OBJECT_ID('dbo.lakeflowFixPermissions', 'P') IS NOT NULL
        PRINT N'✓ lakeflowFixPermissions procedure created successfully'
    ELSE
        PRINT N'✗ lakeflowFixPermissions procedure creation failed';

    IF OBJECT_ID('dbo.lakeflowSetupChangeTracking', 'P') IS NOT NULL
        PRINT N'✓ lakeflowSetupChangeTracking procedure created successfully'
    ELSE
        PRINT N'✗ lakeflowSetupChangeTracking procedure creation failed';

    IF OBJECT_ID('dbo.lakeflowSetupChangeDataCapture', 'P') IS NOT NULL
        PRINT N'✓ lakeflowSetupChangeDataCapture procedure created successfully'
    ELSE
        PRINT N'✗ lakeflowSetupChangeDataCapture procedure creation failed';

    PRINT N'';
    PRINT N'=== Usage Examples ===';
    PRINT N'-- Table-specific setup:';
    PRINT N'EXEC dbo.lakeflowSetupChangeTracking @Tables = ''dbo.Table1,Sales.Orders'', @User = ''YourUsername'';';
    PRINT N'';
    PRINT N'-- Enable change tracking on all user tables (auto-discovers, skips tables without PKs):';
    PRINT N'EXEC dbo.lakeflowSetupChangeTracking @Tables = ''ALL'', @User = ''YourUsername'';';
    PRINT N'';
    PRINT N'-- Enable change tracking on all tables in specific schemas:';
    PRINT N'EXEC dbo.lakeflowSetupChangeTracking @Tables = ''SCHEMAS:Sales,HR,Production'', @User = ''YourUsername'';';
    PRINT N'';
    PRINT N'-- Enable change tracking with wildcard support:';
    PRINT N'EXEC dbo.lakeflowSetupChangeTracking @Tables = ''Sales.*,HR.Employees,dbo.SpecialTable'', @User = ''YourUsername'';';
    PRINT N'';
    PRINT N'-- Enable CDC on all user tables (processes tables with and without PKs):';
    PRINT N'EXEC dbo.lakeflowSetupChangeDataCapture @Tables = ''ALL'', @User = ''YourUsername'';';
    PRINT N'';
    PRINT N'-- Smart two-step approach for complete coverage:';
    PRINT N'-- Step 1: Enable CT on tables with primary keys';
    PRINT N'EXEC dbo.lakeflowSetupChangeTracking @Tables = ''ALL'', @User = ''YourUsername'';';
    PRINT N'-- Step 2: Enable CDC on tables without primary keys';
    PRINT N'EXEC dbo.lakeflowSetupChangeDataCapture @Tables = ''ALL'', @User = ''YourUsername'';';
    PRINT N'';
    PRINT N'-- Fix permissions for a user:';
    PRINT N'EXEC dbo.lakeflowFixPermissions @User = ''YourUsername'';';
    PRINT N'';
    PRINT N'-- Grant table permissions for specific tables:';
    PRINT N'EXEC dbo.lakeflowFixPermissions @User = ''YourUsername'', @Tables = ''ALL'';';
    PRINT N'EXEC dbo.lakeflowFixPermissions @User = ''YourUsername'', @Tables = ''Sales.*,HR.Employees'';';
    PRINT N'';
    PRINT N'-- Setup change tracking at database level only (no table processing):';
    PRINT N'EXEC dbo.lakeflowSetupChangeTracking @Tables = NULL, @User = ''YourUsername'';';
    PRINT N'';
    PRINT N'-- NOTE: The @User parameter is optional. If provided, the procedures will grant';
    PRINT N'-- the specified user permissions to access DDL support objects (audit tables, etc.)';
    PRINT N'-- This is useful for granting read access to change tracking metadata.';
    PRINT N'';
    PRINT N'-- Cleanup DDL support objects:';
    PRINT N'EXEC dbo.lakeflowSetupChangeTracking @Mode = ''CLEANUP'';';
    PRINT N'EXEC dbo.lakeflowSetupChangeDataCapture @Mode = ''CLEANUP'';';
    PRINT N'';
    PRINT N'=== Installation Complete ===';
    PRINT N'All utility objects have been installed successfully.';
    PRINT N'';
    PRINT N'=== Available Procedures ===';
    PRINT N'1. lakeflowFixPermissions - Fix user permissions for ingestion';
    PRINT N'2. lakeflowSetupChangeTracking - Setup change tracking and DDL audit objects';
    PRINT N'3. lakeflowSetupChangeDataCapture - Setup CDC and capture instance objects';
    PRINT N'';
    PRINT N'For more information, visit: https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-source-setup';
END

