/*

    Description:
    This script prepares the SQL Server for CDC (Change Data Capture) by creating the necessary database objects and triggers.

    Tables Created:
    - [MSXDBCDC].[dbo].[xdbcdc_databases]
    - [MSXDBCDC].[dbo].[xdbcdc_services]
    - [MSXDBCDC].[dbo].[xdbcdc_trace]

    Triggers Created:
    - [MSXDBCDC].[dbo].[tr_after_ui_dbo_xdbcdc_databases]

    Stored Procedures Created:
    - [dbo].[xdbcdc_enable_db]
    - [dbo].[xdbcdc_disable_db]

    Usage:
    1. Execute this script in the SQL Server Management Studio or any other SQL Server query tool.
    2. Make sure you have the necessary permissions to create databases, tables, triggers, and stored procedures.
    3. Modify the script if needed to match your specific requirements.

    Note:
    - This script assumes that the SQL Server is already installed and running.
    - This script assumes that the user executing the script has the necessary permissions.
*/
--Prepare SQL server

USE [master]
GO

IF NOT EXISTS (SELECT 1 FROM [sys].[databases] WHERE [name] = N'MSXDBCDC') 
CREATE DATABASE [MSXDBCDC] 
COLLATE SQL_Latin1_General_CP1_CS_AS
GO

USE MSXDBCDC
GO

IF NOT EXISTS (SELECT 1 FROM [sys].[tables] WHERE [name] = N'xdbcdc_databases') 
CREATE TABLE [dbo].[xdbcdc_databases] ( 
	 [name] [nvarchar](128) NOT NULL, 
	 [config_version] [datetime] NULL, 
	 [cdc_service_name] NVARCHAR(256), 
	 [enabled] [bit] NOT NULL 
CONSTRAINT [xdbcdc_databases_clustered_idx] PRIMARY KEY CLUSTERED 
([name] ASC) WITH 
(PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, 
IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, 
ALLOW_PAGE_LOCKS  = OFF) ON [PRIMARY] 
)
GO

CREATE TRIGGER [tr_after_ui_dbo_xdbcdc_databases] 
ON [MSXDBCDC].[dbo].[xdbcdc_databases] AFTER INSERT,UPDATE AS 
BEGIN 
	 DECLARE @new_version DATETIME 
	 DECLARE @dbName NVARCHAR(128) 
	 SET @new_version = GETUTCDATE() 
	 IF EXISTS (SELECT [name] FROM deleted) 
		 SET @dbName = (SELECT [name] FROM deleted) 
	 ELSE 
		 SET @dbName = (SELECT [name] FROM inserted) 
	 UPDATE [dbo].[xdbcdc_databases] 
	 SET [config_version] = @new_version 
WHERE [name] = @dbName 
END 
GO

IF NOT EXISTS (SELECT 1 FROM [sys].[tables] WHERE [name] = N'xdbcdc_services') 
CREATE TABLE [dbo].[xdbcdc_services] ( 
	 [cdc_service_name] NVARCHAR (128), 
	 [cdc_service_sql_login] NVARCHAR (128), 
	 [active_service_node] NVARCHAR (24) NULL, 
	 [active_service_heartbeat] DATETIME NULL, 
	 [ref_count] INT, 
	 [options] NVARCHAR(1024), 
CONSTRAINT [xdbcdc_services_clustered_idx] PRIMARY KEY CLUSTERED 
([cdc_service_name] ASC) WITH 
(PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, 
IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, 
ALLOW_PAGE_LOCKS  = OFF) ON [PRIMARY] 
)
GO

IF NOT EXISTS (SELECT 1 FROM [sys].[tables] WHERE [name]='xdbcdc_trace') 
CREATE TABLE [dbo].[xdbcdc_trace] 
( 
	 [timestamp] DATETIME2 NOT NULL, 
	 [type] NVARCHAR(8) NOT NULL, 
	 [node] NVARCHAR(24), 
	 [status] NVARCHAR(16) NOT NULL, 
	 [sub_status] NVARCHAR(64) NOT NULL, 
	 [status_message] NVARCHAR(512) NOT NULL, 
	 [source] VARCHAR(128), 
	 [text_data] NVARCHAR(MAX), 
	 [binary_data] VARBINARY(MAX)) 
ALTER TABLE [dbo].[xdbcdc_trace] 
ADD CONSTRAINT [xdbcdc_trace_clustered_idx] PRIMARY KEY CLUSTERED ( 
	 [timestamp] ASC 
) WITH ( 
PAD_INDEX=OFF, STATISTICS_NORECOMPUTE=OFF, SORT_IN_TEMPDB=OFF, 
IGNORE_DUP_KEY=OFF, ONLINE=OFF, ALLOW_ROW_LOCKS=ON, 
ALLOW_PAGE_LOCKS=ON)
GO

CREATE PROCEDURE [dbo].[xdbcdc_enable_db] 
( 
	 @dbname NVARCHAR (128), 
	 @svcname NVARCHAR (128) 
) 
AS 
BEGIN 
DECLARE @EndLine nchar(2); 
SET @EndLine = NCHAR(13) + NCHAR(10); 
DECLARE @sql nvarchar(MAX) 
DECLARE @BigSQL nvarchar(4000); 
DECLARE @QuotedDbName NVARCHAR (256) 
SET @QuotedDbName = replace(@dbname, N']', N']]') 
SET @sql = N'IF NOT EXISTS (SELECT 1 FROM [sys].[tables] WHERE [name]=''''xdbcdc_config'''')' + @EndLine + 
N'CREATE TABLE [cdc].[xdbcdc_config] ' + @EndLine + 
N'( ' + @EndLine + 
N'[version] DATETIME,' + @EndLine + 
N'[connect_string] NVARCHAR(1024), ' + @EndLine + 
N'[use_windows_authentication] BIT, ' + @EndLine + 
N'[username] NVARCHAR(128), ' + @EndLine + 
N'[password] VARBINARY(512), ' + @EndLine + 
N'[transaction_staging_timeout] INT, ' + @EndLine + 
N'[memory_limit] INT, ' + @EndLine + 
N'[options] NVARCHAR(1024) ' + @EndLine + 
N') ' + @EndLine + 
N'IF NOT EXISTS (SELECT 1 FROM [sys].[tables] WHERE [name]=''''xdbcdc_trace'''') ' + @EndLine + 
N'CREATE TABLE [cdc].[xdbcdc_trace] ' + @EndLine + 
N'( ' + @EndLine + 
N'[timestamp] DATETIME2 NOT NULL, ' + @EndLine + 
N'[type] NVARCHAR(8) NOT NULL, ' + @EndLine + 
N'[node] NVARCHAR(24), ' + @EndLine + 
N'[status] NVARCHAR(16) NOT NULL, ' + @EndLine + 
N'[sub_status] NVARCHAR(64) NOT NULL, ' + @EndLine + 
N'[status_message] NVARCHAR(512) NOT NULL, ' + @EndLine + 
N'[source] VARCHAR(128), ' + @EndLine + 
N'[text_data] NVARCHAR(MAX), ' + @EndLine + 
N'[binary_data] VARBINARY(MAX)) ' + @EndLine + 
N'ALTER TABLE [cdc].[xdbcdc_trace] ' + @EndLine + 
N'ADD CONSTRAINT [xdbcdc_trace_clustered_idx] PRIMARY KEY CLUSTERED ( ' + @EndLine + 
N'[timestamp] ASC ' + @EndLine + 
N') WITH ( ' + @EndLine + 
N'PAD_INDEX=OFF, STATISTICS_NORECOMPUTE=OFF, SORT_IN_TEMPDB=OFF, ' + @EndLine + 
N'IGNORE_DUP_KEY=OFF, ONLINE=OFF, ALLOW_ROW_LOCKS=ON, ' + @EndLine + 
N'ALLOW_PAGE_LOCKS=ON) ' + @EndLine + 
N'IF NOT EXISTS (SELECT 1 FROM [sys].[tables] WHERE [name]=''''xdbcdc_state'''') ' + @EndLine + 
N'CREATE TABLE [cdc].[xdbcdc_state] ' + @EndLine + 
N'( ' + @EndLine + 
N'[status] NVARCHAR(16), ' + @EndLine + 
N'[sub_status] NVARCHAR(64), ' + @EndLine + 
N'[active] BIT, ' + @EndLine + 
N'[error] BIT, ' + @EndLine + 
N'[status_message] NVARCHAR(512), ' + @EndLine + 
N'[timestamp] DATETIME, ' + @EndLine + 
N'[active_capture_node] NVARCHAR(24), ' + @EndLine + 
N'[last_transaction_timestamp] DATETIME2, ' + @EndLine + 
N'[last_change_timestamp] DATETIME2, ' + @EndLine + 
N'[transaction_log_head_cn] BINARY(22), ' + @EndLine + 
N'[transaction_log_tail_cn] BINARY(22), ' + @EndLine + 
N'[current_cn] BINARY(22), ' + @EndLine + 
N'[software_version] INT, ' + @EndLine + 
N'[completed_transactions] BIGINT, ' + @EndLine + 
N'[written_changes] BIGINT, ' + @EndLine + 
N'[read_changes] BIGINT, ' + @EndLine + 
N'[active_transactions] INT, ' + @EndLine + 
N'[staged_transactions] INT ' + @EndLine + 
N') ' + @EndLine + 
N'IF NOT EXISTS (SELECT 1 FROM [sys].[tables] WHERE [name]=''''xdbcdc_staged_transactions'''') ' + @EndLine + 
N'CREATE TABLE [cdc].[xdbcdc_staged_transactions] ' + @EndLine + 
N'( ' + @EndLine + 
N'[transaction_id] BINARY(10) NOT NULL, ' + @EndLine + 
N'[seq_num] INT NOT NULL, ' + @EndLine + 
N'[data_start_cn] BINARY(22) NULL, ' + @EndLine + 
N'[data_end_cn] BINARY(22) NOT NULL, ' + @EndLine + 
N'[data] VARBINARY(MAX)) ' + @EndLine + 
N'ALTER TABLE [cdc].[xdbcdc_staged_transactions] ' + @EndLine + 
N'ADD CONSTRAINT [xdbcdc_staged_transactions_clustered_idx] PRIMARY KEY CLUSTERED ( ' + @EndLine + 
N'[transaction_id] ASC, ' + @EndLine + 
N'[seq_num] ASC ' + @EndLine + 
N') WITH ( ' + @EndLine + 
N'PAD_INDEX=OFF, STATISTICS_NORECOMPUTE=OFF, SORT_IN_TEMPDB=OFF, ' + @EndLine + 
N'IGNORE_DUP_KEY=OFF, ONLINE=OFF, ALLOW_ROW_LOCKS=ON, ' + @EndLine + 
N'ALLOW_PAGE_LOCKS=ON ' + @EndLine + 
N')' 
SET @BigSQL = 'USE [' + @QuotedDbName + ']; EXEC sp_executesql N''' + @sql + ''''; 
PRINT @BigSQL 
EXEC (@BigSQL) 
SET @sql = N'CREATE TRIGGER [tr_after_u_cdc_xdbcdc_config]' + @EndLine + 
N'ON [cdc].[xdbcdc_config] AFTER UPDATE AS' + @EndLine + 
N'BEGIN' + @EndLine + 
N'  DECLARE @new_version DATETIME' + @EndLine + 
N'  SET @new_version = GETUTCDATE()' + @EndLine + 
N'  UPDATE [cdc].[xdbcdc_config]' + @EndLine + 
N'  SET [version] = @new_version' + @EndLine + 
N'  IF EXISTS(SELECT name FROM [MSXDBCDC].[dbo].[xdbcdc_databases]' + @EndLine + 
N'  WHERE name = db_name())' + @EndLine + 
N'  BEGIN' + @EndLine + 
N'  UPDATE [MSXDBCDC].[dbo].[xdbcdc_databases]' + @EndLine + 
N'  SET config_version = @new_version' + @EndLine + 
N'  WHERE name = db_name()' + @EndLine + 
N'  END' + @EndLine + 
N'  ELSE' + @EndLine + 
N'  BEGIN' + @EndLine + 
N'  INSERT INTO [MSXDBCDC].[dbo].[xdbcdc_databases](name,config_version,enabled)' + @EndLine + 
N'  VALUES(db_name(), @new_version, 0)' + @EndLine + 
N'  END' + @EndLine + 
N'END' 
SET @BigSQL = 'USE [' + @QuotedDbName + ']; EXEC sp_executesql N''' + @sql + ''''; 
PRINT @BigSQL 
EXEC (@BigSQL) 
SET @sql = N'EXEC [sys].[sp_MS_marksystemobject]  N''''cdc.xdbcdc_config''''' + @EndLine + 
N'EXEC [sys].[sp_MS_marksystemobject]  N''''cdc.xdbcdc_state''''' + @EndLine + 
N'EXEC [sys].[sp_MS_marksystemobject]  N''''cdc.xdbcdc_staged_transactions''''' + @EndLine + 
N'EXEC [sys].[sp_MS_marksystemobject]  N''''cdc.xdbcdc_trace''''' 
SET @BigSQL = 'USE [' + @QuotedDbName + ']; EXEC sp_executesql N''' + @sql + ''''; 
PRINT @BigSQL 
EXEC (@BigSQL) 
SET @sql = N'INSERT INTO [cdc].[xdbcdc_config]([version])' + @EndLine + 
N'VALUES(GETUTCDATE())' 
SET @BigSQL = 'USE [' + @QuotedDbName + ']; EXEC sp_executesql N''' + @sql + ''''; 
PRINT @BigSQL 
EXEC (@BigSQL) 
SET @sql = N' CREATE TRIGGER [tr_insteadof_id_cdc_xdbcdc_config]' + @EndLine + 
N' ON [cdc].[xdbcdc_config] INSTEAD OF INSERT,DELETE AS' + @EndLine + 
N' BEGIN' + @EndLine + 
N' RAISERROR (''''Cannot DELETE or INSERT [cdc].[xdbcdc_config]'''', -1, -1)' + @EndLine + 
N' END' 
SET @BigSQL = 'USE [' + @QuotedDbName + ']; EXEC sp_executesql N''' + @sql + ''''; 
PRINT @BigSQL 
EXEC (@BigSQL) 
SET @sql = N'GRANT SELECT ON [cdc].[xdbcdc_state] TO public' 
SET @BigSQL = 'USE [' + @QuotedDbName + ']; EXEC sp_executesql N''' + @sql + ''''; 
PRINT @BigSQL 
EXEC (@BigSQL) 
INSERT INTO [MSXDBCDC].[dbo].[xdbcdc_databases] (name, config_version, cdc_service_name, enabled) 
VALUES (@dbname , GETUTCDATE(), @svcname, 0) 
DECLARE @service_login NVARCHAR (128) 
DECLARE @current_login NVARCHAR (128) 
SELECT @service_login = [cdc_service_sql_login] 
FROM [MSXDBCDC].[dbo].[xdbcdc_services] 
WHERE [cdc_service_name] = @svcname 
SELECT @current_login = SYSTEM_USER 
IF @service_login <> 'sa' 
BEGIN 
IF UPPER(@service_login) <> UPPER(@current_login) 
		 BEGIN 
		 SET @sql = N'CREATE USER[cdc_service] FOR LOGIN [' + @service_login + '] ' + @EndLine + 
		 N'EXEC [sys].[sp_addrolemember] N''''db_owner'''', N''''cdc_service''''' 
		 SET @BigSQL = 'USE [' + @QuotedDbName + ']; EXEC sp_executesql N''' + @sql + ''''; 
		 PRINT @BigSQL 
		 EXEC (@BigSQL) 
		 END 
	 END 
END
GO

CREATE PROCEDURE [dbo].[xdbcdc_disable_db] 
( 	 @dbname NVARCHAR (128) 
) 
AS 
	 BEGIN 
IF EXISTS (SELECT 1 FROM [MSXDBCDC].[dbo].[xdbcdc_databases] WHERE [name] = @dbname) DELETE FROM [dbo].[xdbcdc_databases] 
WHERE [name] = @dbname 
END 
GO

CREATE PROCEDURE [dbo].[xdbcdc_reset_db] 
	 @dbname nvarchar(128), 
	 @Result bit output 
AS 
BEGIN 
BEGIN TRANSACTION 
/* Stop CDC instance */ 
UPDATE [dbo].[xdbcdc_databases] 
SET [enabled] = 0 
WHERE [name] = @dbname 
/* Truncate all capture tables */ 
DECLARE @OuterSql nvarchar(MAX) 
DECLARE @InnerSql nvarchar(MAX) 
DECLARE @EndLine nchar(2) 
SET @EndLine = NCHAR(13) + NCHAR(10) 
DECLARE @QuotedDbName NVARCHAR (256) 
SET @QuotedDbName = replace(@dbname, N']', N']]') 
SET @InnerSql = 
N' USE [' + @QuotedDbName + '];' + @EndLine + 
N' DECLARE @Owner nvarchar(128) ' + @EndLine + 
N' DECLARE @TableName nvarchar(128) ' + @EndLine + 
N' DECLARE @RowNum int ' + @EndLine + 
N' DECLARE @QuotedTableName nvarchar(256) ' + @EndLine + 
N' DECLARE @dynSQL nvarchar(300) ' + @EndLine + 
N' DECLARE instancesList CURSOR ' + @EndLine + 
N' FOR SELECT OBJECT_SCHEMA_NAME(object_id) AS Owner, OBJECT_NAME(object_id) AS TableName' + @EndLine + 
N' FROM cdc.change_tables ' + @EndLine + 
N' OPEN instancesList ' + @EndLine + 
N' FETCH NEXT FROM instancesList  ' + @EndLine + 
N' INTO @Owner, @TableName  ' + @EndLine + 
N' SET @RowNum = 0 ' + @EndLine + 
N' WHILE @@FETCH_STATUS = 0 ' + @EndLine + 
N' BEGIN ' + @EndLine + 
N' SET @RowNum = @RowNum + 1 ' + @EndLine + 
N' SET @QuotedTableName = replace(@TableName, N'']'', N'']]'') ' + @EndLine + 
N' SET @dynSQL = ''TRUNCATE TABLE ['' + @Owner + ''].['' + @QuotedTableName + '']''' + @EndLine + 
N' EXEC (@dynSQL) ' + @EndLine + 
N' FETCH NEXT FROM instancesList ' + @EndLine + 
N' INTO @Owner, @TableName  ' + @EndLine + 
N' END ' + @EndLine + 
N' CLOSE instancesList ' + @EndLine + 
N' DEALLOCATE instancesList ' 
PRINT @InnerSql 
EXEC (@InnerSql) 
SET @InnerSql = 
N' /* Reset start_lsn for all change_tables */ ' + @EndLine + 
N' UPDATE [cdc].[change_tables] ' + @EndLine + 
N' SET [start_lsn] = 0x00000000000000000000 ' + @EndLine + 
N' /* Truncate xdbcdc_state */ ' + @EndLine + 
N' TRUNCATE TABLE [cdc].[xdbcdc_state] ' + @EndLine + 
N' /* Truncate xdbcdc_staged_transactions*/ ' + @EndLine + 
N' TRUNCATE TABLE [cdc].[xdbcdc_staged_transactions] ' + @EndLine + 
N' /* Truncate ddl_history*/ ' + @EndLine + 
N' TRUNCATE TABLE cdc.ddl_history ' + @EndLine + 
N' /* Truncate lsn_time_mapping*/ ' + @EndLine + 
N' TRUNCATE TABLE cdc.lsn_time_mapping ' 
SET @OuterSql = 'USE [' + @QuotedDbName + ']; EXEC sp_executesql N''' + @InnerSql + ''''; 
PRINT @OuterSql 
EXEC (@OuterSql) 
IF @@error <> 0 
BEGIN 
	 ROLLBACK TRANSACTION 
	 SET @Result = 0 
	 RETURN (@Result) 
END 
COMMIT TRANSACTION 
/* RETURN SUCCESS  */ 
SET @Result = 1 
RETURN (@Result) 
END
GO

CREATE PROCEDURE [dbo].[xdbcdc_add_service] 
( 
	 @svcname NVARCHAR (128), 
	 @sqlusr NVARCHAR (128) 
) 
 AS 
BEGIN 
	 DECLARE @ref_count int 
	 SELECT @ref_count = count(ref_count) 
	 FROM [dbo].[xdbcdc_services] 
	 WHERE [cdc_service_name] = @svcname 
	 SET @ref_count = @ref_count +1 
	 IF @ref_count = 1 
	 BEGIN 
		 INSERT INTO [dbo].[xdbcdc_services](cdc_service_name, cdc_service_sql_login, active_service_node, active_service_heartbeat, ref_count, options) 
		 VALUES(@svcname, @sqlusr, NULL, NULL, @ref_count, NULL) 
	 END 
	 ELSE 
		 UPDATE [dbo].[xdbcdc_services] 
		 SET [ref_count] = @ref_count 
		 WHERE [cdc_service_name] = @svcname 
END
GO

CREATE PROCEDURE [dbo].[xdbcdc_remove_service] 
( 
	 @svcname NVARCHAR (128) 
) 
AS 
BEGIN 
	 DECLARE @ref_count int 
	 SELECT @ref_count = ref_count 
	 FROM [dbo].[xdbcdc_services] 
	 WHERE [cdc_service_name] = @svcname 
	 IF @ref_count = 1 
		 DELETE FROM [dbo].[xdbcdc_services] 
		  WHERE [cdc_service_name] = @svcname 
	 ELSE 
		 UPDATE [dbo].[xdbcdc_services] 
		 SET [ref_count] = @ref_count -1 
		 WHERE [cdc_service_name] = @svcname 
END
GO

CREATE PROCEDURE [dbo].[xdbcdc_start] 
( 
	 @dbname NVARCHAR (128) 
) 
AS 
BEGIN 
	 UPDATE [dbo].[xdbcdc_databases] 
	 SET [enabled] = 1 
	 WHERE [name] = @dbname 
END
GO

CREATE PROCEDURE [dbo].[xdbcdc_stop] 
( 
	 @dbname NVARCHAR (128) 
) 
AS 
BEGIN 
	 UPDATE [dbo].[xdbcdc_databases] 
	 SET [enabled] = 0 
	 WHERE [name] = @dbname 
END
GO

CREATE PROCEDURE [dbo].[xdbcdc_update_config_version] 
( 
	 @dbname NVARCHAR (128) 
) 
AS 
BEGIN 
	 UPDATE [dbo].[xdbcdc_databases] 
	 SET [config_version] = GETUTCDATE() 
	 WHERE [name] = @dbname 
END
GO
