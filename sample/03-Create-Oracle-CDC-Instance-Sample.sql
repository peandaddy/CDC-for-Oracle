/*
Create Oracle CDC Instance Sample

creates the change data database
	-- adjusted database file sizes and filegroup configuration
	-- used an appropriate collation
	-- set database recovery model
	-- changed the database owner to a specific server principal

This script creates an Oracle CDC instance in the [OraCDC] database. It sets various database properties and enables CDC for the database. It also creates a trigger [tr_after_di_cdc_change_tables] that updates the [cdc].[change_tables] table after an insert operation. Additionally, it enables CDC for the [OraCDC] database using the [sys.sp_cdc_enable_db] stored procedure.

The second script creates mirror tables and capture instances in the change data database. Mirror tables are tables in the SQL Server change data database that correspond one-to-one to the source tables in the Oracle database. Each mirror table has the same column names as the Oracle table and denies all DML permissions. It also enables CDC for the specified table using the [sys.sp_cdc_enable_table] stored procedure.

Note: Please replace the placeholders [<<Schema name>>], [<<ObjectName>>], [<<Schema_ObjectName>>], [<<Object Table name>>], [<<Oracle serverName>>], [<<Oracle Instance name>>], and [<<Oracle User Name>>] with the actual values before executing the script.
*/

USE [master]

CREATE DATABASE [OraCDC] COLLATE SQL_Latin1_General_CP1_CS_AS; 

ALTER DATABASE [OraCDC] SET COMPATIBILITY_LEVEL = 110 -- 120+ accordingly
ALTER DATABASE [OraCDC] SET ANSI_NULL_DEFAULT OFF 
ALTER DATABASE [OraCDC] SET ANSI_NULLS OFF 
ALTER DATABASE [OraCDC] SET ANSI_PADDING ON 
ALTER DATABASE [OraCDC] SET ANSI_WARNINGS OFF 
ALTER DATABASE [OraCDC] SET ARITHABORT OFF 
ALTER DATABASE [OraCDC] SET AUTO_CLOSE OFF 
ALTER DATABASE [OraCDC] SET AUTO_CREATE_STATISTICS ON 
ALTER DATABASE [OraCDC] SET AUTO_SHRINK OFF 
ALTER DATABASE [OraCDC] SET AUTO_UPDATE_STATISTICS ON 
ALTER DATABASE [OraCDC] SET CURSOR_CLOSE_ON_COMMIT OFF 
ALTER DATABASE [OraCDC] SET CURSOR_DEFAULT  GLOBAL 
ALTER DATABASE [OraCDC] SET CONCAT_NULL_YIELDS_NULL OFF 
ALTER DATABASE [OraCDC] SET NUMERIC_ROUNDABORT OFF 
ALTER DATABASE [OraCDC] SET QUOTED_IDENTIFIER OFF 
ALTER DATABASE [OraCDC] SET RECURSIVE_TRIGGERS OFF 
ALTER DATABASE [OraCDC] SET  DISABLE_BROKER 
ALTER DATABASE [OraCDC] SET AUTO_UPDATE_STATISTICS_ASYNC OFF 
ALTER DATABASE [OraCDC] SET DATE_CORRELATION_OPTIMIZATION OFF 
ALTER DATABASE [OraCDC] SET ALLOW_SNAPSHOT_ISOLATION OFF 
ALTER DATABASE [OraCDC] SET PARAMETERIZATION SIMPLE 
ALTER DATABASE [OraCDC] SET READ_COMMITTED_SNAPSHOT OFF 
ALTER DATABASE [OraCDC] SET  READ_WRITE 
ALTER DATABASE [OraCDC] SET RECOVERY FULL 
ALTER DATABASE [OraCDC] SET  MULTI_USER 
ALTER DATABASE [OraCDC] SET PAGE_VERIFY CHECKSUM

EXEC [OraCDC].sys.sp_addextendedproperty @name=N'Description', @value=N'Oracle CDC project xxxxxx' 

GO

USE [OraCDC]
EXEC sys.sp_cdc_enable_db
GO

CREATE TRIGGER [tr_after_di_cdc_change_tables] 
ON [cdc].[change_tables] AFTER INSERT AS 
BEGIN 
	 DECLARE @ID int 
	 DECLARE IDsList CURSOR 
	 FOR SELECT [object_id] FROM inserted 
	 OPEN IDsList 
	 FETCH NEXT FROM IDsList 
	 INTO @ID 
	 WHILE @@FETCH_STATUS = 0 
	 BEGIN 
		 UPDATE [cdc].[change_tables] 
		 SET [start_lsn] = 0x00000000000000000000 
		 WHERE [object_id] = @ID 
		 FETCH NEXT FROM IDsList 
		 INTO @ID 
	 END 
CLOSE IDsList 
DEALLOCATE IDsList 
END 
GO

EXEC [MSXDBCDC].[dbo].[xdbcdc_enable_db] @dbname =N'OraCDC', @svcname= N'OracleCDCService1' 

/*
Script 2
	The second script creates mirror tables and capture instances in the change data database. This is a good point to take a step back and describe some relevant details of SQL Oracle CDC architecture.

	Mirror tables are tables in the SQL Server change data database that correspond one-to-one to the source tables in the Oracle database. Each mirror table has exactly the same column names as the Oracle table, these columns are in the same order as in the Oracle table, and have data types that match the Oracle data types as closely as possible.

	Mirror tables will remain empty at all times. In fact, to ensure that, the generated deployment script denies all DML permissions on each mirror table.
	
*/

if schema_id(N'<<Schema name>>') IS NULL EXEC ('CREATE SCHEMA [<<Schema name>>]' )

IF NOT EXISTS (SELECT 1 FROM [sys].[tables] WHERE SCHEMA_NAME([schema_id]) = N'<<Schema>>' AND [name] = N'ObjectName') 
CREATE TABLE [<<Schema name>>].[ObjectName] ( 
[Column1] int,
[Column2] NVARCHAR(200),
[Column3....] DATETIME
);


DENY INSERT, UPDATE, DELETE ON [<<Schema name>>].[<<ObjectName>>] TO public 


IF NOT EXISTS(SELECT [name], [is_tracked_by_cdc] FROM [sys].[tables] WHERE object_id = object_id(N'<<Schema.ObjectName>>') AND [is_tracked_by_cdc] = 1) 
EXEC [sys].[sp_cdc_enable_table] 
@source_schema = N'<<Schema name>>', 
@source_name = N'<<Object Table name>>', 
@role_name = NULL, 
@capture_instance=N'<<Schema_ObjectName>>', 
@captured_column_list = N'[Column1],[Column2],[Column3],[Column4.....]'

/*
 drops the CDC capture job
*/
--:setvar OracleAccountPassword "<SENSITIVE>" 
--:setvar OracleConnectString "Provider=OraOLEDB.Oracle;Data Source=<<Oracle serverName>>/<<Oracle Instance name>>;" 

-- Wait for the CDC service to create the encryption key
WHILE NOT EXISTS (
                 SELECT 1
                 FROM sys.asymmetric_keys
                 WHERE name = 'xdbcdc_asym_key'
                 )
    WAITFOR DELAY '00:00:05';
 
UPDATE cdc.xdbcdc_config SET 
    connect_string = N'Provider=OraOLEDB.Oracle;Data Source=<<Oracle serverName>>/<<Oracle Instance name>>;', 
    use_windows_authentication = 0, 
    username =N'<<Oracle User Name>>', 
    password = ENCRYPTBYASYMKEY(ASYMKEY_ID('xdbcdc_asym_key'), N'$(OracleAccountPassword)'), --Change your PSW
    transaction_staging_timeout = 120, 
    memory_limit= 50, 
    options= N'cdc_stop_on_breaking_schema_changes=1;trace=0;'
;

EXEC [sys].[sp_cdc_drop_job] @job_type = N'capture'

UPDATE [cdc].[xdbcdc_config] 
SET connect_string = N'Provider=OraOLEDB.Oracle;Data Source=<<Oracle serverName>>/<<Oracle Instance name>>;', use_windows_authentication = 0, username =N'<<Oracle User Name>>', transaction_staging_timeout = 120, memory_limit= 50, options= N'' 

