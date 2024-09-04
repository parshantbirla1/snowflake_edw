
/*
USE ROLE DATA_ENG_TEST_RW_RL;
USE WAREHOUSE DATA_ENGINEER_TEST_WH;
USE DATABASE LANDING_TEST;
USE SCHEMA BLUECHERRY;
*/

/*
USE ROLE DATA_ENG_DEV_RW_RL;
USE WAREHOUSE DATA_ENGINEER_DEV_WH;
USE DATABASE LANDING_DEV;
USE SCHEMA BLUECHERRY;
*/

CREATE OR REPLACE PROCEDURE BLUECHERRY.SP_LOAD_BC_DATA_S3_TO_SF(ENV STRING) 
RETURNS VARCHAR(20000) 
LANGUAGE JAVASCRIPT EXECUTE AS CALLER 
AS 
'
    var qt = String.fromCharCode(39);
    var newline = String.fromCharCode(10);
    var outputtxt = '''';
    
    try      
    {      

//---------------------------------------------------------------------------------------------
//---- 1. ZZEUPCNR
//---------------------------------------------------------------------------------------------
        var sqlcmd = `COPY INTO LANDING_` + ENV + `.BLUECHERRY.ZZEUPCNR
                FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
                        t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
                        t.$21, t.$22, t.$23, t.$24, t.$25, t.$26, t.$27, t.$28, t.$29, t.$30,
                        t.$31, t.$32, t.$33, t.$34, t.$35, t.$36, t.$37, t.$38, t.$39, t.$40,
                        t.$41, t.$42, t.$43, t.$44, t.$45,
                        METADATA$FILENAME AS METADATA_FILENAME, 
                        METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
                        METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
                        METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
                            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, NULL AS ETL_ROW_UPDATED_TS, ''BLUECHERRY'' AS ETL_SOURCE_SYSTEM_CD
                        FROM @LANDING_` + ENV + `.BLUECHERRY.STAGE_S3_BLUECHERRY/
                        (file_format => ''LANDING_` + ENV + `.BLUECHERRY.STAGE_CSV_FORMAT'', PATTERN =>     ''year.*/dbo.zzeupcnr_2.*'')T) 
                        ON_ERROR=CONTINUE`;  

        var stmt = snowflake.createStatement({sqlText : sqlcmd});
        var res=stmt.execute(); 

        while (res.next()) {
            if( res.getColumnValue(1) == ''Copy executed with 0 files processed.'')
                { 
                outputtxt += "ZZEUPCNR -  No file to load"+ newline
                }
            else
                {
                outputtxt += "1. ZZEUPCNR Load Result - " + res.getColumnValue("file") + newline;
                outputtxt += "Status: " + res.getColumnValue("status") + newline;
                outputtxt += "Rows Parsed: " + res.getColumnValue("rows_parsed") + newline;
                outputtxt += "Rows Loaded: " + res.getColumnValue("rows_loaded") + newline;
                outputtxt += "Error Limit: " + res.getColumnValue("error_limit") + newline;
                outputtxt += "Errors Seen: " + res.getColumnValue("errors_seen") + newline;
                outputtxt += "First Error: " + res.getColumnValue("first_error") + newline;
                outputtxt += "First Error Line: " + res.getColumnValue("first_error_line") + newline;
                outputtxt += "First Error Character: " + res.getColumnValue("first_error_character") + newline;
                outputtxt += "First Error Column Name: " + res.getColumnValue("first_error_column_name") + newline;
                }
        }

//---------------------------------------------------------------------------------------------
//---- 2. ZZXBGTBL
//---------------------------------------------------------------------------------------------

        var sqlcmd = `COPY INTO LANDING_` + ENV + `.BLUECHERRY.ZZXBGTBL
                FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
                        t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
                        t.$21, t.$22, t.$23, t.$24, t.$25, t.$26, t.$27, t.$28, t.$29, t.$30,
                        t.$31, t.$32, t.$33, t.$34, t.$35, t.$36, t.$37, t.$38, t.$39, t.$40,
                        t.$41, t.$42, t.$43, t.$44, t.$45, t.$46, t.$47, t.$48, t.$49, t.$50,
                        t.$51, t.$52, t.$53, t.$54, t.$55, t.$56, t.$57, t.$58, t.$59, t.$60,
                        t.$61, t.$62, t.$63, t.$64, t.$65, t.$66, t.$67, t.$68, t.$69, t.$70,
                        t.$71, t.$72, t.$73, t.$74, t.$75, t.$76, t.$77, t.$78, t.$79, t.$80, 
                        t.$81, t.$82, t.$83, t.$84, t.$85, t.$86, t.$87, t.$88, t.$89, t.$90, 
                        t.$91, t.$92, t.$93, t.$94, t.$95, t.$96, t.$97, t.$98, t.$99, t.$100, 
                        t.$101, t.$102, t.$103, t.$104, t.$105, t.$106, t.$107, t.$108, t.$109, t.$110,
                        t.$111, t.$112, t.$113, t.$114, t.$115, t.$116, t.$117, t.$118, t.$119, t.$120,
                        t.$121, t.$122, t.$123, t.$124, t.$125, t.$126, t.$127, t.$128, t.$129, t.$130,
                        t.$131, t.$132, t.$133, t.$134, t.$135, t.$136, t.$137, t.$138, t.$139, t.$140,
                        t.$141, t.$142, t.$143, t.$144, t.$145, t.$146, t.$147, t.$148, t.$149, t.$150,      
                        t.$151, t.$152, t.$153, t.$154, t.$155, t.$156, t.$157, t.$158, t.$159, t.$160,      
                        t.$161, t.$162, t.$163, t.$164, t.$165, t.$166, t.$167, t.$168, t.$169, t.$170,      
                        t.$171, t.$172, t.$173, t.$174, t.$175, t.$176, t.$177, t.$178, t.$179, t.$180,      
                        t.$181, t.$182, t.$183, t.$184, t.$185, t.$186, t.$187, t.$188, t.$189, t.$190,      
                        t.$191, t.$192, t.$193, t.$194, t.$195, t.$196, t.$197, t.$198, t.$199, t.$200,      
                        t.$201, t.$202, t.$203, t.$204, t.$205, t.$206, t.$207, t.$208, t.$209, t.$210,
                        t.$211, t.$212, t.$213, t.$214, t.$215, t.$216, t.$217, t.$218, t.$219, t.$220,
                        t.$221, t.$222, t.$223, t.$224, t.$225, t.$226, t.$227, t.$228, t.$229, t.$230,
                        t.$231, t.$232, t.$233, t.$234, t.$235, t.$236, t.$237, t.$238, t.$239, t.$240,
                        t.$241, t.$242, t.$243, t.$244, t.$245, t.$246, t.$247, t.$248, t.$249, t.$250,
                        t.$251, t.$252, t.$253, t.$254, t.$255, t.$256, t.$257, t.$258, t.$259, t.$260,
                        t.$261, t.$262, t.$263, t.$264, t.$265, t.$266, t.$267, t.$268, t.$269, t.$270,
                        t.$271, t.$272, t.$273, t.$274, t.$275, t.$276,
                        METADATA$FILENAME AS METADATA_FILENAME, 
                        METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
                        METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
                        METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
                            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, NULL AS ETL_ROW_UPDATED_TS, ''BLUECHERRY'' AS ETL_SOURCE_SYSTEM_CD
                        FROM @LANDING_` + ENV + `.BLUECHERRY.STAGE_S3_BLUECHERRY/
                        (file_format => ''LANDING_` + ENV + `.BLUECHERRY.STAGE_CSV_FORMAT'', PATTERN =>     ''year.*/dbo.zzxbgtbl_2.*'')T) 
                        ON_ERROR=CONTINUE`;  

        var stmt = snowflake.createStatement({sqlText : sqlcmd});
        var res=stmt.execute(); 
        while (res.next()) {
            if( res.getColumnValue(1) == ''Copy executed with 0 files processed.'')
                { 
                outputtxt += "2. ZZXBGTBL - No file to load"+ newline
                }
            else
                {
                outputtxt += "2. ZZXBGTBL Load Result - " + res.getColumnValue("file") + newline;
                outputtxt += "Status: " + res.getColumnValue("status") + newline;
                outputtxt += "Rows Parsed: " + res.getColumnValue("rows_parsed") + newline;
                outputtxt += "Rows Loaded: " + res.getColumnValue("rows_loaded") + newline;
                outputtxt += "Error Limit: " + res.getColumnValue("error_limit") + newline;
                outputtxt += "Errors Seen: " + res.getColumnValue("errors_seen") + newline;
                outputtxt += "First Error: " + res.getColumnValue("first_error") + newline;
                outputtxt += "First Error Line: " + res.getColumnValue("first_error_line") + newline;
                outputtxt += "First Error Character: " + res.getColumnValue("first_error_character") + newline;
                outputtxt += "First Error Column Name: " + res.getColumnValue("first_error_column_name") + newline;
                }
        }

//---------------------------------------------------------------------------------------------
//---- 3. ZZXCNTYR
//---------------------------------------------------------------------------------------------

        var sqlcmd = `COPY INTO LANDING_` + ENV + `.BLUECHERRY.ZZXCNTYR
                FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
                        t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17,
                        METADATA$FILENAME AS METADATA_FILENAME, 
                        METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
                        METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
                        METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
                            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, NULL AS ETL_ROW_UPDATED_TS, ''BLUECHERRY'' AS ETL_SOURCE_SYSTEM_CD
                        FROM @LANDING_` + ENV + `.BLUECHERRY.STAGE_S3_BLUECHERRY/
                        (file_format => ''LANDING_` + ENV + `.BLUECHERRY.STAGE_CSV_FORMAT'', PATTERN =>     ''year.*/dbo.zzxcntyr_2.*'')T) 
                        ON_ERROR=CONTINUE`;  

        var stmt = snowflake.createStatement({sqlText : sqlcmd});
        var res=stmt.execute(); 
        while (res.next()) {
            if( res.getColumnValue(1) == ''Copy executed with 0 files processed.'')
                { 
                outputtxt += "3. ZZXCNTYR - No file to load"+ newline
                }
            else
                {
                outputtxt += "3. ZZXCNTYR Load Result - " + res.getColumnValue("file") + newline;
                outputtxt += "Status: " + res.getColumnValue("status") + newline;
                outputtxt += "Rows Parsed: " + res.getColumnValue("rows_parsed") + newline;
                outputtxt += "Rows Loaded: " + res.getColumnValue("rows_loaded") + newline;
                outputtxt += "Error Limit: " + res.getColumnValue("error_limit") + newline;
                outputtxt += "Errors Seen: " + res.getColumnValue("errors_seen") + newline;
                outputtxt += "First Error: " + res.getColumnValue("first_error") + newline;
                outputtxt += "First Error Line: " + res.getColumnValue("first_error_line") + newline;
                outputtxt += "First Error Character: " + res.getColumnValue("first_error_character") + newline;
                outputtxt += "First Error Column Name: " + res.getColumnValue("first_error_column_name") + newline;
                }
        }

//---------------------------------------------------------------------------------------------
//---- 4. ZZXCOLRR
//---------------------------------------------------------------------------------------------

        var sqlcmd = `COPY INTO LANDING_` + ENV + `.BLUECHERRY.ZZXCOLRR
                FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
                        t.$11, t.$12, t.$13, 
                        METADATA$FILENAME AS METADATA_FILENAME, 
                        METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
                        METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
                        METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
                            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, NULL AS ETL_ROW_UPDATED_TS, ''BLUECHERRY'' AS ETL_SOURCE_SYSTEM_CD
                        FROM @LANDING_` + ENV + `.BLUECHERRY.STAGE_S3_BLUECHERRY/
                        (file_format => ''LANDING_` + ENV + `.BLUECHERRY.STAGE_CSV_FORMAT'', PATTERN =>     ''year.*/dbo.zzxcolrr_2.*'')T) 
                        ON_ERROR=CONTINUE`;  

        var stmt = snowflake.createStatement({sqlText : sqlcmd});
        var res=stmt.execute(); 
        while (res.next()) {
            if( res.getColumnValue(1) == ''Copy executed with 0 files processed.'')
                { 
                outputtxt += "4. ZZXCOLRR - No file to load"+ newline
                }
            else
                {
                outputtxt += "4. ZZXCOLRR Load Result - " + res.getColumnValue("file") + newline;
                outputtxt += "Status: " + res.getColumnValue("status") + newline;
                outputtxt += "Rows Parsed: " + res.getColumnValue("rows_parsed") + newline;
                outputtxt += "Rows Loaded: " + res.getColumnValue("rows_loaded") + newline;
                outputtxt += "Error Limit: " + res.getColumnValue("error_limit") + newline;
                outputtxt += "Errors Seen: " + res.getColumnValue("errors_seen") + newline;
                outputtxt += "First Error: " + res.getColumnValue("first_error") + newline;
                outputtxt += "First Error Line: " + res.getColumnValue("first_error_line") + newline;
                outputtxt += "First Error Character: " + res.getColumnValue("first_error_character") + newline;
                outputtxt += "First Error Column Name: " + res.getColumnValue("first_error_column_name") + newline;
                }
        }

//---------------------------------------------------------------------------------------------
//---- 5. ZZXCONTR
//---------------------------------------------------------------------------------------------

        var sqlcmd = `COPY INTO LANDING_` + ENV + `.BLUECHERRY.ZZXCONTR
                    FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, 
                        METADATA$FILENAME AS METADATA_FILENAME, 
                        METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
                        METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
                        METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
                            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, NULL AS ETL_ROW_UPDATED_TS, ''BLUECHERRY'' AS ETL_SOURCE_SYSTEM_CD
                        FROM @LANDING_` + ENV + `.BLUECHERRY.STAGE_S3_BLUECHERRY/
                        (file_format => ''LANDING_` + ENV + `.BLUECHERRY.STAGE_CSV_FORMAT'', PATTERN =>     ''year.*/dbo.zzxcontr_2.*'')T) 
                        ON_ERROR=CONTINUE`;  

        var stmt = snowflake.createStatement({sqlText : sqlcmd});
        var res=stmt.execute(); 
        while (res.next()) {
            if( res.getColumnValue(1) == ''Copy executed with 0 files processed.'')
                { 
                outputtxt += "5. ZZXCONTR - No file to load"+ newline
                }
            else
                {
                outputtxt += "5. ZZXCONTR Load Result - " + res.getColumnValue("file") + newline;
                outputtxt += "Status: " + res.getColumnValue("status") + newline;
                outputtxt += "Rows Parsed: " + res.getColumnValue("rows_parsed") + newline;
                outputtxt += "Rows Loaded: " + res.getColumnValue("rows_loaded") + newline;
                outputtxt += "Error Limit: " + res.getColumnValue("error_limit") + newline;
                outputtxt += "Errors Seen: " + res.getColumnValue("errors_seen") + newline;
                outputtxt += "First Error: " + res.getColumnValue("first_error") + newline;
                outputtxt += "First Error Line: " + res.getColumnValue("first_error_line") + newline;
                outputtxt += "First Error Character: " + res.getColumnValue("first_error_character") + newline;
                outputtxt += "First Error Column Name: " + res.getColumnValue("first_error_column_name") + newline;
                }
        }

//---------------------------------------------------------------------------------------------
//---- 6. ZZXHSNOR
//---------------------------------------------------------------------------------------------

        var sqlcmd = `COPY INTO LANDING_` + ENV + `.BLUECHERRY.ZZXHSNOR
                FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
                        t.$11, t.$12, t.$13, t.$14, t.$15,
                        METADATA$FILENAME AS METADATA_FILENAME, 
                        METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
                        METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
                        METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
                            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, NULL AS ETL_ROW_UPDATED_TS, ''BLUECHERRY'' AS ETL_SOURCE_SYSTEM_CD
                        FROM @LANDING_` + ENV + `.BLUECHERRY.STAGE_S3_BLUECHERRY/
                        (file_format => ''LANDING_` + ENV + `.BLUECHERRY.STAGE_CSV_FORMAT'', PATTERN =>     ''year.*/dbo.zzxhsnor_2.*'')T) 
                        ON_ERROR=CONTINUE`;  

        var stmt = snowflake.createStatement({sqlText : sqlcmd});
        var res=stmt.execute(); 
        while (res.next()) {
            if( res.getColumnValue(1) == ''Copy executed with 0 files processed.'')
                { 
                outputtxt += "6. ZZXHSNOR - No file to load"+ newline
                }
            else
                {
                outputtxt += "6. ZZXHSNOR Load Result - " + res.getColumnValue("file") + newline;
                outputtxt += "Status: " + res.getColumnValue("status") + newline;
                outputtxt += "Rows Parsed: " + res.getColumnValue("rows_parsed") + newline;
                outputtxt += "Rows Loaded: " + res.getColumnValue("rows_loaded") + newline;
                outputtxt += "Error Limit: " + res.getColumnValue("error_limit") + newline;
                outputtxt += "Errors Seen: " + res.getColumnValue("errors_seen") + newline;
                outputtxt += "First Error: " + res.getColumnValue("first_error") + newline;
                outputtxt += "First Error Line: " + res.getColumnValue("first_error_line") + newline;
                outputtxt += "First Error Character: " + res.getColumnValue("first_error_character") + newline;
                outputtxt += "First Error Column Name: " + res.getColumnValue("first_error_column_name") + newline;
                }
        }

//---------------------------------------------------------------------------------------------
//---- 7. ZZXINTSD
//---------------------------------------------------------------------------------------------

        var sqlcmd = `COPY INTO LANDING_` + ENV + `.BLUECHERRY.ZZXINTSD
                FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
                        t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
                        t.$21, t.$22,
                        METADATA$FILENAME AS METADATA_FILENAME, 
                        METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
                        METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
                        METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
                            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, NULL AS ETL_ROW_UPDATED_TS, ''BLUECHERRY'' AS ETL_SOURCE_SYSTEM_CD
                        FROM @LANDING_` + ENV + `.BLUECHERRY.STAGE_S3_BLUECHERRY/
                        (file_format => ''LANDING_` + ENV + `.BLUECHERRY.STAGE_CSV_FORMAT'', PATTERN =>     ''year.*/dbo.zzxintsd_2.*'')T) 
                        ON_ERROR=CONTINUE`;  

        var stmt = snowflake.createStatement({sqlText : sqlcmd});
        var res=stmt.execute(); 
        while (res.next()) {
            if( res.getColumnValue(1) == ''Copy executed with 0 files processed.'')
                { 
                outputtxt += "7. ZZXINTSD - No file to load"+ newline
                }
            else
                {
                outputtxt += "7. ZZXINTSD Load Result - " + res.getColumnValue("file") + newline;
                outputtxt += "Status: " + res.getColumnValue("status") + newline;
                outputtxt += "Rows Parsed: " + res.getColumnValue("rows_parsed") + newline;
                outputtxt += "Rows Loaded: " + res.getColumnValue("rows_loaded") + newline;
                outputtxt += "Error Limit: " + res.getColumnValue("error_limit") + newline;
                outputtxt += "Errors Seen: " + res.getColumnValue("errors_seen") + newline;
                outputtxt += "First Error: " + res.getColumnValue("first_error") + newline;
                outputtxt += "First Error Line: " + res.getColumnValue("first_error_line") + newline;
                outputtxt += "First Error Character: " + res.getColumnValue("first_error_character") + newline;
                outputtxt += "First Error Column Name: " + res.getColumnValue("first_error_column_name") + newline;
                }
        }

//---------------------------------------------------------------------------------------------
//---- 8. ZZXPGRPR
//---------------------------------------------------------------------------------------------

        var sqlcmd = `COPY INTO LANDING_` + ENV + `.BLUECHERRY.ZZXPGRPR
                FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
                        t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
                        t.$21, 
                        METADATA$FILENAME AS METADATA_FILENAME, 
                        METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
                        METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
                        METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
                            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, NULL AS ETL_ROW_UPDATED_TS, ''BLUECHERRY'' AS ETL_SOURCE_SYSTEM_CD
                        FROM @LANDING_` + ENV + `.BLUECHERRY.STAGE_S3_BLUECHERRY/
                        (file_format => ''LANDING_` + ENV + `.BLUECHERRY.STAGE_CSV_FORMAT'', PATTERN =>     ''year.*/dbo.zzxpgrpr_2.*'')T) 
                        ON_ERROR=CONTINUE`;  

        var stmt = snowflake.createStatement({sqlText : sqlcmd});
        var res=stmt.execute(); 
        while (res.next()) {
            if( res.getColumnValue(1) == ''Copy executed with 0 files processed.'')
                { 
                outputtxt += "8. ZZXPGRPR - No file to load"+ newline
                }
            else
                {
                outputtxt += "8. ZZXPGRPR Load Result - " + res.getColumnValue("file") + newline;
                outputtxt += "Status: " + res.getColumnValue("status") + newline;
                outputtxt += "Rows Parsed: " + res.getColumnValue("rows_parsed") + newline;
                outputtxt += "Rows Loaded: " + res.getColumnValue("rows_loaded") + newline;
                outputtxt += "Error Limit: " + res.getColumnValue("error_limit") + newline;
                outputtxt += "Errors Seen: " + res.getColumnValue("errors_seen") + newline;
                outputtxt += "First Error: " + res.getColumnValue("first_error") + newline;
                outputtxt += "First Error Line: " + res.getColumnValue("first_error_line") + newline;
                outputtxt += "First Error Character: " + res.getColumnValue("first_error_character") + newline;
                outputtxt += "First Error Column Name: " + res.getColumnValue("first_error_column_name") + newline;
                }
        }

//---------------------------------------------------------------------------------------------
//---- 9. ZZXSCOLR
//---------------------------------------------------------------------------------------------

        var sqlcmd = `COPY INTO LANDING_` + ENV + `.BLUECHERRY.ZZXSCOLR
                FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
                        t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
                        t.$21, t.$22, t.$23, t.$24, t.$25, t.$26, t.$27, t.$28, t.$29, t.$30,
                        t.$31, t.$32, t.$33, t.$34, t.$35, t.$36, t.$37, t.$38, t.$39, t.$40,
                        t.$41, t.$42, t.$43, t.$44, t.$45, t.$46, t.$47, t.$48, t.$49, t.$50,
                        t.$51, t.$52, t.$53, t.$54, t.$55, t.$56, t.$57, t.$58, t.$59, t.$60,
                        t.$61, t.$62, t.$63, t.$64, t.$65, t.$66, t.$67, t.$68, t.$69, t.$70,
                        t.$71, t.$72, t.$73, t.$74, t.$75, t.$76, t.$77, t.$78, t.$79, t.$80, 
                        t.$81, t.$82, t.$83, t.$84, t.$85, t.$86, t.$87, t.$88, t.$89, t.$90, 
                        t.$91, t.$92, t.$93, t.$94, t.$95, t.$96, t.$97, t.$98, t.$99, t.$100, 
                        t.$101, t.$102, t.$103, t.$104, t.$105, t.$106, t.$107, t.$108, t.$109, t.$110,
                        t.$111, t.$112, t.$113, t.$114, t.$115, t.$116, t.$117, t.$118, t.$119, t.$120,
                        t.$121, t.$122, t.$123, t.$124, t.$125, t.$126, t.$127, t.$128, t.$129, t.$130,
                        t.$131, t.$132, t.$133, t.$134, t.$135, t.$136, t.$137, t.$138, t.$139, t.$140,
                        t.$141, t.$142, t.$143, t.$144, t.$145, t.$146, t.$147, t.$148, t.$149, t.$150,      
                        t.$151, t.$152, t.$153, t.$154, t.$155, t.$156, t.$157, t.$158, t.$159, t.$160,      
                        t.$161, t.$162, t.$163, t.$164, t.$165, t.$166, t.$167, t.$168, t.$169, t.$170,      
                        t.$171, t.$172, t.$173, t.$174, t.$175, t.$176, t.$177, t.$178, t.$179, t.$180,      
                        t.$181, t.$182, t.$183, t.$184, t.$185, t.$186, t.$187, t.$188, t.$189, t.$190,      
                        t.$191, t.$192, t.$193, t.$194, t.$195, t.$196, t.$197, t.$198, t.$199, t.$200,      
                        t.$201, t.$202, t.$203, t.$204, t.$205, t.$206, t.$207, t.$208, t.$209, t.$210,
                        t.$211, t.$212, t.$213, t.$214, t.$215, t.$216, t.$217, t.$218, t.$219, t.$220,
                        t.$221, t.$222, t.$223, t.$224, t.$225, t.$226, t.$227, t.$228, t.$229, t.$230,
                        t.$231, t.$232, t.$233, t.$234, t.$235, t.$236, t.$237, t.$238, t.$239, t.$240,
                        t.$241, t.$242, t.$243, t.$244, t.$245, t.$246, t.$247, t.$248, t.$249, t.$250,
                        t.$251, t.$252, t.$253, t.$254, t.$255, t.$256, t.$257, t.$258, t.$259, t.$260,
                        t.$261, t.$262, t.$263, t.$264, t.$265, t.$266, t.$267, t.$268, t.$269, t.$270,
                        t.$271, t.$272, t.$273, t.$274, t.$275, t.$276, t.$277, t.$278, t.$279, t.$280,
                        t.$281, t.$282, t.$283, t.$284, t.$285, t.$286, t.$287, t.$288, t.$289, t.$290,
                        t.$291, t.$292, t.$293, t.$294, t.$295, t.$296, t.$297, t.$298, t.$299, t.$300,
                        t.$301, t.$302,
                        METADATA$FILENAME AS METADATA_FILENAME, 
                        METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
                        METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
                        METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
                            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, NULL AS ETL_ROW_UPDATED_TS, ''BLUECHERRY'' AS ETL_SOURCE_SYSTEM_CD
                        FROM @LANDING_` + ENV + `.BLUECHERRY.STAGE_S3_BLUECHERRY/
                        (file_format => ''LANDING_` + ENV + `.BLUECHERRY.STAGE_CSV_FORMAT'', PATTERN =>     ''year.*/dbo.zzxscolr_2.*'')T) 
                        ON_ERROR=CONTINUE`;  

        var stmt = snowflake.createStatement({sqlText : sqlcmd});
        var res=stmt.execute(); 
        while (res.next()) {
            if( res.getColumnValue(1) == ''Copy executed with 0 files processed.'')
                { 
                outputtxt += "9. ZZXSCOLR - No file to load"+ newline
                }
            else
                {
                outputtxt += "9. ZZXSCOLR Load Result - " + res.getColumnValue("file") + newline;
                outputtxt += "Status: " + res.getColumnValue("status") + newline;
                outputtxt += "Rows Parsed: " + res.getColumnValue("rows_parsed") + newline;
                outputtxt += "Rows Loaded: " + res.getColumnValue("rows_loaded") + newline;
                outputtxt += "Error Limit: " + res.getColumnValue("error_limit") + newline;
                outputtxt += "Errors Seen: " + res.getColumnValue("errors_seen") + newline;
                outputtxt += "First Error: " + res.getColumnValue("first_error") + newline;
                outputtxt += "First Error Line: " + res.getColumnValue("first_error_line") + newline;
                outputtxt += "First Error Character: " + res.getColumnValue("first_error_character") + newline;
                outputtxt += "First Error Column Name: " + res.getColumnValue("first_error_column_name") + newline;
                }
        }

//---------------------------------------------------------------------------------------------
//---- 10. ZZXSTYLR
//---------------------------------------------------------------------------------------------

        var sqlcmd = `COPY INTO LANDING_` + ENV + `.BLUECHERRY.ZZXSTYLR
                FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
                        t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
                        t.$21, t.$22, t.$23, t.$24, t.$25, t.$26, t.$27, t.$28, t.$29, t.$30,
                        t.$31, t.$32, t.$33, t.$34, t.$35, t.$36, t.$37, t.$38, t.$39, t.$40,
                        t.$41, t.$42, t.$43, t.$44, t.$45, t.$46, t.$47, t.$48, t.$49, t.$50,
                        t.$51, t.$52, t.$53, t.$54, t.$55, t.$56, t.$57, t.$58, t.$59, t.$60,
                        t.$61, t.$62, t.$63, t.$64, t.$65, t.$66, t.$67, t.$68, t.$69, t.$70,
                        t.$71, t.$72, t.$73, t.$74, t.$75, t.$76, t.$77, t.$78, t.$79, t.$80, 
                        t.$81, t.$82, t.$83, t.$84, t.$85, t.$86, t.$87, t.$88, t.$89, t.$90, 
                        t.$91, t.$92, t.$93, t.$94, t.$95, t.$96, t.$97, t.$98, t.$99, t.$100, 
                        t.$101, t.$102, t.$103, t.$104, t.$105, t.$106, t.$107, t.$108, t.$109, t.$110,
                        t.$111, t.$112, t.$113, t.$114, t.$115, t.$116, t.$117, t.$118, t.$119, t.$120,
                        t.$121, t.$122, t.$123, t.$124, t.$125, t.$126, t.$127, t.$128, t.$129, t.$130,
                        t.$131, t.$132, t.$133, t.$134, t.$135, t.$136, t.$137, t.$138, t.$139, t.$140,
                        t.$141, t.$142, t.$143, t.$144, t.$145, t.$146, t.$147, t.$148, t.$149, t.$150,      
                        t.$151, t.$152, t.$153, t.$154, t.$155, t.$156, t.$157, t.$158, t.$159, t.$160,      
                        t.$161, t.$162, t.$163, t.$164, t.$165, t.$166, t.$167, t.$168, t.$169, t.$170,      
                        t.$171, t.$172, t.$173, t.$174, t.$175, t.$176, t.$177, t.$178, t.$179, t.$180,      
                        t.$181, t.$182, t.$183, t.$184, t.$185, t.$186, t.$187, t.$188, t.$189, t.$190,      
                        t.$191, t.$192, t.$193, t.$194, t.$195, t.$196, t.$197, t.$198, t.$199, t.$200,      
                        t.$201, t.$202, t.$203, t.$204, t.$205, t.$206, t.$207, t.$208, t.$209, t.$210,
                        t.$211, t.$212, t.$213, t.$214, t.$215, t.$216, t.$217, t.$218, t.$219, t.$220,
                        t.$221, t.$222, t.$223, t.$224, t.$225, t.$226, t.$227, t.$228, t.$229, t.$230,
                        t.$231, t.$232, t.$233, t.$234, t.$235, t.$236, t.$237, t.$238, t.$239,
                        METADATA$FILENAME AS METADATA_FILENAME, 
                        METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
                        METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
                        METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
                            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, NULL AS ETL_ROW_UPDATED_TS, ''BLUECHERRY'' AS ETL_SOURCE_SYSTEM_CD
                        FROM @LANDING_` + ENV + `.BLUECHERRY.STAGE_S3_BLUECHERRY/
                        (file_format => ''LANDING_` + ENV + `.BLUECHERRY.STAGE_CSV_FORMAT'', PATTERN =>     ''year.*/dbo.zzxstylr_2.*'')T) 
                        ON_ERROR=CONTINUE`;  

        var stmt = snowflake.createStatement({sqlText : sqlcmd});
        var res=stmt.execute(); 
        while (res.next()) {
            if( res.getColumnValue(1) == ''Copy executed with 0 files processed.'')
                { 
                outputtxt += "10. ZZXSTYLR - No file to load"+ newline
                }
            else
                {
                outputtxt += "10. ZZXSTYLR Load Result - " + res.getColumnValue("file") + newline;
                outputtxt += "Status: " + res.getColumnValue("status") + newline;
                outputtxt += "Rows Parsed: " + res.getColumnValue("rows_parsed") + newline;
                outputtxt += "Rows Loaded: " + res.getColumnValue("rows_loaded") + newline;
                outputtxt += "Error Limit: " + res.getColumnValue("error_limit") + newline;
                outputtxt += "Errors Seen: " + res.getColumnValue("errors_seen") + newline;
                outputtxt += "First Error: " + res.getColumnValue("first_error") + newline;
                outputtxt += "First Error Line: " + res.getColumnValue("first_error_line") + newline;
                outputtxt += "First Error Character: " + res.getColumnValue("first_error_character") + newline;
                outputtxt += "First Error Column Name: " + res.getColumnValue("first_error_column_name") + newline;
                }
        }

//---------------------------------------------------------------------------------------------
//---- 11. ZZXCUSTR
//---------------------------------------------------------------------------------------------

        var sqlcmd = `COPY INTO LANDING_` + ENV + `.BLUECHERRY.ZZXCUSTR
                FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
                        t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
                        t.$21, t.$22, t.$23, t.$24, t.$25, t.$26, t.$27, t.$28, t.$29, t.$30,
                        t.$31, t.$32, t.$33, t.$34, t.$35, t.$36, t.$37, t.$38, t.$39, t.$40,
                        t.$41, t.$42, t.$43, t.$44, t.$45, t.$46, t.$47, t.$48, t.$49, t.$50,
                        t.$51, t.$52, t.$53, t.$54, t.$55, t.$56, t.$57, t.$58, t.$59, t.$60,
                        t.$61, t.$62, t.$63, t.$64, t.$65, t.$66, t.$67, t.$68, t.$69, t.$70,
                        t.$71, t.$72, t.$73, t.$74, t.$75, t.$76, t.$77, t.$78, t.$79, t.$80, 
                        t.$81, t.$82, t.$83, t.$84, t.$85, t.$86, t.$87, t.$88, t.$89, t.$90, 
                        t.$91, t.$92, t.$93, t.$94, 
                        METADATA$FILENAME AS METADATA_FILENAME, 
                        METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
                        METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
                        METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
                            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, NULL AS ETL_ROW_UPDATED_TS, ''BLUECHERRY'' AS ETL_SOURCE_SYSTEM_CD
                        FROM @LANDING_` + ENV + `.BLUECHERRY.STAGE_S3_BLUECHERRY/
                        (file_format => ''LANDING_` + ENV + `.BLUECHERRY.STAGE_CSV_FORMAT'', PATTERN =>     ''year.*/dbo.zzxcustr_2.*'')T) 
                        ON_ERROR=CONTINUE`;  

        var stmt = snowflake.createStatement({sqlText : sqlcmd});
        var res=stmt.execute(); 
        while (res.next()) {
            if( res.getColumnValue(1) == ''Copy executed with 0 files processed.'')
                { 
                outputtxt += "11. ZZXCUSTR - No file to load"+ newline
                }
            else
                {
                outputtxt += "11. ZZXCUSTR Load Result - " + res.getColumnValue("file") + newline;
                outputtxt += "Status: " + res.getColumnValue("status") + newline;
                outputtxt += "Rows Parsed: " + res.getColumnValue("rows_parsed") + newline;
                outputtxt += "Rows Loaded: " + res.getColumnValue("rows_loaded") + newline;
                outputtxt += "Error Limit: " + res.getColumnValue("error_limit") + newline;
                outputtxt += "Errors Seen: " + res.getColumnValue("errors_seen") + newline;
                outputtxt += "First Error: " + res.getColumnValue("first_error") + newline;
                outputtxt += "First Error Line: " + res.getColumnValue("first_error_line") + newline;
                outputtxt += "First Error Character: " + res.getColumnValue("first_error_character") + newline;
                outputtxt += "First Error Column Name: " + res.getColumnValue("first_error_column_name") + newline;
                }
        }

//---------------------------------------------------------------------------------------------
//---- 12. ZZXSTORR
//---------------------------------------------------------------------------------------------

        var sqlcmd = `COPY INTO LANDING_` + ENV + `.BLUECHERRY.ZZXSTORR
                FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
                        t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
                        t.$21, t.$22, t.$23, t.$24, t.$25, t.$26, t.$27, t.$28, t.$29, t.$30,
                        t.$31, t.$32, t.$33, t.$34, t.$35, t.$36, t.$37, t.$38, t.$39, t.$40,
                        t.$41, t.$42, t.$43, t.$44, t.$45, t.$46, t.$47, t.$48, t.$49, t.$50,
                        t.$51, t.$52, t.$53, t.$54, t.$55,
                        METADATA$FILENAME AS METADATA_FILENAME, 
                        METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
                        METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
                        METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
                            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, NULL AS ETL_ROW_UPDATED_TS, ''BLUECHERRY'' AS ETL_SOURCE_SYSTEM_CD
                        FROM @LANDING_` + ENV + `.BLUECHERRY.STAGE_S3_BLUECHERRY/
                        (file_format => ''LANDING_` + ENV + `.BLUECHERRY.STAGE_CSV_FORMAT'', PATTERN =>     ''year.*/dbo.zzxstorr_2.*'')T) 
                        ON_ERROR=CONTINUE`;  

        var stmt = snowflake.createStatement({sqlText : sqlcmd});
        var res=stmt.execute(); 
        while (res.next()) {
            if( res.getColumnValue(1) == ''Copy executed with 0 files processed.'')
                { 
                outputtxt += "12. ZZXSTORR - No file to load"+ newline
                }
            else
                {
                outputtxt += "12. ZZXSTORR Load Result - " + res.getColumnValue("file") + newline;
                outputtxt += "Status: " + res.getColumnValue("status") + newline;
                outputtxt += "Rows Parsed: " + res.getColumnValue("rows_parsed") + newline;
                outputtxt += "Rows Loaded: " + res.getColumnValue("rows_loaded") + newline;
                outputtxt += "Error Limit: " + res.getColumnValue("error_limit") + newline;
                outputtxt += "Errors Seen: " + res.getColumnValue("errors_seen") + newline;
                outputtxt += "First Error: " + res.getColumnValue("first_error") + newline;
                outputtxt += "First Error Line: " + res.getColumnValue("first_error_line") + newline;
                outputtxt += "First Error Character: " + res.getColumnValue("first_error_character") + newline;
                outputtxt += "First Error Column Name: " + res.getColumnValue("first_error_column_name") + newline;
                }
        }

//---------------------------------------------------------------------------------------------
//---- 13. ZZOORDRH
//---------------------------------------------------------------------------------------------

        var sqlcmd = `COPY INTO LANDING_` + ENV + `.BLUECHERRY.ZZOORDRH
                FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
                        t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
                        t.$21, t.$22, t.$23, t.$24, t.$25, t.$26, t.$27, t.$28, t.$29, t.$30,
                        t.$31, t.$32, t.$33, t.$34, t.$35, t.$36, t.$37, t.$38, t.$39, t.$40,
                        t.$41, t.$42, t.$43, t.$44, t.$45, t.$46, t.$47, t.$48, t.$49, t.$50,
                        t.$51, t.$52, t.$53, t.$54, t.$55, t.$56, t.$57, t.$58, t.$59, t.$60,
                        t.$61, t.$62, t.$63, t.$64, t.$65, t.$66, t.$67, t.$68, t.$69, t.$70,
                        t.$71, t.$72, t.$73, t.$74, t.$75, t.$76, t.$77, t.$78, t.$79, t.$80, 
                        t.$81, t.$82, t.$83, t.$84, t.$85, t.$86, t.$87, t.$88, t.$89, t.$90, 
                        t.$91, t.$92, t.$93, t.$94, t.$95, t.$96, t.$97, t.$98, t.$99, t.$100, 
                        t.$101, t.$102, t.$103, t.$104, t.$105, t.$106, t.$107, t.$108, t.$109, t.$110,
                        t.$111, t.$112, t.$113, t.$114, t.$115, t.$116, t.$117, t.$118, t.$119, t.$120,
                        t.$121, t.$122, t.$123, t.$124, t.$125, t.$126, t.$127, t.$128, t.$129, t.$130,
                        t.$131, t.$132, t.$133, t.$134, t.$135, t.$136, t.$137, t.$138, t.$139, t.$140,
                        t.$141, t.$142, t.$143, t.$144, t.$145, t.$146, t.$147, t.$148, t.$149, t.$150,      
                        t.$151, t.$152, t.$153, t.$154, t.$155, t.$156, t.$157, t.$158, t.$159, t.$160,      
                        t.$161, t.$162, t.$163, t.$164, t.$165, t.$166, t.$167, t.$168, t.$169, t.$170,      
                        t.$171, t.$172, t.$173, t.$174, t.$175, t.$176, t.$177, t.$178, t.$179, t.$180,      
                        t.$181, t.$182, t.$183, t.$184, t.$185, t.$186, t.$187, t.$188, t.$189, t.$190,      
                        t.$191, t.$192, t.$193, t.$194, t.$195, t.$196, t.$197, t.$198, t.$199, t.$200,      
                        t.$201, t.$202, t.$203, t.$204, t.$205, t.$206, t.$207, t.$208, t.$209, t.$210,
                        t.$211, t.$212, t.$213, t.$214, t.$215, t.$216, t.$217, t.$218, t.$219, t.$220,
                        t.$221, t.$222, t.$223, t.$224, t.$225, t.$226, t.$227, t.$228, t.$229, t.$230,
                        t.$231, t.$232, t.$233, t.$234, t.$235, t.$236, t.$237, t.$238, t.$239, t.$240,
                        t.$241, t.$242, t.$243, t.$244, t.$245, t.$246, t.$247, t.$248, t.$249, t.$250,
                        t.$251, t.$252, t.$253, t.$254, t.$255, 
                        METADATA$FILENAME AS METADATA_FILENAME, 
                        METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
                        METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
                        METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
                            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, NULL AS ETL_ROW_UPDATED_TS, ''BLUECHERRY'' AS ETL_SOURCE_SYSTEM_CD
                        FROM @LANDING_` + ENV + `.BLUECHERRY.STAGE_S3_BLUECHERRY/
                        (file_format => ''LANDING_` + ENV + `.BLUECHERRY.STAGE_CSV_FORMAT'', PATTERN =>     ''year.*/dbo.zzoordrh_2.*'')T) 
                        ON_ERROR=CONTINUE`;  

        var stmt = snowflake.createStatement({sqlText : sqlcmd});
        var res=stmt.execute(); 
        while (res.next()) {
            if( res.getColumnValue(1) == ''Copy executed with 0 files processed.'')
                { 
                outputtxt += "13. ZZOORDRH - No file to load"+ newline
                }
            else
                {
                outputtxt += "13. ZZOORDRH Load Result - " + res.getColumnValue("file") + newline;
                outputtxt += "Status: " + res.getColumnValue("status") + newline;
                outputtxt += "Rows Parsed: " + res.getColumnValue("rows_parsed") + newline;
                outputtxt += "Rows Loaded: " + res.getColumnValue("rows_loaded") + newline;
                outputtxt += "Error Limit: " + res.getColumnValue("error_limit") + newline;
                outputtxt += "Errors Seen: " + res.getColumnValue("errors_seen") + newline;
                outputtxt += "First Error: " + res.getColumnValue("first_error") + newline;
                outputtxt += "First Error Line: " + res.getColumnValue("first_error_line") + newline;
                outputtxt += "First Error Character: " + res.getColumnValue("first_error_character") + newline;
                outputtxt += "First Error Column Name: " + res.getColumnValue("first_error_column_name") + newline;
                }
        }

//---------------------------------------------------------------------------------------------
//---- 14. ZZOORDRD
//---------------------------------------------------------------------------------------------

        var sqlcmd = `COPY INTO LANDING_` + ENV + `.BLUECHERRY.ZZOORDRD
                FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
                        t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
                        t.$21, t.$22, t.$23, t.$24, t.$25, t.$26, t.$27, t.$28, t.$29, t.$30,
                        t.$31, t.$32, t.$33, t.$34, t.$35, t.$36, t.$37, t.$38, t.$39, t.$40,
                        t.$41, t.$42, t.$43, t.$44, t.$45, t.$46, t.$47, t.$48, t.$49, t.$50,
                        t.$51, t.$52, t.$53, t.$54, t.$55, t.$56, t.$57, t.$58, t.$59, t.$60,
                        t.$61, t.$62, t.$63, t.$64, t.$65, t.$66, t.$67, t.$68, t.$69, t.$70,
                        t.$71, t.$72, t.$73, t.$74, t.$75, t.$76, t.$77, t.$78, t.$79, t.$80, 
                        t.$81, t.$82, t.$83, t.$84, t.$85, t.$86, t.$87, t.$88, t.$89, t.$90, 
                        t.$91, t.$92, t.$93, t.$94, t.$95, t.$96, t.$97, t.$98, t.$99, t.$100, 
                        t.$101, t.$102, t.$103, t.$104, t.$105, t.$106, t.$107, t.$108, t.$109, t.$110,
                        t.$111, t.$112, t.$113, t.$114, t.$115, t.$116, t.$117, t.$118, t.$119, t.$120,
                        t.$121, t.$122, t.$123, t.$124, t.$125, t.$126, t.$127, t.$128, t.$129, t.$130,
                        t.$131, t.$132, t.$133, t.$134, t.$135, t.$136, t.$137, t.$138, t.$139, t.$140,
                        t.$141, t.$142, t.$143, t.$144, t.$145, t.$146, t.$147, t.$148, t.$149, t.$150,      
                        t.$151, t.$152, t.$153, t.$154, t.$155, t.$156, t.$157, t.$158, t.$159, t.$160,      
                        t.$161, t.$162, t.$163, t.$164, t.$165, t.$166, t.$167, t.$168, t.$169, t.$170,      
                        t.$171, t.$172, t.$173, t.$174, t.$175, t.$176, t.$177, t.$178, t.$179, t.$180,      
                        t.$181, t.$182, t.$183, t.$184, t.$185, t.$186, t.$187, t.$188, t.$189, t.$190,      
                        t.$191, t.$192, t.$193, t.$194, t.$195, t.$196, t.$197, t.$198, t.$199, t.$200,      
                        t.$201, t.$202, t.$203, t.$204, t.$205, t.$206, t.$207, t.$208, t.$209, t.$210,
                        t.$211, t.$212, t.$213, t.$214, t.$215, t.$216, t.$217, t.$218, t.$219, t.$220,
                        t.$221, t.$222, t.$223, t.$224, t.$225, t.$226, t.$227, t.$228, t.$229, t.$230,
                        t.$231, t.$232, t.$233, t.$234, t.$235, t.$236, t.$237, t.$238, t.$239, t.$240,
                        t.$241, t.$242, t.$243, t.$244, t.$245, t.$246, t.$247, t.$248, t.$249, t.$250,
                        t.$251, t.$252, t.$253, t.$254, t.$255, 
                        METADATA$FILENAME AS METADATA_FILENAME, 
                        METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
                        METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
                        METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
                            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, NULL AS ETL_ROW_UPDATED_TS, ''BLUECHERRY'' AS ETL_SOURCE_SYSTEM_CD
                        FROM @LANDING_` + ENV + `.BLUECHERRY.STAGE_S3_BLUECHERRY/
                        (file_format => ''LANDING_` + ENV + `.BLUECHERRY.STAGE_CSV_FORMAT'', PATTERN =>     ''year.*/dbo.zzoordrd_2.*'')T) 
                        ON_ERROR=CONTINUE`;  

        var stmt = snowflake.createStatement({sqlText : sqlcmd});
        var res=stmt.execute(); 
        while (res.next()) {
            if( res.getColumnValue(1) == ''Copy executed with 0 files processed.'')
                { 
                outputtxt += "14. ZZOORDRD - No file to load"+ newline
                }
            else
                {
                outputtxt += "14. ZZOORDRD Load Result - " + res.getColumnValue("file") + newline;
                outputtxt += "Status: " + res.getColumnValue("status") + newline;
                outputtxt += "Rows Parsed: " + res.getColumnValue("rows_parsed") + newline;
                outputtxt += "Rows Loaded: " + res.getColumnValue("rows_loaded") + newline;
                outputtxt += "Error Limit: " + res.getColumnValue("error_limit") + newline;
                outputtxt += "Errors Seen: " + res.getColumnValue("errors_seen") + newline;
                outputtxt += "First Error: " + res.getColumnValue("first_error") + newline;
                outputtxt += "First Error Line: " + res.getColumnValue("first_error_line") + newline;
                outputtxt += "First Error Character: " + res.getColumnValue("first_error_character") + newline;
                outputtxt += "First Error Column Name: " + res.getColumnValue("first_error_column_name") + newline;
                }
        }

//---------------------------------------------------------------------------------------------
//---- 15. ZVEUPCNR
//---------------------------------------------------------------------------------------------

        var sqlcmd = `COPY INTO LANDING_` + ENV + `.BLUECHERRY.ZVEUPCNR
                FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
                        t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
                        t.$21, t.$22, t.$23, t.$24, t.$25, t.$26, t.$27, t.$28, t.$29, t.$30,
                        t.$31, t.$32, t.$33, t.$34, t.$35, t.$36, t.$37, t.$38, t.$39, t.$40,
                        t.$41, t.$42, t.$43, t.$44, t.$45, 
                        METADATA$FILENAME AS METADATA_FILENAME, 
                        METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
                        METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
                        METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
                            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, NULL AS ETL_ROW_UPDATED_TS, ''BLUECHERRY'' AS ETL_SOURCE_SYSTEM_CD
                        FROM @LANDING_` + ENV + `.BLUECHERRY.STAGE_S3_BLUECHERRY/
                        (file_format => ''LANDING_` + ENV + `.BLUECHERRY.STAGE_CSV_FORMAT'', PATTERN =>     ''year.*/dbo.zveupcnr_2.*'')T) 
                        ON_ERROR=CONTINUE`;  

        var stmt = snowflake.createStatement({sqlText : sqlcmd});
        var res=stmt.execute(); 
        while (res.next()) {
            if( res.getColumnValue(1) == ''Copy executed with 0 files processed.'')
                { 
                outputtxt += "15. ZVEUPCNR - No file to load"+ newline
                }
            else
                {
                outputtxt += "15. ZVEUPCNR Load Result - " + res.getColumnValue("file") + newline;
                outputtxt += "Status: " + res.getColumnValue("status") + newline;
                outputtxt += "Rows Parsed: " + res.getColumnValue("rows_parsed") + newline;
                outputtxt += "Rows Loaded: " + res.getColumnValue("rows_loaded") + newline;
                outputtxt += "Error Limit: " + res.getColumnValue("error_limit") + newline;
                outputtxt += "Errors Seen: " + res.getColumnValue("errors_seen") + newline;
                outputtxt += "First Error: " + res.getColumnValue("first_error") + newline;
                outputtxt += "First Error Line: " + res.getColumnValue("first_error_line") + newline;
                outputtxt += "First Error Character: " + res.getColumnValue("first_error_character") + newline;
                outputtxt += "First Error Column Name: " + res.getColumnValue("first_error_column_name") + newline;
                }
        }

        
//---------------------------------------------------------------------------------------------
//---- 16. ZZXBUCKT
//---------------------------------------------------------------------------------------------

        var sqlcmd = `COPY INTO LANDING_` + ENV + `.BLUECHERRY.ZZXBUCKT
                FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
                        t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
                        t.$21, t.$22, t.$23, t.$24, t.$25, t.$26, t.$27, t.$28, t.$29, t.$30,
                        t.$31, t.$32, t.$33, t.$34, t.$35, t.$36, t.$37, t.$38, t.$39, t.$40,
                        t.$41, t.$42, t.$43, t.$44, t.$45, t.$46, t.$47, t.$48, t.$49, t.$50,
                        t.$51, 
                        METADATA$FILENAME AS METADATA_FILENAME, 
                        METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
                        METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
                        METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
                            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, NULL AS ETL_ROW_UPDATED_TS, ''BLUECHERRY'' AS ETL_SOURCE_SYSTEM_CD
                        FROM @LANDING_` + ENV + `.BLUECHERRY.STAGE_S3_BLUECHERRY/
                        (file_format => ''LANDING_` + ENV + `.BLUECHERRY.STAGE_CSV_FORMAT'', PATTERN =>     ''year.*/dbo.zzxbuckt_2.*'')T) 
                        ON_ERROR=CONTINUE`;  

        var stmt = snowflake.createStatement({sqlText : sqlcmd});
        var res=stmt.execute(); 
        while (res.next()) {
            if( res.getColumnValue(1) == ''Copy executed with 0 files processed.'')
                { 
                outputtxt += "16. ZZXBUCKT - No file to load"+ newline
                }
            else
                {
                outputtxt += "16. ZZXBUCKT Load Result - " + res.getColumnValue("file") + newline;
                outputtxt += "Status: " + res.getColumnValue("status") + newline;
                outputtxt += "Rows Parsed: " + res.getColumnValue("rows_parsed") + newline;
                outputtxt += "Rows Loaded: " + res.getColumnValue("rows_loaded") + newline;
                outputtxt += "Error Limit: " + res.getColumnValue("error_limit") + newline;
                outputtxt += "Errors Seen: " + res.getColumnValue("errors_seen") + newline;
                outputtxt += "First Error: " + res.getColumnValue("first_error") + newline;
                outputtxt += "First Error Line: " + res.getColumnValue("first_error_line") + newline;
                outputtxt += "First Error Character: " + res.getColumnValue("first_error_character") + newline;
                outputtxt += "First Error Column Name: " + res.getColumnValue("first_error_column_name") + newline;
                }
        }

        
//---------------------------------------------------------------------------------------------
//---- 17. ZZXSIZER
//---------------------------------------------------------------------------------------------

        var sqlcmd = `COPY INTO LANDING_` + ENV + `.BLUECHERRY.ZZXSIZER
                FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
                        t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
                        t.$21, t.$22, t.$23, t.$24, t.$25, t.$26, t.$27, t.$28, t.$29, t.$30,
                        t.$31, t.$32, t.$33, t.$34, t.$35, t.$36, t.$37, t.$38, t.$39, t.$40,
                        t.$41, t.$42, t.$43, t.$44, t.$45, t.$46, t.$47, t.$48, t.$49, t.$50,
                        t.$51, t.$52, t.$53, t.$54, t.$55, t.$56, t.$57, t.$58, t.$59, t.$60,
                        t.$61, t.$62, t.$63, 
                        METADATA$FILENAME AS METADATA_FILENAME, 
                        METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
                        METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
                        METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
                            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, NULL AS ETL_ROW_UPDATED_TS, ''BLUECHERRY'' AS ETL_SOURCE_SYSTEM_CD
                        FROM @LANDING_` + ENV + `.BLUECHERRY.STAGE_S3_BLUECHERRY/
                        (file_format => ''LANDING_` + ENV + `.BLUECHERRY.STAGE_CSV_FORMAT'', PATTERN =>     ''year.*/dbo.zzxsizer_2.*'')T) 
                        ON_ERROR=CONTINUE`;  

        var stmt = snowflake.createStatement({sqlText : sqlcmd});
        var res=stmt.execute(); 
        while (res.next()) {
            if( res.getColumnValue(1) == ''Copy executed with 0 files processed.'')
                { 
                outputtxt += "17. ZZXSIZER - No file to load"+ newline
                }
            else
                {
                outputtxt += "17. ZZXSIZER Load Result - " + res.getColumnValue("file") + newline;
                outputtxt += "Status: " + res.getColumnValue("status") + newline;
                outputtxt += "Rows Parsed: " + res.getColumnValue("rows_parsed") + newline;
                outputtxt += "Rows Loaded: " + res.getColumnValue("rows_loaded") + newline;
                outputtxt += "Error Limit: " + res.getColumnValue("error_limit") + newline;
                outputtxt += "Errors Seen: " + res.getColumnValue("errors_seen") + newline;
                outputtxt += "First Error: " + res.getColumnValue("first_error") + newline;
                outputtxt += "First Error Line: " + res.getColumnValue("first_error_line") + newline;
                outputtxt += "First Error Character: " + res.getColumnValue("first_error_character") + newline;
                outputtxt += "First Error Column Name: " + res.getColumnValue("first_error_column_name") + newline;
                }
        }

//---------------------------------------------------------------------------------------------
//---- 18. ZZOSHPRD
//---------------------------------------------------------------------------------------------

        var sqlcmd = `COPY INTO LANDING_` + ENV + `.BLUECHERRY.ZZOSHPRD
                FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
                        t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
                        t.$21, t.$22, t.$23, t.$24, t.$25, t.$26, t.$27, t.$28, t.$29, t.$30,
                        t.$31, t.$32, t.$33, t.$34, t.$35, t.$36, t.$37, t.$38, t.$39, t.$40,
                        t.$41, t.$42, t.$43, t.$44, t.$45, t.$46, t.$47, t.$48, t.$49, t.$50,
                        t.$51, t.$52, t.$53, t.$54, t.$55, t.$56, t.$57, t.$58, t.$59, t.$60,
                        t.$61, t.$62, t.$63, t.$64, t.$65, t.$66, t.$67, t.$68, t.$69, t.$70,
                        t.$71, t.$72, t.$73, t.$74, t.$75, t.$76, t.$77, t.$78, t.$79, t.$80, 
                        t.$81, t.$82, t.$83, t.$84, t.$85, t.$86, t.$87, t.$88, t.$89, t.$90, 
                        t.$91, t.$92, t.$93, t.$94, t.$95, t.$96, t.$97, t.$98, t.$99, t.$100, 
                        t.$101, t.$102, t.$103, t.$104, t.$105, t.$106, t.$107, t.$108, t.$109, t.$110,
                        t.$111, t.$112, t.$113, t.$114, t.$115, t.$116, t.$117, t.$118, t.$119, t.$120,
                        t.$121, t.$122, t.$123, t.$124, t.$125, t.$126, t.$127, t.$128, t.$129, t.$130,
                        t.$131, t.$132, t.$133, t.$134, t.$135, t.$136, t.$137, t.$138, t.$139, t.$140,
                        t.$141, t.$142, t.$143, t.$144, t.$145, t.$146, t.$147, t.$148, t.$149, t.$150,      
                        t.$151, t.$152, t.$153, t.$154, t.$155, t.$156, t.$157, t.$158, t.$159, t.$160,      
                        t.$161, t.$162, t.$163, t.$164, t.$165, t.$166, t.$167, t.$168, t.$169, t.$170,      
                        t.$171, t.$172, t.$173, t.$174, t.$175, t.$176, t.$177, t.$178, t.$179, t.$180,      
                        t.$181, t.$182, t.$183, t.$184, t.$185, t.$186, t.$187, t.$188, t.$189, t.$190,      
                        t.$191, t.$192, t.$193, t.$194, t.$195, t.$196, t.$197, t.$198, t.$199, t.$200,      
                        t.$201, t.$202, t.$203, t.$204, t.$205, t.$206, t.$207, t.$208, t.$209, t.$210,
                        t.$211, t.$212, t.$213, t.$214, t.$215, t.$216, t.$217, t.$218, t.$219, t.$220,
                        t.$221, t.$222, t.$223, t.$224, t.$225, t.$226, t.$227, t.$228, t.$229, t.$230,
                        t.$231, t.$232, t.$233, t.$234, t.$235, t.$236, t.$237, t.$238, t.$239, t.$240,
                        t.$241, t.$242, t.$243, t.$244, t.$245, t.$246, t.$247, t.$248, t.$249, t.$250,
                        t.$251, t.$252, t.$253, t.$254, t.$255, 
                        METADATA$FILENAME AS METADATA_FILENAME, 
                        METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
                        METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
                        METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
                            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, NULL AS ETL_ROW_UPDATED_TS, ''BLUECHERRY'' AS ETL_SOURCE_SYSTEM_CD
                        FROM @LANDING_` + ENV + `.BLUECHERRY.STAGE_S3_BLUECHERRY/
                        (file_format => ''LANDING_` + ENV + `.BLUECHERRY.STAGE_CSV_FORMAT'', PATTERN =>     ''year.*/dbo.zzoshprd_2.*'')T) 
                        ON_ERROR=CONTINUE`;  

        var stmt = snowflake.createStatement({sqlText : sqlcmd});
        var res=stmt.execute(); 
        while (res.next()) {
            if( res.getColumnValue(1) == ''Copy executed with 0 files processed.'')
                { 
                outputtxt += "18. ZZOSHPRD - No file to load"+ newline
                }
            else
                {
                outputtxt += "18. ZZOSHPRD Load Result - " + res.getColumnValue("file") + newline;
                outputtxt += "Status: " + res.getColumnValue("status") + newline;
                outputtxt += "Rows Parsed: " + res.getColumnValue("rows_parsed") + newline;
                outputtxt += "Rows Loaded: " + res.getColumnValue("rows_loaded") + newline;
                outputtxt += "Error Limit: " + res.getColumnValue("error_limit") + newline;
                outputtxt += "Errors Seen: " + res.getColumnValue("errors_seen") + newline;
                outputtxt += "First Error: " + res.getColumnValue("first_error") + newline;
                outputtxt += "First Error Line: " + res.getColumnValue("first_error_line") + newline;
                outputtxt += "First Error Character: " + res.getColumnValue("first_error_character") + newline;
                outputtxt += "First Error Column Name: " + res.getColumnValue("first_error_column_name") + newline;
                }
        }

//---------------------------------------------------------------------------------------------
//---- 19. ZZOSHPRH
//---------------------------------------------------------------------------------------------

        var sqlcmd = `COPY INTO LANDING_` + ENV + `.BLUECHERRY.ZZOSHPRH
                FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
                        t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
                        t.$21, t.$22, t.$23, t.$24, t.$25, t.$26, t.$27, t.$28, t.$29, t.$30,
                        t.$31, t.$32, t.$33, t.$34, t.$35, t.$36, t.$37, t.$38, t.$39, t.$40,
                        t.$41, t.$42, t.$43, t.$44, t.$45, t.$46, t.$47, t.$48, t.$49, t.$50,
                        t.$51, t.$52, t.$53, t.$54, t.$55, t.$56, t.$57, t.$58, t.$59, t.$60,
                        t.$61, t.$62, t.$63, t.$64, t.$65, t.$66, t.$67, t.$68, t.$69, t.$70,
                        t.$71, t.$72, t.$73, t.$74, t.$75, t.$76, t.$77, t.$78, t.$79, t.$80, 
                        t.$81, t.$82, t.$83, t.$84, t.$85, t.$86, t.$87, t.$88, t.$89, t.$90, 
                        t.$91, t.$92, t.$93, t.$94, t.$95, t.$96, t.$97, t.$98, t.$99, t.$100, 
                        t.$101, t.$102, t.$103, t.$104, t.$105, t.$106, t.$107, t.$108, t.$109, t.$110,
                        t.$111, t.$112, t.$113, t.$114, t.$115, t.$116, t.$117, t.$118, t.$119, t.$120,
                        t.$121, t.$122, t.$123, t.$124, t.$125, t.$126, t.$127, t.$128, t.$129, t.$130,
                        t.$131, t.$132, t.$133, t.$134, t.$135, t.$136, t.$137, t.$138, t.$139, t.$140,
                        t.$141, t.$142, t.$143, t.$144, t.$145, t.$146, t.$147, t.$148, t.$149, t.$150,      
                        t.$151, t.$152, t.$153, t.$154, t.$155, t.$156, t.$157, t.$158, t.$159, t.$160,      
                        t.$161, t.$162, t.$163, t.$164, t.$165, t.$166, t.$167, t.$168, t.$169, t.$170,      
                        t.$171, t.$172, t.$173, t.$174, t.$175, t.$176, t.$177, t.$178, t.$179, t.$180,      
                        t.$181, t.$182, t.$183, t.$184, t.$185, t.$186, t.$187, t.$188, t.$189, t.$190,      
                        t.$191, t.$192, t.$193, t.$194, t.$195, t.$196, t.$197, t.$198, t.$199, t.$200,      
                        t.$201, t.$202, t.$203, t.$204, t.$205, t.$206, t.$207, t.$208, t.$209, t.$210,
                        t.$211, t.$212, t.$213, t.$214, t.$215, t.$216, t.$217, t.$218, t.$219, t.$220,
                        t.$221, t.$222, t.$223, t.$224, t.$225, t.$226, t.$227, t.$228, t.$229, t.$230,
                        t.$231, t.$232, t.$233, t.$234, t.$235, t.$236, t.$237, t.$238, t.$239, t.$240,
                        t.$241, t.$242, t.$243, t.$244, t.$245, t.$246, t.$247, t.$248, t.$249, t.$250,
                        t.$251, t.$252, t.$253, t.$254, t.$255, 
                        METADATA$FILENAME AS METADATA_FILENAME, 
                        METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
                        METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
                        METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
                            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, NULL AS ETL_ROW_UPDATED_TS, ''BLUECHERRY'' AS ETL_SOURCE_SYSTEM_CD
                        FROM @LANDING_` + ENV + `.BLUECHERRY.STAGE_S3_BLUECHERRY/
                        (file_format => ''LANDING_` + ENV + `.BLUECHERRY.STAGE_CSV_FORMAT'', PATTERN =>     ''year.*/dbo.zzoshprh_2.*'')T) 
                        ON_ERROR=CONTINUE`;  

        var stmt = snowflake.createStatement({sqlText : sqlcmd});
        var res=stmt.execute(); 
        while (res.next()) {
            if( res.getColumnValue(1) == ''Copy executed with 0 files processed.'')
                { 
                outputtxt += "19. ZZOSHPRH - No file to load"+ newline
                }
            else
                {
                outputtxt += "19. ZZOSHPRH Load Result - " + res.getColumnValue("file") + newline;
                outputtxt += "Status: " + res.getColumnValue("status") + newline;
                outputtxt += "Rows Parsed: " + res.getColumnValue("rows_parsed") + newline;
                outputtxt += "Rows Loaded: " + res.getColumnValue("rows_loaded") + newline;
                outputtxt += "Error Limit: " + res.getColumnValue("error_limit") + newline;
                outputtxt += "Errors Seen: " + res.getColumnValue("errors_seen") + newline;
                outputtxt += "First Error: " + res.getColumnValue("first_error") + newline;
                outputtxt += "First Error Line: " + res.getColumnValue("first_error_line") + newline;
                outputtxt += "First Error Character: " + res.getColumnValue("first_error_character") + newline;
                outputtxt += "First Error Column Name: " + res.getColumnValue("first_error_column_name") + newline;
                }
        }
        
//---------------------------------------------------------------------------------------------
//---- 20. ZZXDISCR
//---------------------------------------------------------------------------------------------

        var sqlcmd = `COPY INTO LANDING_` + ENV + `.BLUECHERRY.ZZXDISCR
                FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, 
                        METADATA$FILENAME AS METADATA_FILENAME, 
                        METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
                        METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
                        METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
                            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, NULL AS ETL_ROW_UPDATED_TS, ''BLUECHERRY'' AS ETL_SOURCE_SYSTEM_CD
                        FROM @LANDING_` + ENV + `.BLUECHERRY.STAGE_S3_BLUECHERRY/
                        (file_format => ''LANDING_` + ENV + `.BLUECHERRY.STAGE_CSV_FORMAT'', PATTERN =>     ''year.*/dbo.zzxdiscr_2.*'')T) 
                        ON_ERROR=CONTINUE`;  

        var stmt = snowflake.createStatement({sqlText : sqlcmd});
        var res=stmt.execute(); 
        while (res.next()) {
            if( res.getColumnValue(1) == ''Copy executed with 0 files processed.'')
                { 
                outputtxt += "20. ZZXDISCR - No file to load"+ newline
                }
            else
                {
                outputtxt += "20. ZZXDISCR Load Result - " + res.getColumnValue("file") + newline;
                outputtxt += "Status: " + res.getColumnValue("status") + newline;
                outputtxt += "Rows Parsed: " + res.getColumnValue("rows_parsed") + newline;
                outputtxt += "Rows Loaded: " + res.getColumnValue("rows_loaded") + newline;
                outputtxt += "Error Limit: " + res.getColumnValue("error_limit") + newline;
                outputtxt += "Errors Seen: " + res.getColumnValue("errors_seen") + newline;
                outputtxt += "First Error: " + res.getColumnValue("first_error") + newline;
                outputtxt += "First Error Line: " + res.getColumnValue("first_error_line") + newline;
                outputtxt += "First Error Character: " + res.getColumnValue("first_error_character") + newline;
                outputtxt += "First Error Column Name: " + res.getColumnValue("first_error_column_name") + newline;
                }
        }

//---------------------------------------------------------------------------------------------
//---- 21. ZZNRETND
//---------------------------------------------------------------------------------------------

        var sqlcmd = `COPY INTO LANDING_` + ENV + `.BLUECHERRY.ZZNRETND
                FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
                        t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
                        t.$21, t.$22, t.$23, t.$24, t.$25, t.$26, t.$27, t.$28, t.$29, t.$30,
                        t.$31, t.$32, t.$33, t.$34, t.$35, t.$36, t.$37, t.$38, t.$39, t.$40,
                        t.$41, t.$42, t.$43, t.$44, t.$45, t.$46, t.$47, t.$48, t.$49, t.$50,
                        t.$51, t.$52, t.$53, t.$54, t.$55, t.$56, t.$57, t.$58, t.$59, t.$60,
                        t.$61, t.$62, t.$63, t.$64, t.$65, t.$66, t.$67, t.$68, t.$69, t.$70,
                        t.$71, t.$72, t.$73, t.$74, t.$75, t.$76, t.$77, t.$78, t.$79, t.$80, 
                        t.$81, t.$82, t.$83, t.$84, t.$85, t.$86, t.$87, t.$88, t.$89, t.$90, 
                        t.$91, t.$92, t.$93, t.$94, t.$95, t.$96, t.$97, t.$98, t.$99, t.$100, 
                        t.$101, t.$102, t.$103, t.$104, t.$105, t.$106, t.$107, t.$108, t.$109, t.$110,
                        t.$111, t.$112, t.$113, t.$114, t.$115, t.$116, t.$117, t.$118, t.$119, t.$120,
                        t.$121, t.$122, t.$123, t.$124, t.$125, t.$126, t.$127, t.$128, t.$129, t.$130,
                        t.$131, t.$132, t.$133, t.$134, t.$135, t.$136, t.$137, t.$138, t.$139, t.$140,
                        t.$141, t.$142, t.$143, t.$144, t.$145, t.$146, t.$147, t.$148, t.$149, t.$150,      
                        t.$151, t.$152, t.$153, t.$154, t.$155, t.$156, t.$157, t.$158, t.$159, t.$160,      
                        t.$161, t.$162, t.$163, t.$164, t.$165, t.$166, t.$167, t.$168, t.$169, t.$170,      
                        t.$171, t.$172, t.$173, t.$174, t.$175, t.$176, t.$177, t.$178, t.$179, t.$180,      
                        t.$181, t.$182, t.$183, t.$184, t.$185, t.$186, t.$187, t.$188, t.$189, t.$190,      
                        t.$191, t.$192, t.$193, t.$194, t.$195, t.$196, t.$197, t.$198, t.$199, t.$200,      
                        t.$201, t.$202, t.$203, t.$204, t.$205, t.$206,
                        METADATA$FILENAME AS METADATA_FILENAME, 
                        METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
                        METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
                        METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
                            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, NULL AS ETL_ROW_UPDATED_TS, ''BLUECHERRY'' AS ETL_SOURCE_SYSTEM_CD
                        FROM @LANDING_` + ENV + `.BLUECHERRY.STAGE_S3_BLUECHERRY/
                        (file_format => ''LANDING_` + ENV + `.BLUECHERRY.STAGE_CSV_FORMAT'', PATTERN =>     ''year.*/dbo.zzxdiscr_2.*'')T) 
                        ON_ERROR=CONTINUE`;  

        var stmt = snowflake.createStatement({sqlText : sqlcmd});
        var res=stmt.execute(); 
        while (res.next()) {
            if( res.getColumnValue(1) == ''Copy executed with 0 files processed.'')
                { 
                outputtxt += "21. ZZNRETND - No file to load"+ newline
                }
            else
                {
                outputtxt += "21. ZZNRETND Load Result - " + res.getColumnValue("file") + newline;
                outputtxt += "Status: " + res.getColumnValue("status") + newline;
                outputtxt += "Rows Parsed: " + res.getColumnValue("rows_parsed") + newline;
                outputtxt += "Rows Loaded: " + res.getColumnValue("rows_loaded") + newline;
                outputtxt += "Error Limit: " + res.getColumnValue("error_limit") + newline;
                outputtxt += "Errors Seen: " + res.getColumnValue("errors_seen") + newline;
                outputtxt += "First Error: " + res.getColumnValue("first_error") + newline;
                outputtxt += "First Error Line: " + res.getColumnValue("first_error_line") + newline;
                outputtxt += "First Error Character: " + res.getColumnValue("first_error_character") + newline;
                outputtxt += "First Error Column Name: " + res.getColumnValue("first_error_column_name") + newline;
                }
        }
        
//---------------------------------------------------------------------------------------------
//---- 22. ZZNRETNH
//---------------------------------------------------------------------------------------------

        var sqlcmd = `COPY INTO LANDING_` + ENV + `.BLUECHERRY.ZZNRETNH
                FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
                        t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
                        t.$21, t.$22, t.$23, t.$24, t.$25, t.$26, t.$27, t.$28, t.$29, t.$30,
                        t.$31, t.$32, t.$33, t.$34, t.$35, t.$36, t.$37, t.$38, t.$39, t.$40,
                        t.$41, t.$42, t.$43, t.$44, t.$45, t.$46, t.$47, t.$48, t.$49, t.$50,
                        t.$51, t.$52, t.$53, t.$54, t.$55, t.$56, t.$57, t.$58, t.$59, t.$60,
                        t.$61, t.$62, t.$63, t.$64, t.$65, t.$66, t.$67, t.$68, t.$69, t.$70,
                        t.$71, t.$72, t.$73, t.$74, t.$75, t.$76, t.$77, 
                        METADATA$FILENAME AS METADATA_FILENAME, 
                        METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
                        METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
                        METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
                            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, NULL AS ETL_ROW_UPDATED_TS, ''BLUECHERRY'' AS ETL_SOURCE_SYSTEM_CD
                        FROM @LANDING_` + ENV + `.BLUECHERRY.STAGE_S3_BLUECHERRY/
                        (file_format => ''LANDING_` + ENV + `.BLUECHERRY.STAGE_CSV_FORMAT'', PATTERN =>     ''year.*/dbo.zzxdiscr_2.*'')T) 
                        ON_ERROR=CONTINUE`;  

        var stmt = snowflake.createStatement({sqlText : sqlcmd});
        var res=stmt.execute(); 
        while (res.next()) {
            if( res.getColumnValue(1) == ''Copy executed with 0 files processed.'')
                { 
                outputtxt += "22. ZZNRETNH - No file to load"+ newline
                }
            else
                {
                outputtxt += "22. ZZNRETNH Load Result - " + res.getColumnValue("file") + newline;
                outputtxt += "Status: " + res.getColumnValue("status") + newline;
                outputtxt += "Rows Parsed: " + res.getColumnValue("rows_parsed") + newline;
                outputtxt += "Rows Loaded: " + res.getColumnValue("rows_loaded") + newline;
                outputtxt += "Error Limit: " + res.getColumnValue("error_limit") + newline;
                outputtxt += "Errors Seen: " + res.getColumnValue("errors_seen") + newline;
                outputtxt += "First Error: " + res.getColumnValue("first_error") + newline;
                outputtxt += "First Error Line: " + res.getColumnValue("first_error_line") + newline;
                outputtxt += "First Error Character: " + res.getColumnValue("first_error_character") + newline;
                outputtxt += "First Error Column Name: " + res.getColumnValue("first_error_column_name") + newline;
                }
        }

//---------------------------------------------------------------------------------------------
//---- 23. ZZXCOMMU
//---------------------------------------------------------------------------------------------

        var sqlcmd = `COPY INTO LANDING_` + ENV + `.BLUECHERRY.ZZXCOMMU
                FROM (SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8, t.$9, t.$10,
                        t.$11, t.$12, t.$13, t.$14, t.$15, t.$16, t.$17, t.$18, t.$19, t.$20,
                        t.$21, t.$22, t.$23, t.$24, t.$25, t.$26, t.$27, t.$28, t.$29, t.$30,
                        t.$31, t.$32, t.$33, t.$34, t.$35, t.$36, t.$37, t.$38, t.$39, t.$40,
                        t.$41, t.$42, t.$43, t.$44, t.$45, t.$46, t.$47, t.$48, t.$49, t.$50,
                        t.$51, t.$52, t.$53, t.$54, t.$55, t.$56, t.$57, t.$58, t.$59, t.$60,
                        t.$61, t.$62, t.$63, t.$64, t.$65, t.$66, t.$67, t.$68, t.$69, t.$70,
                        t.$71, t.$72, t.$73, t.$74, t.$75, t.$76, t.$77, 
                        METADATA$FILENAME AS METADATA_FILENAME, 
                        METADATA$FILE_ROW_NUMBER AS METADATA_FILE_ROW_NUMBER,
                        METADATA$FILE_LAST_MODIFIED AS METADATA_FILE_LAST_MODIFIED, 
                        METADATA$START_SCAN_TIME AS METADATA_START_SCAN_TIME,
                            CURRENT_TIMESTAMP AS ETL_ROW_CREATED_TS, NULL AS ETL_ROW_UPDATED_TS, ''BLUECHERRY'' AS ETL_SOURCE_SYSTEM_CD
                        FROM @LANDING_` + ENV + `.BLUECHERRY.STAGE_S3_BLUECHERRY/
                        (file_format => ''LANDING_` + ENV + `.BLUECHERRY.STAGE_CSV_FORMAT'', PATTERN =>     ''year.*/dbo.zzxcommu_2.*'')T) 
                        ON_ERROR=CONTINUE`;  

        var stmt = snowflake.createStatement({sqlText : sqlcmd});
        var res=stmt.execute(); 
        while (res.next()) {
            if( res.getColumnValue(1) == ''Copy executed with 0 files processed.'')
                { 
                outputtxt += "23. ZZXCOMMU - No file to load"+ newline
                }
            else
                {
                outputtxt += "23. ZZXCOMMU Load Result - " + res.getColumnValue("file") + newline;
                outputtxt += "Status: " + res.getColumnValue("status") + newline;
                outputtxt += "Rows Parsed: " + res.getColumnValue("rows_parsed") + newline;
                outputtxt += "Rows Loaded: " + res.getColumnValue("rows_loaded") + newline;
                outputtxt += "Error Limit: " + res.getColumnValue("error_limit") + newline;
                outputtxt += "Errors Seen: " + res.getColumnValue("errors_seen") + newline;
                outputtxt += "First Error: " + res.getColumnValue("first_error") + newline;
                outputtxt += "First Error Line: " + res.getColumnValue("first_error_line") + newline;
                outputtxt += "First Error Character: " + res.getColumnValue("first_error_character") + newline;
                outputtxt += "First Error Column Name: " + res.getColumnValue("first_error_column_name") + newline;
                }
        }

        return outputtxt;
    }  
    catch(err)  
    { 
        return "Procedure Failed: " + outputtxt + " and failure is " + err; 
    } 
';

--CALL BLUECHERRY.SP_LOAD_BC_DATA_S3_TO_SF('DEV');






