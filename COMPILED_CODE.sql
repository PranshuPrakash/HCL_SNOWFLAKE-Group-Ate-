
USE DATABASE SNOWFLAKE_LEARNING_DB;
USE SCHEMA PUBLIC;
CREATE OR REPLACE STAGE HCLSTAGE 
FILE_FORMAT = (TYPE = 'CSV');
CREATE OR REPLACE TABLE STG_CDR AS

WITH deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY CallID
               ORDER BY Timestamp DESC
           ) AS rn
    FROM CDR
)

SELECT 
    TRIM(CallID) AS CALLID,
    TRIM(Caller) AS CALLER,
    TRIM(Receiver) AS RECEIVER,
    COALESCE(Duration, 0) AS Duration,
    
    CallType,
    TowerID,
    Timestamp,

    COALESCE(Duration, 0) * 0.02 AS Revenue,
    UPPER(TRIM(CALLTYPE)) AS CALLTYPE,
    UPPER(TRIM(TOWERID)) AS TOWERID,
    

    CASE 
        WHEN CallType = 'INT' THEN 1
        ELSE 0

    END AS IsInternational
    
    
  

FROM deduped
WHERE rn = 1;

CREATE OR REPLACE TABLE ENRICHED_CDR AS

SELECT 
    stg.CallID,

    stg.Caller,
    c1.CustomerName AS CallerName,
    c1.PlanType AS CallerPlan,

    stg.Receiver,
    c2.CustomerName AS ReceiverName,
    c2.PlanType AS ReceiverPlan,

    stg.Duration,
    stg.Revenue,
    stg.IsInternational,

    stg.TowerID,
    t.Region,
    t.City,

    stg.Timestamp

FROM STG_CDR stg

LEFT JOIN CUSTOMER c1
    ON stg.Caller = c1.PhoneNumber

LEFT JOIN CUSTOMER c2
    ON stg.Receiver = c2.PhoneNumber

LEFT JOIN TOWER t
    ON stg.TowerID = t.TowerID;
select * from enriched_cdr;

CREATE OR REPLACE TABLE FACT_CDR (
    CDR_Key NUMBER AUTOINCREMENT,
    CallID STRING,
    Caller STRING,
    Receiver STRING,
    Duration NUMBER,
    Revenue NUMBER,
    IsInternational NUMBER,
    CallDate DATE
);

CREATE OR REPLACE TASK DAILY_LOAD
WAREHOUSE=COMPUTE_WH
SCHEDULE='USING CRON */600 * * * * UTC'
AS
MERGE INTO FACT_CDR tgt
USING (
    SELECT 
        CallID,
        Caller,
        Receiver,
        Duration,
        Revenue,
        IsInternational,
        CAST(Timestamp AS DATE) AS CallDate
    FROM ENRICHED_CDR
) src

ON tgt.CallID = src.CallID

WHEN MATCHED THEN UPDATE SET
    tgt.Duration = src.Duration,
    tgt.Revenue = src.Revenue,
    tgt.IsInternational = src.IsInternational,
    tgt.CallDate = src.CallDate

WHEN NOT MATCHED THEN INSERT (
    CallID,
    Caller,
    Receiver,
    Duration,
    Revenue,
    IsInternational,
    CallDate
)
VALUES (
    src.CallID,
    src.Caller,
    src.Receiver,
    src.Duration,
    src.Revenue,
    src.IsInternational,
    src.CallDate
);

CREATE OR REPLACE VIEW VW_CUSTOMER_USAGE AS

SELECT 
    Caller AS PhoneNumber,
    COUNT(CallID) AS TotalCalls,
    SUM(Duration) AS TotalDuration,
    SUM(Revenue) AS TotalRevenue,
    AVG(Duration) AS AvgCallDuration,
    SUM(IsInternational) AS InternationalCalls

FROM FACT_CDR
GROUP BY Caller;

SELECT * FROM VW_CUSTOMER_USAGE;

CREATE OR REPLACE TABLE HIGH_USAGE_ALERT AS

SELECT 
    Caller,
    SUM(Duration) AS TotalDuration
FROM FACT_CDR
GROUP BY Caller
HAVING SUM(Duration) > 200;



SELECT * FROM STG_CDR
WHERE LENGTH(Caller) <> 10 OR LENGTH(Receiver) <> 10;

Negative duration
SELECT * FROM STG_CDR
WHERE Duration < 0;

-- NULL callid
SELECT * FROM STG_CDR
WHERE CallID IS NULL;


CREATE OR REPLACE TABLE LOAD_AUDIT_LOG (
    LoadTime TIMESTAMP,
    TotalRecords NUMBER,
    SuccessfulRecords NUMBER,
    FailedRecords NUMBER,
    Status STRING
);


INSERT INTO LOAD_AUDIT_LOG
SELECT 
    CURRENT_TIMESTAMP(),
    COUNT(*) AS TotalRecords,

    SUM(CASE 
        WHEN CallID IS NOT NULL AND Duration >= 0 THEN 1 
        ELSE 0 
    END) AS SuccessfulRecords,

    SUM(CASE 
        WHEN CallID IS NULL OR Duration < 0 THEN 1 
        ELSE 0 
    END) AS FailedRecords,

    'SUCCESS' AS Status

FROM STG_CDR;
