IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'IMEX_DataFlow_GetList'
)
    DROP PROCEDURE IMEX_DataFlow_GetList;
GO
CREATE PROCEDURE [dbo].[IMEX_DataFlow_GetList]
    @Job INT,
    @Step INT
AS
BEGIN
	SELECT a.DataFlowID,
	       a.Job,
	       a.Step,
	       SourceConn = b.ConnectionString,
	       a.SourceData,
	       a.SourceSQL,
	       DestConn = c.ConnectionString,
	       a.DestData,
	       a.DestSQL,
	       a.NumOfCols
	FROM IMEX_DataFlow a
	    JOIN IMEX_Connection b
	        ON a.SourceConn = b.ConnectionID
	    JOIN IMEX_Connection c
	        ON a.DestConn = c.ConnectionID
	WHERE a.Job = @Job
	      AND a.Step = @Step
	      AND a.Inactive = 0;
END

GO

