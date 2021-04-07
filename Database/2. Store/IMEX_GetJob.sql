IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'IMEX_GetJob'
)
    DROP PROCEDURE IMEX_GetJob;
GO
CREATE PROCEDURE [dbo].[IMEX_GetJob]
AS
BEGIN

    SELECT DISTINCT
           Job
    FROM IMEX_JobProcess
    WHERE Status = 'N';

END;