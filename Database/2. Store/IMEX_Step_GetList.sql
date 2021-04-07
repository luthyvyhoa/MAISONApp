IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'IMEX_Step_GetList'
)
    DROP PROCEDURE IMEX_Step_GetList;
GO
CREATE PROCEDURE [dbo].[IMEX_Step_GetList] @Job INT
AS
BEGIN

    UPDATE IMEX_JobProcess
    SET Status = 'P',
        LastModifiedDate = GETDATE()
    WHERE Job = @Job
          AND CONVERT(DATE, GETDATE()) = CONVERT(DATE, CreatedDate);

    SELECT *
    FROM IMEX_Step
    WHERE Job = @Job
    ORDER BY Step;

END;