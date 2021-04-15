IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'IMEX_GetJobTrackingProcess'
)
    DROP PROCEDURE IMEX_GetJobTrackingProcess;
GO
CREATE PROCEDURE [dbo].[IMEX_GetJobTrackingProcess]
    @Job INT,
    @Step INT
AS
BEGIN
    DECLARE @_ID INT = 0;
    DECLARE @_IsAgain INT = 0;

    UPDATE IMEX_JobProcess
    SET Status = 'P',
        LastModifiedDate = GETDATE()
    WHERE Job = @Job
          AND Step = @Step
          AND Status = 'N';

    INSERT INTO dbo.IMEX_JobTrackingProcess
    (
        Job,
        Step,
        Status,
        CreatedDate,
        LastModifiedDate,
        Message,
        IsAgain
    )
    VALUES
    (   @Job,      -- Job - int
        @Step,     -- Step - int
        'N',       -- Status - char(1)
        GETDATE(), -- CreatedDate - datetime
        NULL, N'', -- Message - nvarchar(max)
        NULL       -- IsAgain - bit
        );
END;