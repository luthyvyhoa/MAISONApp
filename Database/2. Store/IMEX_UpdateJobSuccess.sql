IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'IMEX_UpdateJobSuccess'
)
    DROP PROCEDURE IMEX_UpdateJobSuccess;
GO
CREATE PROCEDURE [dbo].[IMEX_UpdateJobSuccess]
    @Job INT,
    @Step INT
AS
BEGIN
    UPDATE IMEX_JobProcess
    SET Status = 'S',
        LastModifiedDate = GETDATE()
    WHERE Job = @Job
          AND Step = @Step
          AND Status = 'P';

    UPDATE IMEX_JobTrackingProcess
    SET Status = 'S',
        LastModifiedDate = GETDATE(),
        IsAgain = 0
    WHERE Job = @Job
          AND Step = @Step
          AND Status = 'N';
END;