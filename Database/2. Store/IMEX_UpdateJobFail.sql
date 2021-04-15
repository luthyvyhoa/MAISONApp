IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'IMEX_UpdateJobFail'
)
    DROP PROCEDURE IMEX_UpdateJobFail;
GO
CREATE PROCEDURE [dbo].[IMEX_UpdateJobFail]
    @Job INT,
    @Step INT,
    @Message NVARCHAR(MAX)
AS
BEGIN
    UPDATE IMEX_JobProcess
    SET Status = 'F',
        LastModifiedDate = GETDATE(),
        Message = @Message
    WHERE Job = @Job
          AND Step = @Step
          AND Status = 'P';

    UPDATE IMEX_JobTrackingProcess
    SET Status = 'F',
        LastModifiedDate = GETDATE(),
        IsAgain = 0,
		Message = @Message
    WHERE Job = @Job
          AND Step = @Step
          AND Status = 'N';
END;