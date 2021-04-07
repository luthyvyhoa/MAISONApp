IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'IMEX_CreateProcessJob'
)
    DROP PROCEDURE IMEX_CreateProcessJob;
GO
CREATE PROCEDURE [dbo].[IMEX_CreateProcessJob]
AS
BEGIN

    UPDATE IMEX_JobProcess
    SET Status = 'C',
        Message = 'Auto cancel old job when start new process',
        LastModifiedDate = GETDATE()
    WHERE Status = 'N'
          OR Status = 'P';

    INSERT INTO dbo.IMEX_JobProcess
    (
        Job,
        Step,
        Status,
        CreatedDate,
        LastModifiedDate,
        Message
    )
    SELECT job.Job,
           step.Step,
           'N',
           GETDATE(),
           NULL,
           NULL
    FROM dbo.IMEX_Job job
        JOIN IMEX_Step step
            ON step.Job = job.Job
    WHERE job.Inactive = 0
          AND step.Inactive = 0;

END;