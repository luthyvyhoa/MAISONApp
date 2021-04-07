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

    SELECT @_ID = ID,
           @_IsAgain = IsAgain
    FROM dbo.IMEX_JobTrackingProcess
    WHERE Job = @Job
          AND Step = @Step
          AND CONVERT(DATE, GETDATE()) = CONVERT(DATE, CreatedDate);

    IF @_IsAgain = 1
        RETURN;
    ELSE IF @_ID <> 0
    BEGIN
        SELECT @_ID;
    END;
    ELSE
    BEGIN
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

END;