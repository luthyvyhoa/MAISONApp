IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'InsertDataToTableBSIM'
)
    DROP PROCEDURE InsertDataToTableBSIM;
GO
CREATE PROCEDURE [dbo].[InsertDataToTableBSIM]
AS
BEGIN

    DECLARE @_MinDate DATETIME2;
    DECLARE @_MaxDate DATETIME2;

    SELECT @_MinDate = CASE
                           WHEN MIN(CONVERT(DATE, sim.BUDAT, 104)) < MIN(CONVERT(DATE, sim.BLDAT, 104)) THEN
                               MIN(CONVERT(DATE, sim.BUDAT, 104))
                           ELSE
                               MIN(CONVERT(DATE, sim.BLDAT, 104))
                       END,
           @_MaxDate = CASE
                           WHEN MAX(CONVERT(DATE, sim.BUDAT, 104)) > MAX(CONVERT(DATE, sim.BLDAT, 104)) THEN
                               MAX(CONVERT(DATE, sim.BUDAT, 104))
                           ELSE
                               MAX(CONVERT(DATE, sim.BLDAT, 104))
                       END
    FROM dbo.TMP_DT0_bsim sim;

    DELETE sim
    FROM dbo.SAP_DT0_bsim sim
    WHERE CONVERT(DATE, sim.BUDAT, 104)
          BETWEEN @_MinDate AND @_MaxDate
          OR CONVERT(DATE, sim.BLDAT, 104)
          BETWEEN @_MinDate AND @_MaxDate;

    INSERT INTO dbo.SAP_DT0_bsim
    (
        MATNR,
        BWKEY,
        BWTAR,
        BELNR,
        GJAHR,
        BUZEI,
        BUZID,
        SHKZG,
        DMBTR,
        MENGE,
        MEINS,
        BUDAT,
        BLDAT,
        BLART
    )
    SELECT MATNR,
           BWKEY,
           BWTAR,
           BELNR,
           GJAHR,
           BUZEI,
           BUZID,
           SHKZG,
           DMBTR,
           MENGE,
           MEINS,
           BUDAT,
           BLDAT,
           BLART
    FROM dbo.TMP_DT0_bsim;

	TRUNCATE TABLE dbo.TMP_DT0_bsim;
END;