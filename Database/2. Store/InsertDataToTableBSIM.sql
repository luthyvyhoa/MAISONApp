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
    DECLARE @_LastDayOfLastMonth DATETIME = CONVERT(DATE, DATEADD(d, - (DAY(GETDATE())), GETDATE()));

    DELETE sim
    FROM dbo.SAP_DT0_bsim sim
        JOIN dbo.TMP_DT0_bsim t
            ON t.MATNR = sim.MATNR
               AND t.BWKEY = sim.BWKEY
               AND t.BUDAT = sim.BUDAT
               AND t.BLDAT = sim.BLDAT;

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