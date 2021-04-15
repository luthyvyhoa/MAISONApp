IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'InsertDataToTableMBEWH'
)
    DROP PROCEDURE InsertDataToTableMBEWH;
GO
CREATE PROCEDURE [dbo].[InsertDataToTableMBEWH]
AS
BEGIN

    DECLARE @_Month INT;
    DECLARE @_Year INT;

    SELECT @_Month = MAX(mbe.LFMON),
           @_Year = MAX(mbe.LFGJA)
    FROM dbo.TMP_DT0_mbewh mbe;

    DELETE mbe
    FROM dbo.SAP_DT0_mbewh mbe
    WHERE mbe.LFGJA = @_Year
          AND mbe.LFMON = @_Month;

    INSERT INTO dbo.SAP_DT0_mbewh
    (
        MATNR,
        BWKEY,
        BWTAR,
        LFGJA,
        LFMON,
        LBKUM,
        SALK3,
        VPRSV,
        VERPR,
        STPRS,
        PEINH,
        BKLAS,
        SALKV,
        VKSAL
    )
    SELECT MATNR,
           BWKEY,
           BWTAR,
           LFGJA,
           LFMON,
           LBKUM,
           SALK3,
           VPRSV,
           VERPR,
           STPRS,
           PEINH,
           BKLAS,
           SALKV,
           VKSAL
    FROM dbo.TMP_DT0_mbewh;

	TRUNCATE TABLE dbo.TMP_DT0_mbewh
END;