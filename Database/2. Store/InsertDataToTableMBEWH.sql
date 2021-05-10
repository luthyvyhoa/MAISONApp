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
BEGIN;

    DELETE u
    FROM dbo.SAP_DT0_mbewh u
        JOIN dbo.TMP_DT0_mbewh t
            ON t.MATNR = u.MATNR
               AND t.BWKEY = u.BWKEY
               AND t.LFGJA = u.LFGJA
               AND t.LFMON = u.LFMON;

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

    TRUNCATE TABLE dbo.TMP_DT0_mbewh;
END;