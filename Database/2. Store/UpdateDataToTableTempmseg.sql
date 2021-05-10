IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'UpdateDataToTableTempmseg'
)
    DROP PROCEDURE UpdateDataToTableTempmseg;
GO
CREATE PROCEDURE [dbo].[UpdateDataToTableTempmseg]
AS
BEGIN

    UPDATE ms
    SET ms.VFDAT = mk.BLDAT,
        ms.HSDAT = mk.BUDAT
    FROM dbo.TMP_DT0_mseg ms
        JOIN dbo.TMP_DT0_mkpf mk
            ON mk.MBLNR = ms.MBLNR
               AND mk.MJAHR = ms.MJAHR;

    DELETE FROM dbo.TMP_DT0_mseg
    WHERE VFDAT = '00.00.0000'
          OR VFDAT IS NULL
          OR XAUTO = 'X';

    TRUNCATE TABLE dbo.TMP_DT0_mkpf;
END;
