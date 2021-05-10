IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'InsertDataToTablemkpf'
)
    DROP PROCEDURE InsertDataToTablemkpf;
GO
CREATE PROCEDURE [dbo].[InsertDataToTablemkpf]
AS
BEGIN

    DELETE sim
    FROM dbo.SAP_DT0_mkpf sim
        JOIN dbo.TMP_DT0_mkpf t
            ON t.MBLNR = sim.MBLNR
               AND t.MJAHR = sim.MJAHR
               AND t.BUDAT = sim.BUDAT
               AND t.BLDAT = sim.BLDAT;

    INSERT INTO dbo.SAP_DT0_mkpf
    (
        MBLNR,
        MJAHR,
        VGART,
        BLART,
        BLAUM,
        BLDAT,
        BUDAT,
        CPUDT,
        CPUTM,
        USNAM,
        XBLNR,
        BKTXT
    )
    SELECT MBLNR,
           MJAHR,
           VGART,
           BLART,
           BLAUM,
           BLDAT,
           BUDAT,
           CPUDT,
           CPUTM,
           USNAM,
           XBLNR,
           BKTXT
    FROM dbo.TMP_DT0_mkpf;

    TRUNCATE TABLE dbo.TMP_DT0_mkpf;
END;