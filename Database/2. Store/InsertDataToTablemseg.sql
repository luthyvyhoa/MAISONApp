IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'InsertDataToTablemseg'
)
    DROP PROCEDURE InsertDataToTablemseg;
GO
CREATE PROCEDURE [dbo].[InsertDataToTablemseg]
AS
BEGIN

    DECLARE @_LastDayOfLastMonth DATETIME = CONVERT(DATE, DATEADD(d, - (DAY(GETDATE())), GETDATE()));

    DELETE sim
    FROM dbo.SAP_DT0_mseg sim
        JOIN dbo.TMP_DT0_mseg t
            ON t.MATNR = sim.MATNR
               AND t.WERKS = sim.WERKS
               AND t.MBLNR = sim.MBLNR;

    UPDATE ms
    SET ms.VFDAT = mk.BLDAT,
        ms.HSDAT = mk.BUDAT
    FROM dbo.TMP_DT0_mseg ms
        JOIN dbo.SAP_DT0_mkpf mk
            ON mk.MBLNR = ms.MBLNR
               AND mk.MJAHR = ms.MJAHR;

    INSERT INTO dbo.SAP_DT0_mseg
    (
        MBLNR,
        MJAHR,
        ZEILE,
        LINE_ID,
        PARENT_ID,
        LINE_DEPTH,
        BWART,
        MATNR,
        WERKS,
        LGORT,
        CHARG,
        SHKZG,
        WAERS,
        DMBTR,
        MENGE,
        MEINS,
        ERFMG,
        ERFME,
        EBELN,
        KOKRS,
        GJAHR,
        BUKRS,
        BELNR,
        KZSTR,
        PRCTR,
        SAKTO,
        EXBWR,
        VFDAT,
        MATBF,
        URZEI,
        HSDAT,
        KUNNR,
        SJAHR,
        SMBLN,
        SMBLP,
        LFBNR,
        XAUTO
    )
    SELECT DISTINCT
           MBLNR,
           MJAHR,
           ZEILE,
           LINE_ID,
           PARENT_ID,
           LINE_DEPTH,
           BWART,
           MATNR,
           WERKS,
           LGORT,
           CHARG,
           SHKZG,
           WAERS,
           DMBTR,
           MENGE,
           MEINS,
           ERFMG,
           ERFME,
           EBELN,
           KOKRS,
           GJAHR,
           BUKRS,
           BELNR,
           KZSTR,
           PRCTR,
           SAKTO,
           EXBWR,
           VFDAT,
           MATBF,
           URZEI,
           HSDAT,
           KUNNR,
           SJAHR,
           SMBLN,
           SMBLP,
           LFBNR,
           XAUTO
    FROM dbo.TMP_DT0_mseg;

    SELECT sim.MATNR 'ItemNumber',
           sim.WERKS 'Warehouse',
           SUM(   CASE
                      WHEN sim.SHKZG = 'H' THEN
                          -1
                      ELSE
                          1
                  END
                  * ISNULL(
                              CASE
                                  WHEN CHARINDEX('-', sim.DMBTR) > 0 THEN
                                      -1
                                  ELSE
                                      1
                              END
                              * CONVERT(DECIMAL(38, 0), REPLACE(REPLACE(REPLACE(sim.DMBTR, ',', ''), '.', ''), '-', '')),
                              0
                          )
              ) 'ValueTotal',
           SUM(   CASE
                      WHEN sim.SHKZG = 'H' THEN
                          -1
                      ELSE
                          1
                  END * ISNULL(   CASE
                                      WHEN CHARINDEX('-', sim.MENGE) > 0 THEN
                                          -1
                                      ELSE
                                          1
                                  END * CONVERT(DECIMAL(38, 0), REPLACE(sim.MENGE, ',', '')),
                                  0
                              )
              ) 'QtyTotal',
           MAX(CONVERT(DATE, sim.HSDAT, 104)) 'maxDate',
           mbe.MATNR 'IsExist'
    INTO #tmpUSE_DT0_mbewh
    FROM dbo.TMP_DT0_mseg sim
        LEFT JOIN dbo.USE_DT0_mbewh mbe
            ON mbe.MATNR = sim.MATNR
               AND mbe.BWKEY = sim.WERKS
    WHERE sim.XAUTO IS NULL
          AND CONVERT(DATE, sim.HSDAT, 104) < @_LastDayOfLastMonth
    GROUP BY sim.MATNR,
             sim.WERKS,
             mbe.MATNR;

    UPDATE mbe
    SET mbe.SALK3 = tmp.ValueTotal,
        mbe.LBKUM = tmp.QtyTotal,
        mbe.LFMON = MONTH(@_LastDayOfLastMonth),
        mbe.LFGJA = YEAR(@_LastDayOfLastMonth)
    FROM dbo.USE_DT0_mbewh mbe
        JOIN #tmpUSE_DT0_mbewh tmp
            ON tmp.ItemNumber = mbe.MATNR
               AND tmp.Warehouse = mbe.BWKEY;

    INSERT INTO dbo.USE_DT0_mbewh
    (
        MATNR,
        BWKEY,
        LFGJA,
        LFMON,
        LBKUM,
        SALK3
    )
    SELECT ItemNumber,
           Warehouse,
           YEAR(maxDate),
           MONTH(maxDate),
           QtyTotal,
           ValueTotal
    FROM #tmpUSE_DT0_mbewh
    WHERE IsExist IS NULL;

    TRUNCATE TABLE dbo.TMP_DT0_mseg;
END;