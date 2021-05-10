IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'UpdateDataToTableUSE'
)
    DROP PROCEDURE UpdateDataToTableUSE;
GO
CREATE PROCEDURE [dbo].[UpdateDataToTableUSE]
AS
BEGIN

    DECLARE @_LastDayOfLastMonth DATETIME = CONVERT(DATE, DATEADD(d, - (DAY(GETDATE())), GETDATE()));
    SELECT SUBSTRING(mbe.MATNR, PATINDEX('%[^0]%', mbe.MATNR + '.'), LEN(mbe.MATNR)) 'ItemNumber',
           mbe.BWKEY 'Warehouse',
           MAX(ISNULL(
                         CASE
                             WHEN CHARINDEX('-', mbe.SALK3) > 0 THEN
                                 -1
                             ELSE
                                 1
                         END
                         * CONVERT(
                                      DECIMAL(38, 0),
                                      CASE
                                          WHEN CHARINDEX('-', mbe.SALK3) > 0 THEN
                                              -1
                                          ELSE
                                              1
                                      END * REPLACE(REPLACE(REPLACE(mbe.SALK3, ',', ''), '.', ''), '-', '')
                                  ),
                         0
                     )
              )
           + SUM(   CASE
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
                                * CONVERT(
                                             DECIMAL(38, 0),
                                             REPLACE(REPLACE(REPLACE(sim.DMBTR, ',', ''), '.', ''), '-', '')
                                         ),
                                0
                            )
                ) 'ValueTotal',
           MAX(ISNULL(   CASE
                             WHEN CHARINDEX('-', mbe.LBKUM) > 0 THEN
                                 -1
                             ELSE
                                 1
                         END * CONVERT(DECIMAL(38, 0), REPLACE(REPLACE(mbe.LBKUM, ',', ''), '-', '')),
                         0
                     )
              )
           + SUM(   CASE
                        WHEN sim.SHKZG = 'H' THEN
                            -1
                        ELSE
                            1
                    END * ISNULL(   CASE
                                        WHEN CHARINDEX('-', sim.MENGE) > 0 THEN
                                            -1
                                        ELSE
                                            1
                                    END * CONVERT(DECIMAL(38, 0), REPLACE(REPLACE(sim.MENGE, ',', ''), '-', '')),
                                    0
                                )
                ) 'QtyTotal'
    INTO #tmpUSE_DT0_mbewh
    FROM dbo.USE_DT0_mbewh mbe
        JOIN dbo.SAP_DT0_mseg sim
            ON sim.MATNR = mbe.MATNR
               AND sim.WERKS = mbe.BWKEY
               AND CONVERT(DATE, sim.HSDAT, 104)
               BETWEEN DATEADD(
                                  d,
                                  - (DAY(DATEADD(m, 1, CONVERT(DATE, mbe.LFGJA + '-' + mbe.LFMON + '-01')))),
                                  DATEADD(m, 1, CONVERT(DATE, mbe.LFGJA + '-' + mbe.LFMON + '-02'))
                              ) AND @_LastDayOfLastMonth
    GROUP BY mbe.MATNR,
             mbe.BWKEY;
    UPDATE mbe
    SET mbe.SALK3 = tmp.ValueTotal,
        mbe.LBKUM = tmp.QtyTotal,
        mbe.LFMON = MONTH(@_LastDayOfLastMonth),
        mbe.LFGJA = YEAR(@_LastDayOfLastMonth)
    FROM dbo.USE_DT0_mbewh mbe
        JOIN #tmpUSE_DT0_mbewh tmp
            ON tmp.ItemNumber = SUBSTRING(mbe.MATNR, PATINDEX('%[^0]%', mbe.MATNR + '.'), LEN(mbe.MATNR))
               AND tmp.Warehouse = mbe.BWKEY;
END;
