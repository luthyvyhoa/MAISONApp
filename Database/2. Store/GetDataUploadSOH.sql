USE [CKDATA];
GO

IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'GetDataUploadSOH'
)
    DROP PROCEDURE GetDataUploadSOH;
GO
CREATE PROCEDURE [dbo].[GetDataUploadSOH]
AS
BEGIN

    DECLARE @_DayAdd INT = 0;
    SELECT @_DayAdd = CASE
                          WHEN DATENAME(dw, GETDATE()) = 'Monday' THEN
                              -1
                          WHEN DATENAME(dw, GETDATE()) = 'Tuesday' THEN
                              -2
                          WHEN DATENAME(dw, GETDATE()) = 'Wednesday' THEN
                              -3
                          WHEN DATENAME(dw, GETDATE()) = 'Thursday' THEN
                              -4
                          WHEN DATENAME(dw, GETDATE()) = 'Friday' THEN
                              -5
                          WHEN DATENAME(dw, GETDATE()) = 'Saturday' THEN
                              -6
                          ELSE
                              -7
                      END;
    DECLARE @_ToDate DATETIME = CONVERT(DATE, DATEADD(DAY, @_DayAdd, GETDATE()));
    DECLARE @_LastDayOfLastMonth DATETIME = CONVERT(DATE, DATEADD(d, - (DAY(GETDATE())), GETDATE()));
    DECLARE @_FirstDayOfMonth DATETIME = CONVERT(DATE, DATEADD(d, - (DAY(GETDATE() - 1)), GETDATE()));

    SELECT SUBSTRING(sim.MATNR, PATINDEX('%[^0]%', sim.MATNR + '.'), LEN(sim.MATNR)) 'ItemNumber',
           sim.WERKS 'Warehouse',
           cs.ShopNo 'StoreName',
           SUM(   CASE
                      WHEN sim.SHKZG = 'H' THEN
                          -1
                      ELSE
                          1
                  END
                  * ISNULL(
                              TRY_CONVERT(NUMERIC(38, 0), REPLACE(
                                                                     REPLACE(REPLACE(sim.DMBTR, ',', ''), '.', ''),
                                                                     '-',
                                                                     ''
                                                                 )),
                              0
                          )
              ) 'ValueClose',
           SUM(   CASE
                      WHEN sim.SHKZG = 'H' THEN
                          -1
                      ELSE
                          1
                  END * ISNULL(TRY_CONVERT(NUMERIC(38, 0), REPLACE(sim.MENGE, ',', '')), 0)
              ) 'QtyClose'
    INTO #tmpSAP_DT0_bsim
    FROM [10.8.1.38].MaisonDW.dbo.SAP_DT0_mseg sim
        JOIN dbo.CK_Store cs
            ON cs.StoreCode = sim.WERKS
               AND cs.Active = 1
    WHERE SUBSTRING(sim.MBLNR, 0, 1) <> '5'
          AND sim.BWART <> '101'
          AND CONVERT(DATE, sim.HSDAT, 104)
          BETWEEN @_FirstDayOfMonth AND @_ToDate
    GROUP BY sim.MATNR,
             sim.WERKS,
             cs.ShopNo;

    SELECT SUBSTRING(mbe.MATNR, PATINDEX('%[^0]%', mbe.MATNR + '.'), LEN(mbe.MATNR)) 'ItemNumber',
           mbe.BWKEY 'Warehouse',
           cs.ShopNo 'StoreName',
           ISNULL(
                     CASE
                         WHEN CHARINDEX('-', mbe.SALK3) > 0 THEN
                             -1
                         ELSE
                             1
                     END * TRY_CONVERT(NUMERIC(38, 0), REPLACE(REPLACE(REPLACE(mbe.SALK3, ',', ''), '.', ''), '-', '')),
                     0
                 ) 'ValueTotal',
           ISNULL(   CASE
                         WHEN CHARINDEX('-', mbe.LBKUM) > 0 THEN
                             -1
                         ELSE
                             1
                     END * TRY_CONVERT(NUMERIC(38, 0), REPLACE(REPLACE(mbe.LBKUM, ',', ''), '-', '')),
                     0
                 ) 'QtyTotal'
    INTO #tmpUSE_DT0_mbewh
    FROM [10.8.1.38].MaisonDW.dbo.USE_DT0_mbewh mbe
        JOIN dbo.CK_Store cs
            ON cs.StoreCode = mbe.BWKEY
               AND cs.Active = 1;

    SELECT rownum = ROW_NUMBER() OVER (PARTITION BY spl.Company,
                                                    spl.ItemNumber,
                                                    spl.Entitycode1
                                       ORDER BY spl.SalesCampaign,
                                                ValidFrom DESC,
                                                CreateDate DESC,
                                                CreateTime DESC
                                      ),
           spl.Company,
           spl.ItemNumber,
           spl.Entitycode1,
           spl.SalesCampaign,
           spl.CreateDate,
           spl.SalesPrice,
           spl.ValidFrom,
           spl.CreateTime
    INTO #tmpSalesPriceList
    FROM [10.8.1.38].ETPEASV55.dbo.SalesPriceList spl
    WHERE spl.ValidFrom <= FORMAT(GETDATE(), 'yyyyMMdd');

    DELETE FROM #tmpSalesPriceList
    WHERE rownum <> 1;

    SELECT rownum = ROW_NUMBER() OVER (PARTITION BY spl.Company,
                                                    spl.ItemNumber
                                       ORDER BY spl.SalesCampaign,
                                                ValidFrom DESC,
                                                CreateDate DESC,
                                                CreateTime DESC
                                      ),
           spl.Company,
           spl.ItemNumber,
           spl.SalesCampaign,
           spl.CreateDate,
           spl.SalesPrice
    INTO #tmpSalesPriceListMini
    FROM #tmpSalesPriceList spl;

    DELETE FROM #tmpSalesPriceList
    WHERE rownum <> 1;

    SELECT rownum = ROW_NUMBER() OVER (PARTITION BY spl.Company,
                                                    spl.ItemNumber
                                       ORDER BY spl.CreateDate DESC
                                      ),
           spl.Company,
           spl.ItemNumber,
           spl.AliasNumber
    INTO #tmpAliasNumber
    FROM [10.8.1.38].ETPEASV55.dbo.AliasNumber spl;

    DELETE FROM #tmpAliasNumber
    WHERE rownum <> 1;

    SELECT *
    INTO #tmpProductLocationBalance
    FROM
    (
        SELECT Warehouse,
               ItemNumber,
               StoreName
        FROM #tmpSAP_DT0_bsim
        UNION
        SELECT Warehouse,
               ItemNumber,
               StoreName
        FROM #tmpUSE_DT0_mbewh
    ) abc;

    SELECT rownum = ROW_NUMBER() OVER (PARTITION BY spl.Company,
                                                    spl.ItemNumber
                                       ORDER BY spl.CreateDate DESC
                                      ),
           spl.Company,
           spl.ItemNumber,
           spl.Specification1
    INTO #tmpProductRSF
    FROM [10.8.1.38].ETPEASV55.dbo.ProductRSF spl
    WHERE spl.BusinessArea = '10';

    DELETE FROM #tmpProductRSF
    WHERE rownum <> 1;

    SELECT pr.Company,
           pr.ItemNumber,
           pr.Specification1,
           plb.Warehouse,
           plb.StoreName
    INTO #tmpResult1
    FROM #tmpProductRSF pr
        JOIN #tmpProductLocationBalance plb
            ON plb.ItemNumber = pr.ItemNumber;

    SELECT pr.Company,
           pr.ItemNumber,
           pr.Specification1,
           pr.Warehouse,
           pr.StoreName,
           CAST((ISNULL(sim.ValueClose, 0) + ISNULL(mbe.ValueTotal, 0))
                / (ISNULL(sim.QtyClose, 0) + ISNULL(mbe.QtyTotal, 0)) AS NUMERIC(36, 0)) 'UnitLandedCost',
           (ISNULL(sim.QtyClose, 0) + ISNULL(mbe.QtyTotal, 0)) AS 'OnHandQty'
    INTO #tmpResult2
    FROM #tmpResult1 pr
        LEFT JOIN #tmpSAP_DT0_bsim sim
            ON sim.ItemNumber = pr.ItemNumber
               AND pr.Warehouse = sim.Warehouse
        LEFT JOIN #tmpUSE_DT0_mbewh mbe
            ON mbe.ItemNumber = pr.ItemNumber
               AND pr.Warehouse = mbe.Warehouse
    WHERE ISNULL(sim.QtyClose, 0) + ISNULL(mbe.QtyTotal, 0) > 0;

    SELECT pr.Company,
           pr.ItemNumber,
           pr.Specification1,
           pr.Warehouse,
           ISNULL(spl.SalesCampaign, splMini.SalesCampaign) 'SalesCampaign',
           ISNULL(spl.SalesPrice, splMini.SalesPrice) 'SalesPrice',
           pr.UnitLandedCost,
           pr.StoreName,
           pr.OnHandQty
    INTO #tmpResult3
    FROM #tmpResult2 pr
        LEFT JOIN #tmpSalesPriceList spl
            ON pr.ItemNumber = spl.ItemNumber
               AND pr.Company = spl.Company
               AND spl.Entitycode1 = pr.Warehouse
        LEFT JOIN #tmpSalesPriceListMini splMini
            ON pr.ItemNumber = splMini.ItemNumber
               AND pr.Company = splMini.Company;

    SELECT DISTINCT
           'Vietnam' AS 'Country',
           FORMAT(GETDATE(), 'yyyy/MM/dd') AS 'Date',
           pr.StoreName,
           pr.Specification1 'ArticleNo',
           an.AliasNumber 'Barcode',
           '' AS 'Batch',
           CAST(pr.OnHandQty AS NUMERIC(36, 2)) 'OnHandQty',
           pr.UnitLandedCost,
           UnitGrossPrice = CASE
                                WHEN pr.SalesCampaign = 'N' THEN
                                    CAST(ROUND(pr.SalesPrice / 1.1, 2) AS NUMERIC(36, 2))
                                ELSE
                                    CAST(pr.SalesPrice AS NUMERIC(36, 2))
                            END,
           CAST(ROUND(pr.SalesPrice / 1.1, 2) AS NUMERIC(36, 2)) AS 'UnitNetPrice'
    FROM #tmpResult3 pr
        LEFT JOIN #tmpAliasNumber an
            ON pr.ItemNumber = an.ItemNumber
               AND pr.Company = an.Company;
    --WHERE pr.Warehouse = '1021'
    --      AND an.ItemNumber = '100115585002';

    --SELECT *
    --FROM #tmpResult3
    --WHERE Warehouse = '1029' AND ItemNumber = '1004043708205';

    DROP TABLE #tmpSAP_DT0_bsim;
    DROP TABLE #tmpUSE_DT0_mbewh;
    DROP TABLE #tmpSalesPriceList;
    DROP TABLE #tmpSalesPriceListMini;
    DROP TABLE #tmpAliasNumber;
    DROP TABLE #tmpProductLocationBalance;
    DROP TABLE #tmpProductRSF;
    DROP TABLE #tmpResult1;
    DROP TABLE #tmpResult2;
    DROP TABLE #tmpResult3;

END;
GO