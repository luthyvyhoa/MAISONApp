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
    DECLARE @_ToDate DATETIME = DATEADD(DAY, @_DayAdd, GETDATE());
    DECLARE @_FromDate DATETIME = DATEADD(DAY, -6, @_ToDate);
    DECLARE @_LastDayOfLastMonth DATETIME = CONVERT(DATE, DATEADD(d, - (DAY(GETDATE())), GETDATE()));
    DECLARE @_FirstDayOfMonth DATETIME = CONVERT(DATE, DATEADD(d, - (DAY(GETDATE() - 1)), GETDATE()));
    DECLARE @_LastDayOfMonth DATETIME
        = CONVERT(DATE, DATEADD(d, - (DAY(DATEADD(m, 1, GETDATE()))), DATEADD(m, 1, GETDATE())));

    SELECT SUBSTRING(mbe.MATNR, PATINDEX('%[^0]%', mbe.MATNR + '.'), LEN(mbe.MATNR)) 'ItemNumber',
           mbe.BWKEY 'Warehouse',
           ISNULL(TRY_CONVERT(NUMERIC(38, 0), REPLACE(REPLACE(mbe.SALK3, ',', ''), '.', '')), 0) 'ValueTotal',
           ISNULL(TRY_CONVERT(NUMERIC(38, 0), REPLACE(mbe.LBKUM, ',', '')), 0) 'QtyTotal'
    INTO #tmpSAP_DT0_mbewh
    FROM [10.8.1.38].MaisonDW.dbo.SAP_DT0_mbewh mbe
    WHERE YEAR(@_LastDayOfLastMonth) = mbe.LFGJA
          AND MONTH(@_LastDayOfLastMonth) = mbe.LFMON;

    SELECT SUBSTRING(sim.MATNR, PATINDEX('%[^0]%', sim.MATNR + '.'), LEN(sim.MATNR)) 'ItemNumber',
           sim.BWKEY 'Warehouse',
           SUM(ISNULL(TRY_CONVERT(NUMERIC(38, 0), REPLACE(REPLACE(sim.DMBTR, ',', ''), '.', '')), 0)) 'ValueClose',
           SUM(ISNULL(TRY_CONVERT(NUMERIC(38, 0), REPLACE(sim.MENGE, ',', '')), 0)) 'QtyClose'
    INTO #tmpSAP_DT0_bsim
    FROM [10.8.1.38].MaisonDW.dbo.SAP_DT0_bsim sim
    WHERE CONVERT(DATE, sim.BUDAT, 104)
          BETWEEN @_FirstDayOfMonth AND @_LastDayOfMonth
          OR CONVERT(DATE, sim.BLDAT, 104)
          BETWEEN @_FirstDayOfMonth AND @_LastDayOfMonth
    GROUP BY sim.MATNR,
             sim.BWKEY;

    SELECT *
    INTO #tmpOrderTrn
    FROM
    (
        SELECT Company,
               ItemNumber,
               Warehouse
        FROM [10.8.1.38].ETPEASV55.dbo.CashOrderTrn
        WHERE CreateDate
              BETWEEN FORMAT(@_FromDate, 'yyyyMMdd') AND FORMAT(@_ToDate, 'yyyyMMdd')
              AND
              (
                  LotNumber IS NULL
                  OR LotNumber = ''
              )
              AND LocalAmount <> 0
        UNION
        SELECT Company,
               ReturnItemNumber 'ItemNumber',
               Warehouse
        FROM [10.8.1.38].ETPEASV55.dbo.SalesReturnTrn
        WHERE CreateDate
              BETWEEN FORMAT(@_FromDate, 'yyyyMMdd') AND FORMAT(@_ToDate, 'yyyyMMdd')
              AND LocalAmount <> 0
    ) abc
    WHERE abc.ItemNumber <> ''
          AND abc.ItemNumber IS NOT NULL
          AND abc.Warehouse IS NOT NULL;

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
           spl.SalesPrice
    INTO #tmpSalesPriceList
    FROM [10.8.1.38].ETPEASV55.dbo.SalesPriceList spl
    WHERE spl.ValidFrom <= FORMAT(GETDATE(), 'yyyyMMdd');

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

    SELECT rownum = ROW_NUMBER() OVER (PARTITION BY spl.Company,
                                                    spl.ItemNumber,
                                                    spl.Warehouse
                                       ORDER BY spl.CreateDate DESC
                                      ),
           spl.Company,
           spl.ItemNumber,
           spl.Warehouse,
           spl.BalanceApproved
    INTO #tmpProductLocationBalance
    FROM [10.8.1.38].ETPEASV55.dbo.ProductLocationBalance spl;

    DELETE FROM #tmpProductLocationBalance
    WHERE rownum <> 1;

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
           plb.BalanceApproved
    INTO #tmpResult1
    FROM #tmpProductRSF pr
        JOIN #tmpProductLocationBalance plb
            ON plb.ItemNumber = pr.ItemNumber
               AND plb.Company = pr.Company;

    SELECT pr.Company,
           pr.ItemNumber,
           pr.Specification1,
           pr.Warehouse,
           pr.BalanceApproved
    INTO #tmpResult11
    FROM #tmpResult1 pr
        LEFT JOIN #tmpOrderTrn ot
            ON ot.ItemNumber = pr.ItemNumber
               AND ot.Company = pr.Company
               AND ot.Warehouse = pr.Warehouse
    WHERE CAST(pr.BalanceApproved AS NUMERIC(36, 2)) > 0
          OR ot.Company IS NOT NULL;

    SELECT pr.Company,
           pr.ItemNumber,
           pr.Specification1,
           pr.Warehouse,
           pr.BalanceApproved,
           CAST((sim.ValueClose + mbe.ValueTotal) / (sim.QtyClose + mbe.QtyTotal) AS NUMERIC(36, 0)) 'UnitLandedCost'
    INTO #tmpResult2
    FROM #tmpResult11 pr
        LEFT JOIN #tmpSAP_DT0_bsim sim
            ON sim.ItemNumber = pr.ItemNumber
               AND pr.Warehouse = sim.Warehouse
        LEFT JOIN #tmpSAP_DT0_mbewh mbe
            ON mbe.ItemNumber = pr.ItemNumber
               AND pr.Warehouse = mbe.Warehouse;

    SELECT pr.Company,
           pr.ItemNumber,
           pr.Specification1,
           pr.Warehouse,
           spl.SalesCampaign,
           spl.SalesPrice,
           pr.BalanceApproved,
           pr.UnitLandedCost
    INTO #tmpResult3
    FROM #tmpResult2 pr
        JOIN #tmpSalesPriceList spl
            ON pr.ItemNumber = spl.ItemNumber
               AND pr.Company = spl.Company
               AND spl.Entitycode1 = pr.Warehouse;

    SELECT DISTINCT
           'Vietnam' AS 'Country',
           FORMAT(GETDATE(), 'yyyy/MM/dd') AS 'Date',
           cs.ShopNo AS 'StoreName',
           pr.Specification1 'ArticleNo',
           an.AliasNumber 'Barcode',
           '' AS 'Batch',
           CAST(pr.BalanceApproved AS NUMERIC(36, 2)) 'OnHandQty',
           pr.UnitLandedCost,
           UnitGrossPrice = CASE
                                WHEN pr.SalesCampaign = 'N' THEN
                                    CAST(ROUND(pr.SalesPrice / 1.1, 2) AS NUMERIC(36, 2))
                                ELSE
                                    CAST(pr.SalesPrice AS NUMERIC(36, 2))
                            END,
           CAST(ROUND(pr.SalesPrice / 1.1, 2) AS NUMERIC(36, 2)) AS 'UnitNetPrice'
    FROM #tmpResult3 pr
        JOIN dbo.CK_Store cs
            ON cs.StoreCode = pr.Warehouse
        LEFT JOIN #tmpAliasNumber an
            ON pr.ItemNumber = an.ItemNumber
               AND pr.Company = an.Company;

    DROP TABLE #tmpSAP_DT0_bsim;
    DROP TABLE #tmpSAP_DT0_mbewh;
    DROP TABLE #tmpOrderTrn;
    DROP TABLE #tmpSalesPriceList;
    DROP TABLE #tmpAliasNumber;
    DROP TABLE #tmpProductLocationBalance;
    DROP TABLE #tmpProductRSF;
    DROP TABLE #tmpResult1;
    DROP TABLE #tmpResult11;
    DROP TABLE #tmpResult2;
    DROP TABLE #tmpResult3;

END;
GO