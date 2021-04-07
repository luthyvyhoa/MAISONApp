USE [CKDATA]
GO

IF EXISTS ( SELECT  *
            FROM    sys.objects
           WHERE   type = 'P'
                   AND name = 'GetDataUploadSales' )
   DROP PROCEDURE GetDataUploadSales;
GO
CREATE PROCEDURE [dbo].[GetDataUploadSales]
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

    SELECT Company,
           ItemNumber,
           Warehouse,
           CreateDate,
           InvoiceNumber,
           SUM(InvoiceQuantity) 'QtyOut',
           SUM(LocalAmount / InvoiceQuantity / 1.1) 'UnitNetPriceOut'
    INTO #tmpCashOrderTrn
    FROM [10.8.1.123].ETPEASV55.dbo.CashOrderTrn
    WHERE InvoiceType = 31
          AND CreateDate
          BETWEEN FORMAT(@_FromDate, 'yyyyMMdd') AND FORMAT(@_ToDate, 'yyyyMMdd')
    GROUP BY Company,
             ItemNumber,
             Warehouse,
             CreateDate,
             InvoiceNumber;

    SELECT Company,
           ReturnItemNumber 'ItemNumber',
           Warehouse,
           CreateDate,
           SalesReturnNumber,
           SUM(ReturnQuantity) 'QtyIn',
           SUM(LocalAmount / ReturnQuantity / 1.1) 'UnitNetPriceIn'
    INTO #tmpSalesReturnTrn
    FROM [10.8.1.123].ETPEASV55.dbo.SalesReturnTrn
    WHERE InvoiceType = 31
          AND CreateDate
          BETWEEN FORMAT(@_FromDate, 'yyyyMMdd') AND FORMAT(@_ToDate, 'yyyyMMdd')
    GROUP BY Company,
             ReturnItemNumber,
             Warehouse,
             CreateDate,
             SalesReturnNumber;

    SELECT rownum = ROW_NUMBER() OVER (PARTITION BY spl.Company,
                                                    spl.ItemNumber,
                                                    spl.Entitycode1
                                       ORDER BY ValidFrom DESC,
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
    FROM [10.8.1.123].ETPEASV55.dbo.SalesPriceList spl;

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
    FROM [10.8.1.123].ETPEASV55.dbo.AliasNumber spl;

    DELETE FROM #tmpAliasNumber
    WHERE rownum <> 1;

    SELECT rownum = ROW_NUMBER() OVER (PARTITION BY spl.Company,
                                                    spl.ItemNumber,
                                                    spl.Warehouse
                                       ORDER BY spl.CreateDate DESC
                                      ),
           spl.Company,
           spl.ItemNumber,
           spl.Warehouse
    INTO #tmpProductLocationBalance
    FROM [10.8.1.123].ETPEASV55.dbo.ProductLocationBalance spl;

    DELETE FROM #tmpProductLocationBalance
    WHERE rownum <> 1;

    SELECT rownum = ROW_NUMBER() OVER (PARTITION BY spl.Company,
                                                    spl.ItemNumber
                                       ORDER BY spl.CreateDate DESC
                                      ),
           spl.Company,
           spl.ItemNumber,
           spl.Specification1,
           spl.DimensionX,
           spl.DimensionY
    INTO #tmpProductRSF
    FROM [10.8.1.123].ETPEASV55.dbo.ProductRSF spl;

    DELETE FROM #tmpProductRSF
    WHERE rownum <> 1;

    SELECT pr.Company,
           pr.ItemNumber,
           pr.Specification1,
           pr.DimensionX,
           pr.DimensionY,
           plb.Warehouse
    INTO #tmpResult1
    FROM #tmpProductRSF pr
        JOIN #tmpProductLocationBalance plb
            ON plb.ItemNumber = pr.ItemNumber
               AND plb.Company = pr.Company;

    SELECT pr.Company,
           pr.ItemNumber,
           pr.Specification1,
           pr.DimensionX,
           pr.DimensionY,
           pr.Warehouse,
           spl.SalesCampaign,
           spl.SalesPrice
    INTO #tmpResult2
    FROM #tmpResult1 pr
        JOIN #tmpSalesPriceList spl
            ON pr.ItemNumber = spl.ItemNumber
               AND pr.Company = spl.Company
               AND spl.Entitycode1 = pr.Warehouse;

    SELECT pr.Company,
           pr.ItemNumber,
           pr.Specification1,
           pr.DimensionX,
           pr.DimensionY,
           pr.Warehouse,
           pr.SalesCampaign,
           pr.SalesPrice,
           an.AliasNumber,
           cs.ShopNo
    INTO #tmpResult3
    FROM #tmpResult2 pr
        JOIN dbo.CK_Store cs
            ON cs.StoreCode = pr.Warehouse
        LEFT JOIN #tmpAliasNumber an
            ON pr.ItemNumber = an.ItemNumber
               AND pr.Company = an.Company;

    SELECT DISTINCT
           'Vietnam' AS 'Country',
           FORMAT(CONVERT(DATE, CONVERT(NVARCHAR, co.CreateDate), 112), 'yyyy/MM/dd') AS 'Date',
           pr.ShopNo AS 'Location',
           pr.Specification1 'Stylecode',
           pr.DimensionX 'Color',
           pr.DimensionY 'Size',
           pr.AliasNumber 'Barcode',
           InvoiceQuantity = CAST(ISNULL(co.QtyOut, 0) AS NUMERIC(36, 2)),
           '' AS 'UnitCost',
           Grossprice = CASE
                            WHEN pr.SalesCampaign = 'N' THEN
                                CAST(ROUND(pr.SalesPrice / 1.1, 2) AS NUMERIC(36, 2))
                            ELSE
                                CAST(pr.SalesPrice AS NUMERIC(36, 2))
                        END,
           NetPrice = CAST(ISNULL(co.UnitNetPriceOut, 0) AS NUMERIC(36, 2)),
           '' AS 'NetSGD',
           co.InvoiceNumber 'TranNo',
           '' AS 'staffcode'
    FROM #tmpResult3 pr
        JOIN #tmpCashOrderTrn co
            ON co.Company = pr.Company
               AND co.ItemNumber = pr.ItemNumber
               AND co.Warehouse = pr.Warehouse
    UNION
    SELECT DISTINCT
           'Vietnam' AS 'Country',
           FORMAT(CONVERT(DATE, CONVERT(NVARCHAR, sr.CreateDate), 112), 'yyyy/MM/dd') AS 'Date',
           pr.ShopNo AS 'Location',
           pr.Specification1 'Stylecode',
           pr.DimensionX 'Color',
           pr.DimensionY 'Size',
           pr.AliasNumber 'Barcode',
           InvoiceQuantity = CAST(0 - ISNULL(sr.QtyIn, 0) AS NUMERIC(36, 2)),
           '' AS 'UnitCost',
           Grossprice = CASE
                            WHEN pr.SalesCampaign = 'N' THEN
                                CAST(ROUND(pr.SalesPrice / 1.1, 2) AS NUMERIC(36, 2))
                            ELSE
                                CAST(pr.SalesPrice AS NUMERIC(36, 2))
                        END,
           NetPrice = CAST(ISNULL(sr.UnitNetPriceIn, 0) AS NUMERIC(36, 2)),
           '' AS 'NetSGD',
           sr.SalesReturnNumber 'TranNo',
           '' AS 'staffcode'
    FROM #tmpResult3 pr
        JOIN #tmpSalesReturnTrn sr
            ON sr.Company = pr.Company
               AND sr.ItemNumber = pr.ItemNumber
               AND sr.Warehouse = pr.Warehouse;

    DROP TABLE #tmpSalesPriceList;
    DROP TABLE #tmpCashOrderTrn;
    DROP TABLE #tmpSalesReturnTrn;
    DROP TABLE #tmpAliasNumber;
    DROP TABLE #tmpProductLocationBalance;
    DROP TABLE #tmpProductRSF;
    DROP TABLE #tmpResult1;
    DROP TABLE #tmpResult2;
    DROP TABLE #tmpResult3;
END;
GO