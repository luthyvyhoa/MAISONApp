USE [CKDATA];
GO

IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'GetDataUploadSales'
)
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
    DECLARE @_ToDate DATETIME = CONVERT(DATE, DATEADD(DAY, @_DayAdd, GETDATE()));
    DECLARE @_FromDate DATETIME = CONVERT(DATE, DATEADD(DAY, -6, @_ToDate));

    SELECT XBLNR 'InvoiceNumber',
           VBELN 'BillNumber'
    INTO #tmpSAP_DT0_vbrk
    FROM [10.8.1.38].MaisonDW.dbo.SAP_DT0_vbrk
    WHERE CONVERT(DATE, FKDAT, 104)
    BETWEEN @_FromDate AND @_ToDate;

    SELECT p.VBELN 'BillNumber',
           SUBSTRING(p.MATNR, PATINDEX('%[^0]%', p.MATNR + '.'), LEN(p.MATNR)) 'ItemNumber',
           p.WERKS 'Warehouse',
           ISNULL(TRY_CONVERT(NUMERIC(38, 0), REPLACE(REPLACE(p.WAVWR, ',', ''), '.', '')), 0) 'UnitCost',
           CONVERT(DATE, PRSDT, 104) 'PRSDT'
    INTO #tmpSAP_DT0_vbrp
    FROM [10.8.1.38].MaisonDW.dbo.SAP_DT0_vbrp p
    WHERE CONVERT(DATE, PRSDT, 104)
    BETWEEN @_FromDate AND @_ToDate;

    SELECT rownum = ROW_NUMBER() OVER (PARTITION BY k.InvoiceNumber,
                                                    p.ItemNumber,
                                                    p.Warehouse
                                       ORDER BY p.PRSDT DESC
                                      ),
           k.InvoiceNumber,
           p.*
    INTO #tmpSAP_DT0_Order
    FROM #tmpSAP_DT0_vbrp p
        JOIN #tmpSAP_DT0_vbrk k
            ON k.BillNumber = p.BillNumber;

    DELETE FROM #tmpSAP_DT0_Order
    WHERE rownum <> 1;

    SELECT Company,
           ItemNumber,
           Warehouse,
           CreateDate,
           InvoiceNumber,
           InvoiceQuantity,
           InvoiceType,
           LocalAmount
    INTO #tmpCashOrderTrnTotal
    FROM [10.8.1.38].ETPEASV55.dbo.CashOrderTrn
    WHERE InvoiceType IN ( 31, 35 )
          AND CreateDate
          BETWEEN FORMAT(@_FromDate, 'yyyyMMdd') AND FORMAT(@_ToDate, 'yyyyMMdd')
          AND
          (
              LotNumber IS NULL
              OR LotNumber = ''
          )
          AND LocalAmount <> 0;

    SELECT Company,
           Warehouse,
           CreateDate,
           InvoiceNumber,
           SUM(LocalAmount) 'TotalLocalAmount'
    INTO #tmpCashOrderTrnTotalSum
    FROM #tmpCashOrderTrnTotal
    WHERE InvoiceType = 31
    GROUP BY Company,
             Warehouse,
             CreateDate,
             InvoiceNumber;

    SELECT CashOrderTrn.Company,
           CashOrderTrn.ItemNumber,
           CashOrderTrn.Warehouse,
           CashOrderTrn.CreateDate,
           CashOrderTrn.InvoiceNumber,
           SUM(CashOrderTrn.InvoiceQuantity) 'QtyOut',
           SUM((CashOrderTrn.LocalAmount
                - (CashOrderTrn.LocalAmount / ts.TotalLocalAmount * ISNULL(dis.LocalAmount, 0))
               )
               / CashOrderTrn.InvoiceQuantity / 1.1
              ) 'UnitNetPriceOut'
    INTO #tmpCashOrderTrn
    FROM #tmpCashOrderTrnTotal CashOrderTrn
        LEFT JOIN #tmpCashOrderTrnTotal dis
            ON dis.Company = CashOrderTrn.Company
               AND dis.InvoiceNumber = CashOrderTrn.InvoiceNumber
               AND dis.Warehouse = CashOrderTrn.Warehouse
               AND dis.CreateDate = CashOrderTrn.CreateDate
               AND dis.InvoiceType = 35
        JOIN #tmpCashOrderTrnTotalSum ts
            ON ts.Company = CashOrderTrn.Company
               AND ts.CreateDate = CashOrderTrn.CreateDate
               AND ts.InvoiceNumber = CashOrderTrn.InvoiceNumber
    WHERE CashOrderTrn.InvoiceType = 31
    GROUP BY CashOrderTrn.Company,
             CashOrderTrn.ItemNumber,
             CashOrderTrn.Warehouse,
             CashOrderTrn.CreateDate,
             CashOrderTrn.InvoiceNumber,
             ts.TotalLocalAmount,
             dis.LocalAmount;

    SELECT Company,
           ReturnItemNumber,
           Warehouse,
           CreateDate,
           SalesReturnNumber,
           ReturnQuantity,
           InvoiceType,
           LocalAmount
    INTO #tmpSalesReturnTrnTotal
    FROM [10.8.1.38].ETPEASV55.dbo.SalesReturnTrn
    WHERE InvoiceType IN ( 31, 35 )
          AND CreateDate
          BETWEEN FORMAT(@_FromDate, 'yyyyMMdd') AND FORMAT(@_ToDate, 'yyyyMMdd')
          AND LocalAmount <> 0;

    SELECT Company,
           Warehouse,
           CreateDate,
           SalesReturnNumber,
           SUM(LocalAmount) 'TotalLocalAmount'
    INTO #tmpSalesReturnTrnTotalSum
    FROM #tmpSalesReturnTrnTotal
    WHERE InvoiceType = 31
    GROUP BY Company,
             Warehouse,
             CreateDate,
             SalesReturnNumber;

    SELECT SalesReturnTrn.Company,
           SalesReturnTrn.ReturnItemNumber,
           SalesReturnTrn.Warehouse,
           SalesReturnTrn.CreateDate,
           SalesReturnTrn.SalesReturnNumber,
           SUM(SalesReturnTrn.ReturnQuantity) 'QtyIn',
           SUM((SalesReturnTrn.LocalAmount
                - (SalesReturnTrn.LocalAmount / ts.TotalLocalAmount * ISNULL(dis.LocalAmount, 0))
               )
               / SalesReturnTrn.ReturnQuantity / 1.1
              ) 'UnitNetPriceIn'
    INTO #tmpSalesReturnTrn
    FROM #tmpSalesReturnTrnTotal SalesReturnTrn
        LEFT JOIN #tmpSalesReturnTrnTotal dis
            ON dis.Company = SalesReturnTrn.Company
               AND dis.SalesReturnNumber = SalesReturnTrn.SalesReturnNumber
               AND dis.Warehouse = SalesReturnTrn.Warehouse
               AND dis.CreateDate = SalesReturnTrn.CreateDate
               AND dis.InvoiceType = 35
        JOIN #tmpSalesReturnTrnTotalSum ts
            ON ts.Company = SalesReturnTrn.Company
               AND ts.CreateDate = SalesReturnTrn.CreateDate
               AND ts.SalesReturnNumber = SalesReturnTrn.SalesReturnNumber
    WHERE SalesReturnTrn.InvoiceType = 31
    GROUP BY SalesReturnTrn.Company,
             SalesReturnTrn.ReturnItemNumber,
             SalesReturnTrn.Warehouse,
             SalesReturnTrn.CreateDate,
             SalesReturnTrn.SalesReturnNumber,
             ts.TotalLocalAmount,
             dis.LocalAmount;

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
    WHERE spl.ValidFrom <= FORMAT(@_ToDate, 'yyyyMMdd');

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
           spl.Warehouse
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
           spl.Specification1,
           spl.DimensionX,
           spl.DimensionY
    INTO #tmpProductRSF
    FROM [10.8.1.38].ETPEASV55.dbo.ProductRSF spl
    WHERE spl.BusinessArea = '10';

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
           LTRIM(RTRIM(cs.ShopNo)) 'ShopNo'
    INTO #tmpResult3
    FROM #tmpResult2 pr
        JOIN dbo.CK_Store cs
            ON cs.StoreCode LIKE '%' + pr.Warehouse + '%'
			AND cs.Active = 1
        LEFT JOIN #tmpAliasNumber an
            ON pr.ItemNumber = an.ItemNumber
               AND pr.Company = an.Company;

    SELECT 'Vietnam' AS 'Country',
           FORMAT(CONVERT(DATE, CONVERT(NVARCHAR, co.CreateDate), 112), 'yyyy/MM/dd') AS 'Date',
           pr.ShopNo AS 'Location',
           pr.Specification1 'Stylecode',
           pr.DimensionX 'Color',
           pr.DimensionY 'Size',
           pr.AliasNumber 'Barcode',
           InvoiceQuantity = CAST(ISNULL(co.QtyOut, 0) AS NUMERIC(36, 0)),
           p.UnitCost,
           Grossprice = CASE
                            WHEN pr.SalesCampaign = 'N' THEN
                                CAST(ROUND(pr.SalesPrice / 1.1, 0) AS NUMERIC(36, 0))
                            ELSE
                                CAST(pr.SalesPrice AS NUMERIC(36, 0))
                        END,
           NetPrice = CAST(ISNULL(co.UnitNetPriceOut, 0) AS NUMERIC(36, 0)),
           '' AS 'NetSGD',
           co.InvoiceNumber 'TranNo',
           '' AS 'staffcode'
    FROM #tmpResult3 pr
        JOIN #tmpCashOrderTrn co
            ON co.Company = pr.Company
               AND co.ItemNumber = pr.ItemNumber
               AND co.Warehouse = pr.Warehouse
        LEFT JOIN #tmpSAP_DT0_Order p
            ON p.ItemNumber = co.ItemNumber
               AND co.Warehouse = p.Warehouse
               AND CONVERT(NVARCHAR(2000), co.InvoiceNumber) = p.InvoiceNumber
    UNION
    SELECT 'Vietnam' AS 'Country',
           FORMAT(CONVERT(DATE, CONVERT(NVARCHAR, sr.CreateDate), 112), 'yyyy/MM/dd') AS 'Date',
           pr.ShopNo AS 'Location',
           pr.Specification1 'Stylecode',
           pr.DimensionX 'Color',
           pr.DimensionY 'Size',
           pr.AliasNumber 'Barcode',
           InvoiceQuantity = CAST(0 - ISNULL(sr.QtyIn, 0) AS NUMERIC(36, 0)),
           p.UnitCost,
           Grossprice = CASE
                            WHEN pr.SalesCampaign = 'N' THEN
                                CAST(ROUND(pr.SalesPrice / 1.1, 0) AS NUMERIC(36, 0))
                            ELSE
                                CAST(pr.SalesPrice AS NUMERIC(36, 0))
                        END,
           NetPrice = CAST(ISNULL(sr.UnitNetPriceIn, 0) AS NUMERIC(36, 0)),
           '' AS 'NetSGD',
           sr.SalesReturnNumber 'TranNo',
           '' AS 'staffcode'
    FROM #tmpResult3 pr
        JOIN #tmpSalesReturnTrn sr
            ON sr.Company = pr.Company
               AND sr.ReturnItemNumber = pr.ItemNumber
               AND sr.Warehouse = pr.Warehouse
        LEFT JOIN #tmpSAP_DT0_Order p
            ON p.ItemNumber = pr.ItemNumber
               AND pr.Warehouse = p.Warehouse
               AND CONVERT(NVARCHAR(2000), sr.SalesReturnNumber) = p.InvoiceNumber;

    DROP TABLE #tmpSAP_DT0_vbrk;
    DROP TABLE #tmpSAP_DT0_vbrp;
    DROP TABLE #tmpSAP_DT0_Order;
    DROP TABLE #tmpCashOrderTrnTotal;
    DROP TABLE #tmpCashOrderTrnTotalSum;
    DROP TABLE #tmpCashOrderTrn;
    DROP TABLE #tmpSalesReturnTrnTotal;
    DROP TABLE #tmpSalesReturnTrnTotalSum;
    DROP TABLE #tmpSalesReturnTrn;
    DROP TABLE #tmpSalesPriceList;
    DROP TABLE #tmpAliasNumber;
    DROP TABLE #tmpProductLocationBalance;
    DROP TABLE #tmpProductRSF;
    DROP TABLE #tmpResult1;
    DROP TABLE #tmpResult2;
    DROP TABLE #tmpResult3;
END;
GO