USE [CKDATA]
GO

IF EXISTS ( SELECT  *
            FROM    sys.objects
           WHERE   type = 'P'
                   AND name = 'GetDataUploadSOH' )
   DROP PROCEDURE GetDataUploadSOH;
GO
CREATE PROCEDURE [dbo].[GetDataUploadSOH]
AS
BEGIN

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
           spl.Warehouse,
           spl.BalanceApproved
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
           spl.Specification1
    INTO #tmpProductRSF
    FROM [10.8.1.123].ETPEASV55.dbo.ProductRSF spl;

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
           spl.SalesCampaign,
           spl.SalesPrice,
           pr.BalanceApproved
    INTO #tmpResult2
    FROM #tmpResult1 pr
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
           '' AS 'UnitLandedCost',
           UnitGrossPrice = CASE
                                WHEN pr.SalesCampaign = 'N' THEN
                                    CAST(ROUND(pr.SalesPrice / 1.1, 2) AS NUMERIC(36, 2))
                                ELSE
                                    CAST(pr.SalesPrice AS NUMERIC(36, 2))
                            END,
           CAST(ROUND(pr.SalesPrice / 1.1, 2) AS NUMERIC(36, 2)) AS 'UnitNetPrice'
    FROM #tmpResult2 pr
        JOIN dbo.CK_Store cs
            ON cs.StoreCode = pr.Warehouse
        LEFT JOIN #tmpAliasNumber an
            ON pr.ItemNumber = an.ItemNumber
               AND pr.Company = an.Company;

    DROP TABLE #tmpSalesPriceList;
    DROP TABLE #tmpAliasNumber;
    DROP TABLE #tmpProductLocationBalance;
    DROP TABLE #tmpProductRSF;
    DROP TABLE #tmpResult1;
    DROP TABLE #tmpResult2;

END;
GO