USE [CKDATA];
GO

IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'GetDataAPI_Footfall'
)
    DROP PROCEDURE GetDataAPI_Footfall;
GO
CREATE PROCEDURE [dbo].[GetDataAPI_Footfall]
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

    SELECT 'VietNam' AS 'Country',
           FORMAT(TrafficDate, 'yyyy-MM-dd') 'Date',
           CK_Store.ShopNo AS 'LocationCode',
           SUM(InCount) AS 'Incoming'
    FROM [10.8.1.114].MS_DW_STAGE.dbo.API_Footfall
        JOIN dbo.CK_Store
            ON CK_Store.StoreCode LIKE '%' + API_Footfall.ShopNo + '%'
               AND Active = 1
    WHERE CONVERT(DATE, TrafficDate)
          BETWEEN @_FromDate AND @_ToDate
          AND DATEPART(HOUR, TrafficDate) >= 8
    GROUP BY FORMAT(TrafficDate, 'yyyy-MM-dd'),
             CK_Store.ShopNo;

END;
GO