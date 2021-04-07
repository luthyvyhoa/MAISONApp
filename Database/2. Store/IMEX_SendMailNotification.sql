IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'IMEX_SendMailNotification'
)
    DROP PROCEDURE IMEX_SendMailNotification;
GO
CREATE PROCEDURE [dbo].[IMEX_SendMailNotification]
AS
BEGIN

    DECLARE @xml NVARCHAR(MAX);
    DECLARE @body NVARCHAR(MAX);

    SET @xml = CAST(
               (
                   SELECT CountryCode AS 'td',
                          '',
                          PhoneNo AS 'td',
                          '',
                          Content AS 'td',
                          '',
                          UserName AS 'td'
                   FROM dbo.SMS
                   WHERE Status = 0
                   FOR XML PATH('tr'), ELEMENTS
               ) AS NVARCHAR(MAX));

    SET @body
        = N'<html><body><H3>Detail Notification</H3>
			<table border = 1> <tr>
			<th> Country Code </th> <th> Phone </th> <th> Content </th> <th> User Name </th></tr>';

    SET @body = @body + @xml + N'</table></body></html>';

    EXEC msdb.dbo.sp_send_dbmail @profile_name = 'haphan',
                                 @body = @body,
                                 @body_format = 'HTML',
                                 @recipients = 'hoalu@maisonjsc.com',
                                 @subject = 'Notification Job Auto';

    UPDATE dbo.SMS
    SET Status = 1
    WHERE Status = 0;
END;