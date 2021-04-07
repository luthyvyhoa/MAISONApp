IF NOT EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'IMEX_JobProcess')
          AND type IN ( N'U' )
) 
BEGIN
    CREATE TABLE [dbo].[IMEX_JobProcess]
    (
        [ID] [INT] IDENTITY(1, 1) NOT NULL,
        [Job] INT NOT NULL,
        [Step] INT NOT NULL,
        [Status] CHAR(1) NOT NULL,
		[CreatedDate] DATETIME NULL,
		[LastModifiedDate] DATETIME NULL,
		[Message] NVARCHAR(MAX) NULL
        CONSTRAINT [PK_IMEX_JobProcess]
            PRIMARY KEY CLUSTERED (
                                      [ID] ASC
                                  )
            WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON,
                  ALLOW_PAGE_LOCKS = ON
                 ) ON [PRIMARY]
    ) ON [PRIMARY];
END;

GO
SET ANSI_PADDING OFF;
GO
