IF NOT EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'IMEX_Parameter')
          AND type IN ( N'U' )
)
BEGIN
    CREATE TABLE [dbo].[IMEX_Parameter]
    (
        [ID] [INT] IDENTITY(1, 1) NOT NULL,
        [Field] [NVARCHAR](50) NOT NULL,
        [Type] CHAR(1) NOT NULL,
        [Value] [NVARCHAR](200) NOT NULL,
        CONSTRAINT [PK_IMEX_Parameter]
            PRIMARY KEY CLUSTERED (
                                      [Field] ASC,
                                      [Value] ASC
                                  )
            WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON,
                  ALLOW_PAGE_LOCKS = ON
                 ) ON [PRIMARY]
    ) ON [PRIMARY];
END;

GO
SET ANSI_PADDING OFF;
GO
