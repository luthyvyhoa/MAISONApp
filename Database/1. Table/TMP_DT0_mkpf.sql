USE MaisonDW;
IF NOT EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'TMP_DT0_mkpf')
          AND type IN ( N'U' )
)
BEGIN
    CREATE TABLE [dbo].[TMP_DT0_mkpf]
    (
        [ID] [INT] IDENTITY(1, 1) NOT NULL,
        MBLNR [NVARCHAR](2000) NULL,
        MJAHR [NVARCHAR](2000) NULL,
        VGART [NVARCHAR](2000) NULL,
        BLART [NVARCHAR](2000) NULL,
        BLAUM [NVARCHAR](2000) NULL,
        BLDAT [NVARCHAR](2000) NULL,
        BUDAT [NVARCHAR](2000) NULL,
        CPUDT [NVARCHAR](2000) NULL,
        CPUTM [NVARCHAR](2000) NULL,
        USNAM [NVARCHAR](2000) NULL,
        XBLNR [NVARCHAR](2000) NULL,
        BKTXT [NVARCHAR](2000) NULL
            CONSTRAINT [PK_TMP_DT0_mkpf]
            PRIMARY KEY CLUSTERED ([ID] ASC)
            WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON,
                  ALLOW_PAGE_LOCKS = ON
                 ) ON [PRIMARY]
    ) ON [PRIMARY];
END;

GO
SET ANSI_PADDING OFF;
GO
