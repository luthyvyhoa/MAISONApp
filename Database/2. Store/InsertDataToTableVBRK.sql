IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'InsertDataToTableVBRK'
)
    DROP PROCEDURE InsertDataToTableVBRK;
GO
CREATE PROCEDURE [dbo].[InsertDataToTableVBRK]
AS
BEGIN

    DECLARE @_MinDate DATETIME;
    DECLARE @_MaxDate DATETIME;

    SELECT @_MinDate = MIN(CONVERT(DATE, FKDAT, 104)),
           @_MaxDate = MAX(CONVERT(DATE, FKDAT, 104))
    FROM dbo.TMP_DT0_vbrk;

    DELETE k
    FROM dbo.SAP_DT0_vbrk k
    WHERE CONVERT(DATE, FKDAT, 104)
    BETWEEN @_MinDate AND @_MaxDate;

    INSERT INTO dbo.SAP_DT0_vbrk
    (
        VBELN,
        FKART,
        FKTYP,
        VBTYP,
        WAERK,
        VKORG,
        VTWEG,
        KALSM,
        KNUMV,
        VSBED,
        FKDAT,
        BELNR,
        KDGRP,
        BZIRK,
        PLTYP,
        RFBSK,
        KURRF,
        ZTERM,
        KTGRD,
        BUKRS,
        TAXK1,
        NETWR,
        ERNAM,
        ERZET,
        ERDAT,
        STAFO,
        KUNRG,
        KUNAG,
        AEDAT,
        SFAKN,
        STCEG,
        FKART_RL,
        FKDAT_RL,
        MANSP,
        SPART,
        KKBER,
        KNKLI,
        CMWAE,
        CMKUF,
        HITYP_PR,
        BSTNK_VF,
        XBLNR,
        ZUONR,
        MWSBK,
        FKSTO,
        KURRF_DAT,
        KIDNO,
        BUPLA,
        KNUMA,
        LAND1
    )
    SELECT VBELN,
           FKART,
           FKTYP,
           VBTYP,
           WAERK,
           VKORG,
           VTWEG,
           KALSM,
           KNUMV,
           VSBED,
           FKDAT,
           BELNR,
           KDGRP,
           BZIRK,
           PLTYP,
           RFBSK,
           KURRF,
           ZTERM,
           KTGRD,
           BUKRS,
           TAXK1,
           NETWR,
           ERNAM,
           ERZET,
           ERDAT,
           STAFO,
           KUNRG,
           KUNAG,
           AEDAT,
           SFAKN,
           STCEG,
           FKART_RL,
           FKDAT_RL,
           MANSP,
           SPART,
           KKBER,
           KNKLI,
           CMWAE,
           CMKUF,
           HITYP_PR,
           BSTNK_VF,
           XBLNR,
           ZUONR,
           MWSBK,
           FKSTO,
           KURRF_DAT,
           KIDNO,
           BUPLA,
           KNUMA,
           LAND1
    FROM dbo.TMP_DT0_vbrk;

	TRUNCATE TABLE dbo.TMP_DT0_vbrk;
END;