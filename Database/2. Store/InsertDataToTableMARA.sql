IF EXISTS
(
    SELECT *
    FROM sys.objects
    WHERE type = 'P'
          AND name = 'InsertDataToTableMARA'
)
    DROP PROCEDURE InsertDataToTableMARA;
GO
CREATE PROCEDURE [dbo].[InsertDataToTableMARA]
AS
BEGIN

    DECLARE @_MinDate DATETIME2;
    DECLARE @_MaxDate DATETIME2;

    SELECT @_MinDate = MIN(CONVERT(DATE, sim.ERSDA, 104)),
           @_MaxDate = MAX(CONVERT(DATE, sim.ERSDA, 104))
    FROM dbo.TMP_DM0_mara sim;

    DELETE sim
    FROM dbo.SAP_DM0_mara sim
    WHERE CONVERT(DATE, sim.ERSDA, 104)
    BETWEEN @_MinDate AND @_MaxDate;

    INSERT INTO dbo.SAP_DM0_mara
    (
        MATNR,
        ERSDA,
        ERNAM,
        LAEDA,
        PSTAT,
        MTART,
        MBRSH,
        MATKL,
        BISMT,
        MEINS,
        BRGEW,
        NTGEW,
        GEWEI,
        VOLUM,
        RAUBE,
        SPART,
        LAENG,
        BREIT,
        HOEHE,
        MSTDE,
        MSTDV,
        TAKLV,
        MHDRZ,
        MHDHB,
        MHDLP,
        INHAL,
        VPREH,
        INHBR,
        COMPL,
        IPRKZ,
        WHSTC,
        WHMATGR,
        [/BEV1/LULEINH],
        [/BEV1/LULDEGRP],
        VOLEH,
        AENAM,
        MFRPN,
        MFRNR,
        GROES,
        LVORM,
        AESZN,
        FERTH,
        EXTWG,
        PRDHA
    )
    SELECT MATNR,
           ERSDA,
           ERNAM,
           LAEDA,
           PSTAT,
           MTART,
           MBRSH,
           MATKL,
           BISMT,
           MEINS,
           BRGEW,
           NTGEW,
           GEWEI,
           VOLUM,
           RAUBE,
           SPART,
           LAENG,
           BREIT,
           HOEHE,
           MSTDE,
           MSTDV,
           TAKLV,
           MHDRZ,
           MHDHB,
           MHDLP,
           INHAL,
           VPREH,
           INHBR,
           COMPL,
           IPRKZ,
           WHSTC,
           WHMATGR,
           [/BEV1/LULEINH],
           [/BEV1/LULDEGRP],
           VOLEH,
           AENAM,
           MFRPN,
           MFRNR,
           GROES,
           LVORM,
           AESZN,
           FERTH,
           EXTWG,
           PRDHA
    FROM dbo.TMP_DM0_mara;

    TRUNCATE TABLE dbo.TMP_DM0_mara;
END;