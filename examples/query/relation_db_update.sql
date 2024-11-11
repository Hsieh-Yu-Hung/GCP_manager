-- 清空 DB_Relation 表
TRUNCATE TABLE __DB_RELATION__;

-- 插入新的資料
INSERT INTO __DB_RELATION__ (
    "drug_db_id",
    "bio_db_id",
    "Related_Gene",
    "ACCUiNPanel"
)
SELECT 
    drug."drug_db_id",
    bio."uniq_identifier",
    bio."GeneSymbol",
    drug."ACCUiNPanel"
FROM 
    __DRUG_DB__ drug
JOIN 
    __BIO_DB__ bio
ON 
    drug."panel_Associated_Gene" = bio."GeneSymbol"
    AND (
        (drug."POS_Type" = 'DNA' AND bio."Mutation_Exon_number" = drug."POS_numeric" AND bio."VARIANT_CLASS" = drug."POS_Change")
        OR
        (drug."POS_Type" = 'AminoAcid' AND drug."POS_Change" = bio."Amino_acids" AND drug."POS_numeric" = bio."Protein_position")
    )