-- 更新 POS_Type
UPDATE __DRUG_DB__
SET "POS_Type" = CASE
    WHEN LEFT("panel_Mutation_POS", 1) ~ '^[A-Z]' THEN 'AminoAcid' -- a.a. 突變通常以大寫字母開頭 V600E
    WHEN LEFT("panel_Mutation_POS", 1) ~ '^[a-z]' THEN 'DNA' -- dna 突變通常以小寫字母開頭 ex14 del
    WHEN "panel_Mutation_POS" = '-' THEN '-' -- 沒有的話填 '-'
    ELSE NULL
END;

-- 更新 POS_numeric
UPDATE __DRUG_DB__
SET "POS_numeric" = CASE
    WHEN "panel_Mutation_POS" = '-' THEN '-'
    ELSE regexp_replace("panel_Mutation_POS", '[^0-9]', '', 'g')
END;

-- 更新 POS_Change
UPDATE __DRUG_DB__
SET "POS_Change" = CASE
    WHEN "POS_Type" = 'AminoAcid' THEN
        regexp_replace("panel_Mutation_POS", '[0-9]+', '/')
    WHEN "POS_Type" = 'DNA' THEN
        split_part("panel_Mutation_POS", ' ', 2)
    ELSE NULL
END;

-- 再更新 POS_Change
UPDATE __DRUG_DB__
SET "POS_Change" = CASE
    WHEN "POS_Change" = '-' THEN '-'
    WHEN position('/' in "POS_Change") > 0 THEN
        CASE 
            WHEN split_part("POS_Change", '/', 1) = '' AND split_part("POS_Change", '/', 2) = '' THEN '-/-'
            WHEN split_part("POS_Change", '/', 1) = '' THEN '-/' || split_part("POS_Change", '/', 2)
            WHEN split_part("POS_Change", '/', 2) = '' THEN split_part("POS_Change", '/', 1) || '/-'
            ELSE "POS_Change"
        END
    ELSE
        CASE
            WHEN "POS_Change" = 'del' THEN 'deletion'
            WHEN "POS_Change" = 'ins' THEN 'insertion'
            WHEN "POS_Change" = 'sub' THEN 'substitution'
            WHEN "POS_Change" = 'snv' THEN 'SNV'
            ELSE "POS_Change"
        END
END;

-- 若 POS_Change 為 DNA 突變, 對照 accurate_db 的 VARIANT_CLASS 欄位
-- VARIANT_CLASS 有 4 種: SNV, substitution, insertion, deletion
-- 若 POS_Change 為 a.a. 突變, 對照 accurate_db 的 Amino_acids 欄位
-- Amino_acids 的格式為 aa/aa 或 -/aa 或 aa/-