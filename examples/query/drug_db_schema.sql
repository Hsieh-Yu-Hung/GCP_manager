DROP TABLE IF EXISTS DB_Relation;
DROP TABLE IF EXISTS __DRUG_DB_NAME__;
CREATE TABLE __DRUG_DB_NAME__ (
    "drug_db_id" BIGSERIAL PRIMARY KEY,
    "Modify_record" VARCHAR(255),
    "Modify_date" TIMESTAMP,
    "藥物成份" VARCHAR(255),
    "許可證字號" VARCHAR(255),
    "藥物名" VARCHAR(255),
    "癌症別" VARCHAR(255),
    "ACCUiNPanel" VARCHAR(255),
    "免疫組織化學染色" VARCHAR(255),
    "其他檢測_檢驗指標" VARCHAR(255),
    "合併用藥" VARCHAR(255),
    "panel_Associated_Gene" VARCHAR(255),
    "panel_Condition" VARCHAR(255),
    "panel_Type" VARCHAR(255),
    "panel_Mutation_POS" VARCHAR(255),
    "POS_Type" VARCHAR(255),
    "POS_numeric" VARCHAR(255),
    "POS_Change" VARCHAR(255),
    "panel_Note" VARCHAR(255) 
);
CREATE TABLE db_relation (
    "relation_id" BIGSERIAL PRIMARY KEY,
    "drug_db_id" BIGINT,
    "bio_db_id" CHAR(32),
    "Related_Gene" TEXT,
    "ACCUiNPanel" TEXT,
    FOREIGN KEY ("drug_db_id") REFERENCES __DRUG_DB_NAME__("drug_db_id"),
    FOREIGN KEY ("bio_db_id") REFERENCES __BIO_DB_NAME__(__BIO_DB_ID__)
);