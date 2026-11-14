-- 既有库升级：为认领/围栏/租约列补齐；新库由001与create_all直接建全。
ALTER TABLE rank_test_runs ADD COLUMN claim_token VARCHAR(64) NOT NULL DEFAULT '';
ALTER TABLE rank_test_runs ADD COLUMN claimed_at DATETIME NULL;
ALTER TABLE rank_test_runs ADD COLUMN worker_id VARCHAR(80) NOT NULL DEFAULT '';
