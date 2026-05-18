# Allianz Demo — Iteration 2 Tasks

## Refactor: external feeds → bronze landing volume → bronze tables
- [~] Update `external_feeds.py` to write parquet to UC volume
- [ ] Create `ingest_external.py` to load volume → `allianz_bronze.*_raw`
- [ ] Update DLT pipeline: drop `ext_schema` config, read external bronze from `allianz_bronze`
- [ ] Update DAB jobs to include the new ingest step
- [ ] Migrate / drop `allianz_ext` schema

## P&C industry views (joined gold marts)
- [ ] `gold.policy_customer_360` (policy + customer + geo + agent)
- [ ] `gold.claim_360` (claim + policy + customer + geo + peril)
- [ ] `gold.loss_ratio_by_segment` (segment-level loss ratios)
- [ ] `gold.book_health` (frequency, severity, retention, growth)
- [ ] `sql/` directory with re-usable analytic SQL files

## Genie space — P&C industry benchmarks + FAQ
- [ ] Add benchmark instructions (combined ratio targets, severity, frequency)
- [ ] Replace example questions with industry-standard P&C questions
- [ ] Add FAQ instruction
- [ ] Update table list (drop allianz_ext entries; include new gold views)
