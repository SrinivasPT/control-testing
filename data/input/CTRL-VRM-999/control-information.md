# Control ID: CTRL-VRM-999

## Risk Statement
Utilizing critical third-party vendors who have failed security assessments or possess expired SOC2 reports violates OCC and GDPR third-party risk management frameworks.

## Source Information
- active_contracts.xlsx (Procurement log of active vendor engagements)
- vendor_master.xlsx (Metadata defining vendor criticality)
- security_assessments.xlsx (Log of vendor cybersecurity audits)

## Approach Followed
1. Filter `active_contracts` to engagements where `contract_status` is 'ACTIVE'.
2. Join the population to `vendor_master` matching on `vendor_id`.
3. Filter the population again to only include vendors where `criticality` is 'TIER_1'.
4. Join the resulting high-risk population to `security_assessments` matching on `vendor_id`.
5. Verify that the most recent `assessment_status` is 'PASSED'.
6. Verify that the security `expiration_date` is strictly AFTER the contract `renewal_date` (ensuring the vendor's security clearance outlasts the current contract term).