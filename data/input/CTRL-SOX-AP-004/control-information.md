# Control ID: CTRL-SOX-AP-004

## Risk Statement
Unauthorized approval of high-value invoices circumvents the firm's Delegation of Authority matrix, risking financial fraud and SOX 404 violations.

## Source Information
- ap_invoices.xlsx (Accounts Payable invoice ledger)
- employee_titles.xlsx (HR mapping of employees to their corporate titles)

## Approach Followed
1. Filter the `ap_invoices` population to only include invoices where the `invoice_amount` is strictly greater than $100,000.
2. Join to `employee_titles` on `approver_id`.
3. Verify that the `approver_title` of the person who approved the invoice is strictly in the allowed executive list: ['SVP', 'EVP', 'CEO', 'CFO'].