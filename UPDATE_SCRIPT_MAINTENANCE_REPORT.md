# Update Script Maintenance Report

Date: 2026-03-04

- Re-ran `scripts/home_affordability.py` to verify the updater still executes against the current London Datastore XLSX endpoint.
- Local output already matched current year coverage (through 2024), so no data file changes were produced in this run.
- Updated GitHub Actions workflow at `.github/workflows/actions.yml` to improve automation reliability:
  - removed push/PR triggers and kept scheduled/manual execution,
  - added explicit `permissions: contents: write`,
  - upgraded to `actions/checkout@v4` and `actions/setup-python@v5`,
  - simplified dependency/script steps to run without per-job virtualenv shell activation.
- Upstream annual 2025 figures are expected in March 2026; this workflow now keeps the repository ready for that release cadence.
