# Day 15 go/no-go checklist (Thu 15 Oct)

Run in the morning, before any build work. Record go / no-go per row and tell Anil at the 16 Oct review.

## Workspace setup

- [ ] Workspace created in a region that supports serverless, on **Trial (Premium – 14-Days Free DBUs)**
- [ ] Azure budget alerts active; Cost Management shows credit balance and expiry
- [ ] Catalogs `sales_dev` and `sales_prod`, schemas, and the landing volume created
- [ ] Serverless SQL warehouse starts; smallest size, short auto-stop
- [ ] Separate Python environment for Databricks Connect created (not in the main project env)
- [ ] Library versions compared with serverless environment; drift noted in `decisions.md`

## Feature checks

| Check | Pass means | If no | Result |
|---|---|---|---|
| `ai_query` returns text from a Databricks-hosted model | Phase 2 narratives work | Enable cross-Geo processing after sign-off; else fixed text templates | |
| `ai_forecast` runs (Predictive AI Functions preview enabled) | Forecast variance works | Prior-period and same-period-last-year only | |
| SupervisorAgent tile available | Phase 3 uses native supervisor | Phase 3 = deck job only; chat stays in Genie | |
| Genie space can be created | Phase 1 as planned | **Blocker:** escalate to Anil same day | |
| File-arrival trigger can be set on the volume | Daily simulation as designed | Scheduled pipeline 10 min after `simulate_day` | |
| Mac code runs unchanged | Portability works | Fix drift on 15–16 Oct | |
