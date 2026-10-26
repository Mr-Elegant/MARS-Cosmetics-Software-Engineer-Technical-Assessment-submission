# MARS Order & Inventory Sync Service

System design submission for the MARS Cosmetics backend engineering assessment. The proposal describes an order and inventory synchronization service for a 300-orders-per-minute peak, designed for a three-engineer team, a six-week v1 delivery, and a ₹50,000 monthly infrastructure budget.

## Submission files

- **[Final system design](DESIGN_FINAL.md)** — architecture, order and inventory correctness, channel integrations, failure recovery, operations, cost estimate, and delivery plan.
- **[Final design PDF](DESIGN_FINAL.pdf)** — PDF version of the final system design.
- **[Architecture diagram](architecture.png)** — deployment topology and data-flow overview embedded in the Markdown design.
- **[NestJS reference implementation](MARS_NestJS_Reference_Code.zip)** — runnable reference backend illustrating the core transactional and outbox design.
- **[Express reference implementation](MARS_Express_Reference_Code.zip)** — alternate Express reference backend with the same core domain approach.
- **[Interview questions guide](MARS_Interview_Questions_Guide.pdf)** — preparation guide covering the design and likely follow-up questions.

The standalone `DESIGN.md` and `DESIGN.pdf` files are earlier design versions. **Use `DESIGN_FINAL.md` and `DESIGN_FINAL.pdf` as the final submission.**

## Proposed architecture

The design uses a NestJS modular monolith with PostgreSQL as the single authority for orders, inventory, reservations, the audit ledger, and durable delivery jobs. Order ingestion and inventory reservation are handled in a short database transaction. A transactional outbox delivers updates asynchronously to sales channels and the ERP, with separate worker pools to isolate downstream failures.

The proposed deployment uses two EC2 application hosts across Availability Zones behind an Application Load Balancer, an isolated Single-AZ RDS PostgreSQL database, Redis for rate limiting, S3 for Blinkit CSV snapshots, and CloudWatch for monitoring. Conservative channel stock caps and buffers reduce exposure to overselling; the design explicitly treats them as risk controls rather than a guarantee against external marketplace races.

## Reference code

The two ZIP archives are separate local reference projects. Each has its own README with setup instructions, requirements, and API examples. Marketplace and ERP integrations in these projects are simulations; they do not call live seller or ERP APIs. The source archives illustrate the design and should not be read as the production deployment described in the assessment.

## Key design decisions

- PostgreSQL transactions, sorted SKU row locks, and database constraints protect internal inventory consistency.
- Unique event, order, and ledger keys make replays and retries safe.
- A PostgreSQL transactional outbox avoids a database/message-broker dual-write problem.
- Independent delivery workers, rate limits, retries, and reconciliation handle downstream failures.
- Single-AZ RDS is a deliberate v1 cost trade-off; the design documents its availability and recovery limits.
- Channel stock caps target a stock-cancellation SLO and require validation against real flash-sale data.

## Repository contents

```text
.
├── DESIGN_FINAL.md
├── DESIGN_FINAL.pdf
├── architecture.png
├── MARS_NestJS_Reference_Code.zip
├── MARS_Express_Reference_Code.zip
└── MARS_Interview_Questions_Guide.pdf
```

Other design drafts and exports may also be present in the submission folder; the files listed above are the primary final deliverables.

## Author

**Preet Karwal** — Backend Engineering
