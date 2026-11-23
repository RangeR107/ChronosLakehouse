# Chronos – Project Specification

Nov 22, 2026 · @RangeR · Revision 5

A batch lakehouse on AWS that replays real Olist marketplace orders as a late, messy event feed, turns them into a trusted star schema, and publishes only complete, audited releases.

## Summary

Chronos is a batch lakehouse on AWS. It replays about 100,000 real Olist marketplace orders as a daily feed of events. Some events arrive late, twice, out of order or briefly wrong, and the pipeline turns the feed into a trusted star schema anyway. Its quality gate, DataSentinel, publishes a day's data only after it passes its checks, and dashboards switch from one complete release to the next, never to half of one.

Every business fact comes from Olist. The only simulated part is how and when data arrives. Every simulated defect is recorded and later reversed, so once every delayed event has arrived, the warehouse's business tables must equal what Olist recorded, and a test checks exactly that (see What's real and what's simulated).

**What it proves, in interview terms**

- **Dimensional modeling:** declared grains, an accumulating snapshot, role-playing date and geography dimensions, SCD Type 2 with honestly labelled inferred dates, and inferred members.
- **Pipeline correctness:** idempotent reruns; late, duplicate and out-of-order events; corrections; late dimension changes; and metrics "as first reported" versus "as known now".
- **Data quality and releases:** write-audit-publish on Iceberg branches, a release manifest that keeps tables consistent for readers, severity levels that separate bad source rows from bad runs, and measured detection accuracy.
- **AWS:** S3, Glue, Apache Iceberg, Athena on the AWS free plan, with PostgreSQL serving the dashboards, orchestrated by Airflow and defined in Terraform. The whole project costs nothing.

**Scope rule:** every component answers a business question or protects correctness. EMR, streaming and search services stay out: Glue already provides managed Spark, and no question here needs results faster than daily.

**How claims are backed:** tool and pricing facts link to their sources. Dataset numbers come from running the profiling scripts on the Kaggle download (see Data sources). Every design choice, with the alternatives rejected, is in the Decision log, and every known weakness is in Known limitations. Specialist terms are explained in the Glossary.

## At a glance

```text
Replay generator --(hourly JSONL + completion marker)--> S3 landing/
PostgreSQL master (customers, sellers, products) --(high-water-mark extract)--+
                                                                              |
Airflow, one run per business date:                                           v
  Bronze (append-only events + file manifest)
    -> Silver (typed, deduplicated; bad rows flagged or quarantined) -> Quarantine
    -> Gold on the run's Iceberg branch (star schema built with MERGE; main untouched)
    -> DataSentinel audit: all blocking checks pass?
         no  -> Blocked: branch kept, report written, alert sent, main unchanged
         yes -> Publish a release: fast-forward each table, manifest records its snapshots
              -> Amazon Athena (ad-hoc queries, audits, time travel)
              -> PostgreSQL serving (one transaction per release) -> Dashboards (Streamlit)
```

Each run writes silver and gold to its own Iceberg branch. DataSentinel audits that branch. If nothing blocks, each table's main branch is fast-forwarded and a release manifest records the exact snapshot of every table. PostgreSQL loads the release in one transaction, so dashboards see a whole release or none of it. Bad source rows are flagged or quarantined, never silently dropped.

## What's real and what's simulated

Every business fact is real. The only thing simulated is the arrival problems a production pipeline faces, because no public dataset contains them: Olist, like every public dataset, is a clean final copy, not the messy feed that produced it.

| Class | Meaning | Examples | Used in business metrics? |
| --- | --- | --- | --- |
| Observed | A value exactly as Olist recorded it | Prices, freight, payments, statuses, review scores, every timestamp, each order's delivery address | Yes |
| Derived | Computed from observed values by a written, deterministic rule | Events built from timestamps, IBGE regions, late flags, a customer's address history | Yes, with the rule documented in this spec |
| Simulated | Created by the generator to exercise the pipeline | When events arrive (emitted\_at), duplicates, out-of-order arrival, a temporarily wrong value later corrected, a dimension row that arrives late, schema changes, anomalies for the evaluation | Never |

**Rules that keep simulated data out of the evidence**

1. **Nothing about the business is invented.** There are no returns, seller moves, seller tiers, cancellation times or scaled-up volumes. Questions that would need them are out of scope (see Known limitations).
2. **Everything simulated is recorded.** A ground-truth manifest stores each injection's id, type, affected event or file and parameters, plus the generator's random seed, so the same seed reproduces the same replay.
3. **The main replay injects only reversible defects.** Every late event eventually arrives, every duplicate is a copy, every wrong value is followed by a correction carrying the observed value, and every early fact's dimension row eventually arrives. After the final replayed day and a drain period long enough for every delayed event and row to arrive, a truth test compares every current-state business table with an independent build from the Olist files, and any difference fails the build (see Testing and evaluation).
4. **Irreversible anomalies live in a separate evaluation replay.** Lost files, drift and broken schemas run only in their own catalog namespace (chronos\_eval) and serving schema, which no business dashboard reads. They exist to be blocked, and to measure DataSentinel.
5. **Pipeline metrics are labelled as such.** Restatements (question 8) depend on simulated arrival times, so they describe this pipeline's behaviour, not Olist's business.

**Derived values and the rule behind each**

- **Events:** one event per observed timestamp, with event\_time equal to the Olist timestamp.
- **Cancellations:** Olist records that an order ended canceled or unavailable, but not when. These events therefore carry no event time, their arrival time is a documented simulation rule, and no metric uses cancellation timing.
- **Customer address history:** built from the sequence of each person's orders. A new version's effective\_from is the purchase time of the first order seen with the new address. That date is inferred: the person may have moved earlier, or may have sent that order to another address.
- **Regions:** the IBGE mapping from state to region.

## Business questions and metric definitions

Every dashboard number comes from one written definition in docs/metrics.yml, and dashboards read only gold tables that implement it. The shared definitions come first, because most arguments about a metric are really arguments about these. measure\_spec.py is a reference implementation of questions 1 to 7: it prints the headline numbers below and writes every breakdown, at each metric's own grain, to CSV files that the metric regression test compares with gold. Question 8 depends on the replay's publication history, so only the pipeline can produce it.

**Shared definitions**

| Term | Definition |
| --- | --- |
| Business date | The calendar date of the timestamp the metric is about, as recorded. Olist timestamps carry no time zone: treat them as Brasília local time and never convert them |
| Reliable window | Purchases from 1 Jan 2017 to 31 Aug 2018. Late 2016 is sparse, and Sep and Oct 2018 hold only 20 orders between them, so every metric counts only orders purchased inside the window |
| Delivered order | Status delivered and a customer delivery date present. The 8 delivered orders without a date are flagged and left out of delivery metrics |
| Final status | delivered, canceled or unavailable. The 1,729 orders still shipped, invoiced, processing, created or approved when the data was extracted are open, and are left out of any denominator that needs an outcome |
| Qualifying purchase | An order whose status is not canceled or unavailable. Open orders count, because the customer did buy; this is a deliberate choice, used by question 5 |
| Merchandise revenue | Sum of item prices in Brazilian reais (BRL) for orders whose status is not canceled or unavailable, by purchase date. Freight is reported separately; payment values are used only to reconcile payments against items |
| Region | IBGE region of a location: the order's delivery address for customer metrics, the seller's address for seller metrics |
| Order review score | The order's review. When an order has several (547 orders in the Kaggle download), take the latest by creation date, then by answer time, then the highest review\_id. The tie-breakers matter: creation dates have no time of day, and 156 of those 547 orders tie on it |

**The eight questions**

| # | Question | Metric definition | Built from |
| --- | --- | --- | --- |
| 1 | Where are deliveries late? | An order is late when its delivery date is after its estimated delivery date, compared by calendar day because the estimate has no time of day. Late rate = late delivered orders divided by delivered orders, by delivery region and delivery month | fact\_order\_fulfillment, dim\_geography (delivery role), dim\_date |
| 2 | How cautious are delivery promises? | Days early = estimated delivery date minus delivery date, in calendar days (negative means late). Median, 10th and 90th percentiles, and the share at least 7 days early, by delivery region and month | fact\_order\_fulfillment, dim\_geography, dim\_date |
| 3 | Which sellers consistently hand over late? | An item is handed over late when its order's carrier handover time is after the item's shipping limit time. Only single-seller orders count, because the handover time is recorded per order, and rows flagged as handed over before purchase are excluded. Late rate per seller per shipping-limit month, for sellers with at least 20 items that month. In each month with at least 20 such sellers, rank them by late rate with PERCENT\_RANK, lowest first; ties share a rank. Worst decile = percent rank of 0.9 or more. Consistently late = in the worst decile in at least 3 of the seller's last 6 ranked months, evaluated every month. A seller needs 6 ranked months before being classified; until then they show as not enough history | fact\_order\_items, fact\_order\_fulfillment, dim\_seller |
| 4 | Does late delivery go with lower satisfaction? | Average order review score and share of 1-star reviews, late versus on-time delivered orders, by delivery month. This is observational: report the association, never a cause | fact\_order\_fulfillment, fact\_reviews |
| 5 | Do customers come back? | A cohort is the month of a person's (customer\_unique\_id) first qualifying purchase, for people whose first qualifying purchase is inside the reliable window. People who first bought in 2016 are existing customers and belong to no cohort. A repeat within N days (N = 90 and 180) is another qualifying purchase with a later purchase time within N days; orders placed in the same second are one basket. Only people whose first purchase time plus N days is no later than the end of the window (1 Sep 2018 00:00) count in the denominator; later cohorts are shown as incomplete, not as zero | fact\_order\_fulfillment, dim\_customer, dim\_date |
| 6 | How many approved orders end cancelled? | Orders with a payment approval and final status canceled, divided by orders with a payment approval and any final status, by purchase month; unavailable orders are reported beside it. The data says that an approved order was cancelled, not when, so the metric makes no claim about timing | fact\_order\_fulfillment |
| 7 | Which categories earn well but satisfy poorly? | Per category and purchase quarter: merchandise revenue, and the low-score share (1- and 2-star order reviews divided by reviewed orders) over orders whose items all share one category. Top 20% by revenue means a cumulative distribution (CUME\_DIST) of 0.2 or less when every category with revenue is sorted by revenue, highest first. Review statistics are joined only after ranking, and a category with no reviewed orders can't be flagged. Flag a top-revenue category when the lower bound of the 95% Wilson interval of its low-score share is above that quarter's all-category share, so a small sample can't trigger a flag on its own. Show each flagged category's late-delivery rate beside it: among the quarter's single-category orders delivered with a delivery date, the share late by calendar day, as in question 1. The window's last quarter, 2018 Q3, holds only July and August | fact\_order\_items, fact\_reviews, fact\_order\_fulfillment, dim\_product |
| 8 | How much did published numbers change? | Restatement = latest published value minus first published value, per metric, reporting period and slice, shown with numerator and denominator. It describes this pipeline under simulated arrival, not Olist's business | fact\_daily\_metrics\_published |

**Why questions 4 and 7 are defined this way**

- **Reviews belong to orders, not items.** Of orders with items, 90.1% have exactly one item, 96.7% contain one distinct product and 99.2% contain one category. Attributing an order's review to its category is therefore exact for 99.2% of orders; the 786 multi-category orders count toward revenue but not satisfaction. If you add product-level satisfaction, use single-product orders only, and products with at least 20 such reviews.
- **Averages barely separate categories.** Over the reliable window, the 14 of 74 categories in the top 20% by revenue have average scores from only 3.99 to 4.22, while their low-score shares run from 11.4% to 16.2%, so question 7 uses the low-score share. Quarter by quarter, between 1 and 4 categories are flagged; bed, bath and table (cama\_mesa\_banho) and furniture and decor (moveis\_decoracao) are each flagged in 4 of the 7 quarters.
- **A review scores the whole experience.** Late orders average 2.27 with 53.8% 1-star reviews, against 4.29 and 6.6% for on-time orders. A category's low score can reflect delivery rather than the product, which is why question 7 shows the late-delivery rate beside it.

**What the data shows today.** Measured with measure\_spec.py, on orders purchased in the reliable window. These are your sanity checks: gold must reproduce them.

- **Late deliveries:** 6.79% (6,531 of 96,203 delivered orders).
- **Promises are cautious:** the median order arrives 12 days before its estimate, and 78.9% arrive at least 7 days early.
- **Seller handover:** 9.39% of items in single-seller orders are handed over after their shipping limit time (5.97% if you compare calendar days instead). Eighteen months can be ranked (Mar 2017 to Aug 2018), with a median of 61 eligible sellers a month; 9 sellers are ever classified as consistently late, 4 of them in Aug 2018; 10 more meet the 3-in-6 rule with fewer than 6 ranked months and stay unclassified.
- **Retention:** 2.03% buy again within 90 days (1,561 of 76,869 eligible people) and 2.84% within 180 days (1,628 of 57,282). 293 people first bought in 2016 and are left out of cohorts.
- **Cancellations:** of approved orders with a final status, 0.48% ended canceled (464 of 97,263) and 0.62% unavailable (602); 1,694 approved orders were still open and are left out.

## Data sources

The base is the Brazilian E-Commerce Public Dataset by Olist: about 100,000 real, anonymised orders placed from 2016 to 2018 across several Brazilian marketplaces ([dataset README](https://github.com/spdrio/Brazilian-E-Commerce-Public-Dataset-by-Olist)). It is published on [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) under CC BY-NC-SA 4.0, which means non-commercial use with share-alike ([source](https://scripts-and-tables.github.io/daa/data/)). Confirm the licence on the Kaggle page yourself and credit Olist in your README. All money values are Brazilian reais (BRL).

**How these numbers were measured.** The first revisions of this spec were measured on a GitHub mirror of the data, because Kaggle wasn't reachable from where they were written. In Phase 0 (22 Nov 2026) profile\_olist.py and measure\_spec.py were rerun on the Kaggle download. Every count matched the mirror except the reviews file: the mirror has an older 100,000-row version, Kaggle has 99,224 rows. The review-based numbers in this spec (questions 4 and 7, multiple reviews per order, shared review\_ids) are now Kaggle's; the change is recorded in docs/decisions.md. The facts below describe the whole dataset; metric values under Business questions use the reliable window.

| File | Rows (measured) | One row per |
| --- | --- | --- |
| olist\_orders\_dataset | 99,441 | Order, with status and five milestone timestamps |
| olist\_order\_items\_dataset | 112,650 | Item in an order, with product, seller, shipping limit time, price, freight |
| olist\_order\_payments\_dataset | 103,886 | Payment method used on an order |
| olist\_order\_reviews\_dataset | 99,224 (the mirror's older file has 100,000) | Review of an order |
| olist\_customers\_dataset | 99,441 | Order's customer record, including the delivery address (see the first point below) |
| olist\_products\_dataset | 32,951 | Product |
| olist\_sellers\_dataset | 3,095 | Seller |
| product\_category\_name\_translation | 71 | Category name, Portuguese to English |
| olist\_geolocation\_dataset | 1,000,163 (counted, not profiled; not in the mirror) | Zip prefix with coordinates; not used by this project |

**Measured facts that shape the design**

- **customer\_id is per order, not per person.** Each order gets its own customer\_id; customer\_unique\_id identifies the person ([data dictionary](https://pavelgrigoryevds.github.io/olist-deep-dive/contents/01_data_description_and_exploration/01_data_description.html)). The 99,441 customer\_ids belong to 96,096 people, and 2,997 people ordered more than once. Keying dim\_customer on customer\_id would be a modelling error interviewers love to catch. It also means every order carries its own delivery address, which is why regional metrics use the order's address directly.
- **Real address changes exist.** 250 people have orders with more than one zip prefix (122 with more than one city, 39 with more than one state), and 5 of them changed more than once. Counting a change in zip prefix, city or state, as the SCD Type 2 rule does, gives 252 people and 6 who changed more than once: two people keep their zip prefix while the city name changes (for example guara to brasilia and back, at zip 71065). No person has two orders in the same second with different addresses, so the order of address versions is never ambiguous.
- **Usable time window.** Purchases run from 4 Sep 2016 to 17 Oct 2018, but Sep and Oct 2018 hold only 20 orders between them and late 2016 is sparse, hence the reliable window of Jan 2017 to Aug 2018.
- **Order status:** 96,478 delivered, 1,107 shipped, 625 canceled, 609 unavailable, 314 invoiced, 301 processing, 5 created, 2 approved.
- **Delivery performance:** 6.77% of delivered orders arrived after the estimated date (6,534 of 96,470 with a delivery date). Median purchase-to-delivery time is 10.2 days; the 99th percentile is 46.1 days. The estimated delivery date never has a time of day.
- **Approvals and payments:** 160 orders never received a payment approval (141 canceled, 14 delivered, 5 created), yet all 160 have payment rows. 484 of the 625 canceled orders do have an approval. Olist records no cancellation time. One order has no payment rows at all.
- **Order composition:** 1.30% of orders with items (1,278) contain items from two or more sellers; 9.94% contain two or more items; 786 contain items from two or more categories.
- **Reviews:** 547 orders have more than one review, and 156 of them tie on the latest creation date; 789 review\_ids appear on more than one order (Kaggle download).
- **Seasonality:** Monday has 16,196 orders in total and Saturday 10,887, about 33% fewer. The busiest day, 24 Nov 2017, had 1,176 orders, against a median of 171 a day from Jun 2017 to May 2018. That date was Black Friday in Brazil ([Guia da Semana](https://www.guiadasemana.com.br/compras/evento/black-friday-brasil-2017-2017-11-24)). A naive volume check would raise a false alarm on it.
- **Geography:** 14,994 customer zip prefixes; 39 of them appear with more than one spelling of the city, and none with more than one state. Customers are in 27 states, sellers in 23.
- **Money:** on orders not canceled or unavailable, item prices total about R$13.49M and freight about R$2.24M. Payments match items plus freight except for 249 orders that differ by more than 1 BRL (232 paid more, 17 less).
- **Timestamps carry no time zone** (for example 2017-10-02 10:56:33). Store them as given; the shared definitions state the assumption.

**Real defects already in the data.** These are DataSentinel's first test cases, so it meets genuine problems, not only injected ones. Each has a measured baseline rate; the severity levels are explained under DataSentinel.

| Defect | Count (measured) | Severity | Effect on metrics |
| --- | --- | --- | --- |
| Handed to carrier before payment approval | 1,359 orders | Flag | Left out of the approval-to-carrier lag only |
| Handed to carrier before purchase | 166 orders | Flag | Left out of seller handover (question 3) and lags that use the carrier date |
| Delivered before carrier handover | 23 orders | Flag | Left out of the carrier-to-delivery lag only |
| Status delivered but no delivery date | 8 orders | Flag | Left out of delivery metrics (questions 1, 2, 4) |
| Delivered but never approved | 14 orders | Flag | Left out of question 6 and of lags that use the approval time; counted as delivered |
| Canceled but with a delivery date | 6 orders | Flag | Counted as canceled, the final status |
| Order with no items, status canceled or unavailable | 767 orders | None: expected | None |
| Order with no items, any other status | 8 orders | Flag | Counted in status totals; no revenue |
| Order with no payment rows | 1 order | Flag | Left out of payment reconciliation |
| Payments differ from items plus freight by more than 1 BRL | 249 orders | Flag | None on revenue, which uses item prices; listed in the payment reconciliation report |
| Same review\_id on two or more orders | 789 review\_ids | None: expected | The review grain is review\_id plus order\_id |
| Products with no category | 610 | Flag | Grouped as "unknown" |
| Categories with no English translation | 2 (pc\_gamer, portateis\_cozinha\_e\_preparadores\_de\_alimentos) | Warn once | Keep the Portuguese name |
| Payment rows with value 0 | 9 | Flag | None on revenue |
| Payment rows with 0 installments | 2 (credit card, delivered orders) | Flag | None |
| Payment type not\_defined | 3 (value 0.00, canceled orders) | Flag | None on revenue |

**Region mapping.** Brazil's statistics institute (IBGE) groups the states into five major regions: North, Northeast, Southeast, South and Central-West ([IBGE](https://biblioteca.ibge.gov.br/visualizacao/periodicos/2/bn_2022_v30_art02.pdf)). dim\_geography takes region from that mapping.

## Why Olist: alternatives considered

Olist is used because it is one of very few public datasets that is real, relational and records the full order lifecycle, including a promised delivery date; the country is irrelevant. It is also a marketplace that connects small sellers to sales channels ([dataset README](https://github.com/spdrio/Brazilian-E-Commerce-Public-Dataset-by-Olist)), the same third-party-seller model as Amazon's, which makes seller performance a natural question.

What the project needs from its data: linked tables; a timestamp per lifecycle stage; a promised delivery date to measure lateness against; a stable person identifier across orders; and real data, so quality checks meet genuine problems.

| Dataset | Real or synthetic | What it offers | Why it lost |
| --- | --- | --- | --- |
| Olist (chosen) | Real | 9 linked files; purchase, approval, carrier, delivery and estimated delivery timestamps; payments, reviews, sellers | Not a loser, but limited: no returns, no seller history and no cancellation times, so questions needing them are out of scope; widely used in portfolios, so differentiation must come from the engineering |
| TheLook eCommerce (Google BigQuery) | Synthetic: programmatically generated orders for a fictitious store ([Google Cloud](https://cloud.google.com/gemini/docs/conversational-analytics-api/data-agent-authored-context-bq)) | 7 tables, including inventory, distribution centres and web events; created, shipped, delivered and returned timestamps ([Vanna](https://vanna.ai/docs/streamlit.html)) | Synthetic, with no genuine defects; no promised delivery date, so lateness can't be defined; lives on Google Cloud |
| DataCo Smart Supply Chain | Disputed: described both as simulated ([Medium](https://medium.com/@ansharahinayat/exploratory-data-analysis-dataco-smart-supply-chain-dataset-da6cc74c2bee)) and as real ([write-up](https://tech-for-dev.vercel.app/articles/3739995)) | 180,519 orders with 53 features, including real and scheduled shipping days ([arXiv](https://arxiv.org/pdf/2604.05254)) | One flat table; implausible figures, such as a 57.3% late-delivery rate ([report](https://freda-erinmwingbovo.github.io/projects/dataco/dataco_report.html)) and 95.3% of First Class orders late |
| UCI Online Retail II | Real | 1,067,371 transaction rows from a UK online retailer, Dec 2009 to Dec 2011; cancellations marked; CC BY 4.0 ([UCI](https://archive.ics.uci.edu/dataset/502/online+retail+ii), [licence](https://archive-beta.ics.uci.edu/dataset/502/online+retail+ii)) | A single table with no shipping or delivery timestamps |
| REES46 multi-category store | Real | About 380M clickstream events, 47 GB uncompressed, Oct 2019 to Apr 2020 ([arXiv](https://arxiv.org/pdf/2505.19643)) | Only view, cart, remove-from-cart and purchase events ([event types](https://github.com/IIAndrewII/Customer_Behavior)): no fulfilment, payments or returns. Better suited to a streaming project |
| TPC-DS | Synthetic benchmark | 7 fact tables, including returns for three sales channels, and 17 dimensions; scales to any size ([Datacadamia](https://datacadamia.com/data/type/relation/benchmark/tpcds/schema)) | No realistic messiness; generic for a portfolio |
| Indian e-commerce uploads on Kaggle | Mostly simulated or scraped | Single sales tables or product listings | No linked order-lifecycle data found; one popular "Amazon Sales Report" upload describes itself as simulated ([Baselight](https://baselight.app/u/kaggle/dataset/arpit2712_amazonsalesreport)) |

"Why this dataset?" is a likely interview question; this table is your answer. The follow-up, "Why not invent the missing returns?", is answered in the Decision log.

## Architecture

Data flows through six layers. Every write to silver and gold lands on a per-run Iceberg branch, and only DataSentinel can publish it, as a complete release. Build everything locally first, then move storage, compute and serving to AWS in Phase 4.

- **Sources.** PostgreSQL (in Docker) is the customer, seller and product master. Sellers and products are loaded from Olist once. The generator inserts a customer at their first order and updates the row when an order arrives with a new address, as an operational system would. Separately, the replay generator writes order-lifecycle events as hourly JSON Lines files, plus a completion marker per day, into an S3 landing/ prefix.
- **Bronze.** Landing files are never modified. A file manifest records every file loaded: path, ETag, size, row count and run. An append-only Iceberg table stores each event as received, with source\_file, line number and ingested\_at. A file already in the manifest is never loaded twice, so a retry adds nothing, while a genuinely redelivered event in a new file is kept here and deduplicated in silver. That gives both a full audit trail and idempotency.
- **Silver.** PySpark types, validates, flags or quarantines, and deduplicates events and dimension extracts.
- **Gold.** PySpark builds the star schema and the dashboard aggregates.
- **Release.** DataSentinel audits the run's branches and, if nothing blocks, publishes them as one release (see Publishing).
- **Serving.** Athena for ad-hoc queries, audits and time travel over the lake; PostgreSQL tables for dashboards, loaded one release at a time in a single transaction; Streamlit (or Power BI) for the dashboards themselves.

Airflow orchestrates every step. Terraform defines the AWS resources, and GitHub Actions runs the tests on each push.

| Layer | Tool | Why this choice |
| --- | --- | --- |
| Table format | Apache Iceberg, format version 2 | Spark supports MERGE INTO on Iceberg tables for row-level updates ([Iceberg docs](https://iceberg.apache.org/docs/1.8.0/spark-writes)), which late data needs. Branches make write-audit-publish native, and every commit is an atomic, immutable snapshot ([Iceberg docs](https://iceberg.apache.org/docs/1.7.1/reliability)) |
| Processing | PySpark on AWS Glue 5.1, version pinned | Glue 5.1 runs Spark 3.5.6 and Python 3.11, with Iceberg 1.10.0 bundled ([Glue versions](https://docs.aws.amazon.com/glue/latest/dg/release-notes.html)). Locally, pin PySpark 3.5.6 with the matching iceberg-spark-runtime-3.5\_2.12 1.10.0 jar so code moves to Glue unchanged |
| Catalog | AWS Glue Data Catalog (a local Hadoop catalog during development) | Holds the metadata of the lakehouse tables in S3, which both Spark on Glue and Athena use. The dashboard tables live in PostgreSQL, loaded from each release. In a paid setup, Redshift could also read the Iceberg tables through an external schema on the Data Catalog ([Redshift docs](https://docs.aws.amazon.com/redshift/latest/dg/querying-iceberg.html)), which is optional here |
| Ad-hoc and audit queries | Amazon Athena | Supports MERGE INTO on Iceberg tables ([AWS blog](https://aws.amazon.com/blogs/big-data/perform-upserts-in-a-data-lake-using-amazon-athena-and-apache-iceberg)) and reads a table as of a time or a snapshot id with FOR TIMESTAMP AS OF and FOR VERSION AS OF ([Athena docs](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-time-travel-and-version-travel-queries.html)). Nothing runs when idle |
| Dashboard database | PostgreSQL 16 (Docker, local) | Free, and supports MERGE and multi-statement transactions, so a whole release loads atomically. Redshift would add distribution and sort keys but needs a paid AWS plan; its equivalent of this load is MERGE from a staging table ([Redshift docs](https://docs.aws.amazon.com/redshift/latest/dg/t_updating-inserting-using-staging-tables-.html)), the same staging-then-MERGE pattern this project uses |
| Quality checks | Great Expectations (GX Core) plus custom anomaly code | GX runs expectations natively on Spark DataFrames ([GX blog](https://greatexpectations.io/blog/great-expectations-now-supports-execution-in-spark)); the anomaly detection and release logic are the parts you design yourself |
| Metrics store | PostgreSQL | Check results, profiles and release history, read by the DataSentinel console |
| Orchestration | Apache Airflow 3.4 in Docker | Amazon provider operators cover Glue jobs and Athena queries ([MWAA operator list](https://docs.aws.amazon.com/it_it/mwaa/latest/mwaa-serverless-userguide/operators.html)) |

**Why PostgreSQL rather than Redshift.** The project must cost nothing, and Redshift needs a paid AWS plan. The design idea that matters, loading a whole release in one transaction so dashboards never see half of one, works the same in PostgreSQL. In an interview, explain what Redshift would add: distribution and sort keys for large scans, columnar storage, and COPY from S3. Athena (or DuckDB locally) stays the tool for ad-hoc queries and time travel over the lake.

**Why Glue 5.1 rather than 6.0.** Glue 6.0 runs Spark 4.1.1, Python 3.13 and Iceberg 1.11.0 ([Glue 6.0 migration guide](https://docs.aws.amazon.com/glue/latest/dg/migrating-version-60.html)) and is 30% cheaper, at $0.308 instead of $0.44 per DPU-hour ([AWS blog](https://aws.amazon.com/blogs/big-data/introducing-aws-glue-6-0-for-apache-spark/), [pricing summary](https://www.eon.io/blog/aws-glue-pricing)). But Spark 4.1 enables ANSI SQL mode by default ([AWS blog](https://aws.amazon.com/blogs/big-data/upgrade-aws-glue-jobs-to-glue-6-0-with-ai-powered-spark-upgrades/)), and Iceberg v3 tables created in Glue 6.0 can't be read by Athena (migration guide). Start on 5.1 for library compatibility, and create every table with Iceberg format version 2. Migrating to 6.0 later, and documenting what broke, makes a good interview story.

**Two Glue settings to make explicit.**

- **The version.** New jobs created without a version now default to Glue 6.0 ([Glue versions](https://docs.aws.amazon.com/glue/latest/dg/release-notes.html)), and a newer Auto setting can move a job to 6.0 once you approve it ([Classmethod](https://dev.classmethod.jp/en/articles/20261106-aws-glue-auto-upgread-version/)). So pin glue\_version = "5.1" in Terraform and leave Auto off: every job then matches your local Spark 3.5.6, and the runtime changes only when you decide.
- **The S3 region.** Glue 5.1 uses the S3A connector, which defaults to us-east-2 unless a region is set ([AWS re:Post](https://www.repost.aws/articles/ARjELjm9_jRxejMb1havX8xg/breaking-changes-checklist-migrating-aws-glue-jobs-to-version-5-0-5-1)). Set spark.hadoop.fs.s3a.endpoint.region to your region, such as ap-south-1 for Mumbai.

**Repository layout**

```text
chronos/
  generator/          replay generator, defect injector, ground-truth manifest
  contracts/          event schemas per schema_version
  jobs/               land_to_bronze.py, bronze_to_silver.py, build_dims.py,
                      build_facts.py, aggregates.py, export_release.py
  sentinel/           checks/, anomaly/, gate.py, release.py, console/ (Streamlit)
  dags/               chronos_daily.py, maintenance_weekly.py
  sql/                PostgreSQL serving DDL, release load and MERGE scripts, Athena and DuckDB audit queries
  tests/              unit/, scenarios/, truth/, evaluation/
  infra/              Terraform
  docs/               metrics.yml, architecture, data model, decision log, runbook
```

## Event model and the replay generator

The generator turns Olist's final records back into the stream of events that would have produced them, then disturbs how that stream arrives, in controlled and recorded ways. It runs on a simulated clock: each Airflow run processes one historical business date. Two modes share one code path:

- **Truth replay:** reversible defects only. It feeds the main lakehouse and the dashboards, and its business tables must end exactly equal to Olist.
- **Evaluation replay:** reversible defects plus irreversible anomalies, written only to the chronos\_eval namespace, to measure DataSentinel.

**Event types**

| Event | Source in Olist | Event time | Logical key |
| --- | --- | --- | --- |
| order\_placed | order\_purchase\_timestamp, with the order's customer\_id, delivery address, items and payment rows | Observed | order\_id |
| payment\_approved | order\_approved\_at | Observed; 160 orders never get one | order\_id |
| handed\_to\_carrier | order\_delivered\_carrier\_date | Observed | order\_id |
| delivered | order\_delivered\_customer\_date | Observed; empty for the 8 delivered orders with no date | order\_id |
| order\_cancelled, order\_unavailable | Final status canceled or unavailable | None: not in the source | order\_id |
| review\_submitted | review\_creation\_date, review\_answer\_timestamp and score | Observed | order\_id + review\_id |

**Why payment rows ride on order\_placed.** Payment method and value are known at checkout. More importantly, all 160 orders that never received an approval still have payment rows; attached to payment\_approved, those payments would never arrive, and fact\_payments could never match the source.

**Event identity and versions.** event\_id is the hex SHA-256 of event\_type, logical key and version, joined in that order with a separator that can't occur in them (the ASCII unit separator, 0x1F). Never use Python's built-in hash(): it is randomised per process for strings, so ids would change between runs. Version 1 is the original; a correction is the same event type and logical key with the next version, carrying the full corrected payload and a supersedes\_event\_id naming the version it replaces. So two reviews on one order get different ids, a resent duplicate keeps its id and is dropped by deduplication, and a correction gets a new id. The current value of an event is its highest version.

**Arrival (simulated, all of it).** An event is normally emitted in the hourly file that covers its event time. Events with no event time (cancellations and the 8 undated deliveries) are emitted one day after the order's last observed timestamp; that is a documented arrival rule, not a business fact. After the last business date, the replay clock keeps running for a drain period at least as long as the longest configured delay (10 days with the suggested rates), emitting nothing new, so every delayed event, file and dimension row arrives before the truth tests run.

**Completion marker.** When a business date's files are all written, the generator writes a \_complete.json file listing every file it produced for that date, with each file's row count and SHA-256 checksum. This is the producer's delivery contract, the kind a real source system provides; it says what was sent, never what is wrong, so it is not the ground-truth manifest. A late file is listed in the marker but lands a day later, which is exactly what the completeness check catches.

**Event envelope.** event\_id, event\_type, logical key, version, supersedes\_event\_id, schema\_version, event\_time, emitted\_at and payload. Bronze adds source\_file, line number and ingested\_at. So every event has up to three clocks: when it happened, when the source sent it, and when you received it.

**Source database (PostgreSQL)**

| Table | One row per | Columns | Written when |
| --- | --- | --- | --- |
| customers | Person (customer\_unique\_id) | Zip prefix, city, state, address\_effective\_from, created\_at, updated\_at | Inserted at the person's first order; updated when an order arrives with a new address |
| sellers | Seller | Zip prefix, city, state, created\_at, updated\_at | Loaded at the start, except a few held back to simulate early-arriving facts |
| products | Product | Category, size, weight, created\_at, updated\_at | Loaded at the start |

Two timestamps on customers do different jobs. address\_effective\_from is business time: the purchase time of the first order seen with that address, and the effective\_from of the SCD Type 2 version. updated\_at is set by the database: a BEFORE INSERT OR UPDATE trigger writes the replay clock's current time, read from a one-row replay\_clock table that the generator advances. A column default alone wouldn't do, because PostgreSQL applies defaults only on INSERT. updated\_at is the only column extraction filters on. A late address update therefore carries an old address\_effective\_from but a new updated\_at, so the next extract still finds it.

```sql
CREATE TABLE replay_clock (now_ts timestamp NOT NULL);  -- one row, advanced by the generator

CREATE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN
  NEW.updated_at := (SELECT now_ts FROM replay_clock);
  RETURN NEW;
END $$ LANGUAGE plpgsql;

CREATE TRIGGER customers_updated_at BEFORE INSERT OR UPDATE ON customers
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();
-- the same trigger on sellers and products
```

The master holds only the current address. Each order's own address travels in its order\_placed event, so a later master update can never overwrite it. The generator's list of real address changes, derived from Olist, belongs to the ground-truth manifest: tests read it, the pipeline never does, otherwise SCD Type 2 would be copied rather than built.

**Reversible defects (truth replay).** The rates are starting parameters to tune, not facts.

| Defect | Suggested rate | How it is reversed | What it tests |
| --- | --- | --- | --- |
| Late events: carrier and delivery events emitted 1 to 10 days after they happened | 5% of those events | The event arrives later | Accumulating snapshot updates; recomputing only affected dates |
| Out of order: delivered arrives before handed\_to\_carrier | 1% of orders | Both arrive | Milestone precedence rules |
| Exact duplicates (same event\_id, possibly in another file) | 2% of events | Deduplication | Idempotent deduplication |
| Wrong value, then correction: a delivery time or payment value first emitted wrong | 0.5% of events | Version 2 restores Olist's value | Versioning and restatement tracking |
| Early-arriving facts: a customer or seller row reaches PostgreSQL 1 to 3 days after the first order that references it | 0.2% of orders | The row arrives | Inferred members |
| Late dimension change: a customer's address update reaches PostgreSQL 1 to 3 days after the order that carries it; updated\_at is the arrival time, address\_effective\_from stays the order's purchase time | 10% of address changes (about 25 people) | The update arrives | Re-keying facts to the right SCD Type 2 version |
| Late file: an hourly file delivered a day late | About 10 files | The file arrives | Completeness and freshness checks; dirty periods |
| Announced schema change: a new optional field on a fixed date, then a renamed field under a new schema\_version | Once each | The contract knows the new version | Schema contract handling |

**Irreversible anomalies (evaluation replay only)**, about 30 of each, placed on held-out days: hours or half-days of files never delivered; a whole file delivered twice under a new name; volume spikes; payment-type mix or price levels pushed off baseline; an unannounced schema break; bursts of nulls.

## Data model

The gold layer is a star schema with five fact tables, one append-only table of published metrics and five dimensions, each with a declared grain. The centrepiece is fact\_order\_fulfillment, an accumulating snapshot: Kimball defines this as a row inserted once when the process starts and revisited and updated as it progresses ([Kimball Group](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/accumulating-snapshot-fact-table/)). Kimball also recommends pairing it with a transaction fact table holding a row per state change, and often deriving the snapshot from it ([Design Tip #145](https://www.kimballgroup.com/2012/05/design-tip-145-time-stamping-accumulating-snapshot-fact-tables/)). That is exactly the role of fact\_order\_events here.

| Table | Type | Grain (one row per) | Key contents |
| --- | --- | --- | --- |
| fact\_order\_events | Transaction | Accepted event version (event\_id) | event\_type, event\_time (empty when the source has none), emitted\_at, ingested\_at, supersedes\_event\_id, is\_current\_version. The audit trail the snapshot is derived from. Its arrival columns are simulated, so no business metric reads them |
| fact\_order\_fulfillment | Accumulating snapshot | Order | Purchase, approval, carrier, delivery and estimated delivery date keys (dim\_date in five roles); lags in days; is\_late (empty when undelivered or undated); final\_status and is\_open; customer\_key (the version at purchase); delivery\_geography\_key; dq\_flags |
| fact\_order\_items | Transaction | Order item (order\_id + order\_item\_id) | product\_key, seller\_key, seller\_geography\_key, delivery\_geography\_key, customer\_key, purchase date key; price; freight; shipping limit time |
| fact\_payments | Transaction | Payment row (order\_id + payment\_sequential) | Payment type, installments, value |
| fact\_reviews | Transaction | Review on an order (review\_id + order\_id) | Score, creation and answer dates, is\_order\_review (the review that counts as the order's score). review\_id alone is not unique |
| fact\_daily\_metrics\_published | Append-only snapshot | Metric, reporting period, slice and release (metric\_id + period\_type + period\_start + slice\_type + slice\_value + release\_id) | period\_end, value, numerator, denominator, published\_at. period\_type is month, quarter or cohort\_month, matching each metric's own grain; slice\_type is all, region, seller, category or delivery\_group. Each daily release appends a row for every period it recomputed, so June 2017 in the North can have many rows, one per release that republished it |
| dim\_customer | SCD Type 2 | Person (customer\_unique\_id) per version | Zip prefix, city and state as held in the customer master; effective\_from, effective\_from\_basis, effective\_to, is\_current, is\_inferred |
| dim\_seller | Type 1 | Seller | Zip prefix, city, state; is\_inferred |
| dim\_product | Type 1 | Product | English category ("unknown" for the 610 without one; the 2 untranslated keep their Portuguese name); size and weight |
| dim\_geography | Rebuilt from the data | Location (zip prefix + normalised city + state) | State, IBGE region. Played in two roles: delivery location and seller location |
| dim\_date | Static | Day | Weekday, month, quarter, and a flag for known sale days such as Black Friday. Played in five roles |

Dashboard aggregates (late deliveries by day and region, seller handover by month, category satisfaction by quarter, retention cohorts) are recomputed only for dirty periods (see Late-arriving data).

**Geography: keys on the facts, region in one place.** Regional metrics join through the fact's delivery\_geography\_key or seller\_geography\_key, never through the customer or seller dimension. Kimball's guidance is that outrigger dimensions (a dimension referencing another) are permissible but should be used sparingly, and that most correlations between dimensions belong in the fact table as separate foreign keys ([Kimball Group](https://www.kimballgroup.com/?p=19262)). One physical dimension referenced several times plays separate roles ([Kimball Group](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/role-playing-dimension/)). Here it is also more accurate: each Olist order carries its own delivery address, so the fact's geography is observed, while the customer's address history is inferred. dim\_customer keeps only the address fields its source holds; region exists only in dim\_geography, so it can't disagree with itself.

**SCD Type 2 rules (dim\_customer)**

- Key on customer\_unique\_id, never customer\_id.
- A new version starts when the master's zip prefix, city or state changes; 252 people get at least one. City counts even when the zip prefix stays the same, because the consistency test below compares the full address.
- effective\_from comes from the master's address\_effective\_from, the purchase time of the first order seen with the new address, and effective\_from\_basis = 'first\_order\_seen' records that it is inferred. It is never taken from updated\_at, which only says when the row reached PostgreSQL. The data shows each order's address, not the day someone moved, and a new delivery address can also mean a gift or a workplace.
- Facts take the version effective at purchase time: on or after effective\_from and before effective\_to. A consistency test checks that, for every order, this version's address equals the order's delivery address.
- Surrogate keys are deterministic: the first 8 bytes of SHA-256 over customer\_unique\_id and effective\_from, read as a BIGINT, so reruns produce identical keys. A uniqueness check catches the negligible chance of a collision.
- **Late dimension change.** When a version arrives whose effective\_from is earlier than facts already loaded for that person, re-key that person's facts with purchase time on or after the new effective\_from, in the same run. This is the late-arriving dimension case Kimball's Design Tip #78 discusses ([DecisionWorks](https://decisionworks.com/2006/04/design-tip-78-late-arriving-dimension-rows/)).

**Inferred members (early-arriving facts).** When a fact arrives with a customer or seller the dimension hasn't seen, insert a placeholder row from its natural key with is\_inferred = true, and point the fact at it. When the real row arrives, overwrite the placeholder in place, so existing facts stay attached and pick up the real attributes ([LeapFrogBI](https://www.leapfrogbi.com/?p=13291)). Kimball notes that Type 2 changes to a placeholder row can force changes to fact keys ([Design Tip #78](https://decisionworks.com/2006/04/design-tip-78-late-arriving-dimension-rows/)), so write down the rule you choose and test it.

**Why sellers and products are Type 1.** Olist has one snapshot of each, with no history. Inventing seller moves to demonstrate SCD Type 2 would put fake facts in the warehouse; dim\_customer already demonstrates the pattern on real changes. If a source ever supplies seller history, dim\_seller follows the same rules.

## Late-arriving data and corrections

The pipeline's correctness rests on four rules: deduplicate before every MERGE; decide each milestone by event time rather than arrival order; recompute only the reporting periods a change touches; and re-key facts when a dimension change arrives late. Each scenario below has a scripted test with a known expected result.

| # | Scenario | Rule | Test |
| --- | --- | --- | --- |
| 1 | Duplicate event (same event\_id, possibly in another file) | Deduplicate on event\_id within each batch and against silver before MERGE. This is mandatory: Iceberg's MERGE errors if more than one source row matches the same target row ([Iceberg docs](https://www.mintlify.com/apache/iceberg/engines/spark/writes)) | Inject a duplicate; gold and all metrics are unchanged |
| 2 | Late milestone: a delivery event arrives three days after the order row was built | MERGE fills the delivery date, recomputes lags and is\_late, and adds the order to the dirty-periods list (below). Only those periods are recomputed | The snapshot row updates; only the affected periods change |
| 3 | Out of order: delivered arrives before handed\_to\_carrier | Each milestone column takes its value from the current version of its own event type, whatever order events arrive in. Status is derived from milestones and final-status events, not from the last event received | Both arrival orders produce identical rows. Real Olist rows already exercise this: 23 orders were delivered before their recorded carrier handover, and they stay as flagged source data, not pipeline errors |
| 4 | Correction to an earlier event | The correction is the next version of the same event: it carries the observed value and names the version it supersedes, and the highest version wins. fact\_order\_events keeps both versions, with is\_current\_version on the new one | Gold matches Olist's value; the restatement report shows the change |
| 5 | Early-arriving fact: an order for a customer not yet in the dimension | Inferred member (see Data model) | The fact joins to the placeholder, then shows real attributes once they arrive. Customers need no fact rewrite (the placeholder has the real row's key); a placeholder seller's location key on fact\_order\_items is re-keyed when the real seller arrives, through the dirty-period list, and only that seller's rows change |
| 6 | Late dimension change: an address update arrives after orders that used it | The row's updated\_at is when it reached PostgreSQL, so the next extract finds it; its address\_effective\_from is in the past, so the person's facts are re-keyed from that date (see Data model) | After the update arrives, the SCD Type 2 consistency test passes and only that person's facts changed |
| 7 | Retry of the same run | Bronze skips files already in its file manifest; the run's branches are recreated from main; silver MERGEs on event\_id; gold MERGEs on each table's grain; surrogate keys are deterministic | Two runs produce identical tables (row counts and checksums) |
| 8 | Job fails halfway | All writes go to the run's branches; main and the dashboards change only when a release is published | Kill a job mid-run; the serving database is unchanged, and the rerun ends identical to an uninterrupted run. Main unchanged is checked once runs write to branches (Phase 3; until then the test is an expected failure) |
| 9 | Backfill of past dates | Run dates one at a time, in order (max\_active\_runs = 1), because each publish moves main and the next run's branches must start from it | Catching up after days without runs (files and source changes still arriving) gives the same gold as running daily; the serial Airflow backfill is checked by hand and its run list kept in the phase notes |

**Dirty periods.** A changed order can affect several metrics, each at its own grain, so build\_facts records which reporting periods each change touches, and only those periods are recomputed. Each period is recomputed whole, never patched row by row, because ranks, shares and cumulative distributions depend on every row in the period.

| Metric | A changed order dirties | What is recomputed |
| --- | --- | --- |
| 1, 2. Lateness, promises | Its delivery month | That month, for every region and the all slice |
| 3. Seller handover | The shipping-limit month of each of its items | That month's late rates and ranks for every eligible seller, then consistency classifications for that month and the following 5 ranked months |
| 4. Satisfaction | Its delivery month (also when its review changes) | That month's late and on-time groups |
| 5. Retention | The cohort month of its customer | That cohort's 90- and 180-day rates. A late order can also change which order is a person's first, so the person's cohort is derived again, and both the old and the new cohort month are dirty |
| 6. Cancellations | Its purchase month | That month |
| 7. Categories | Its purchase quarter (also when its review changes) | That quarter's revenue, CUME\_DIST ranks, all-category share and flags, for every category |
| 8. Restatements | Nothing directly | Follows from the rows the other metrics republish |

**"As first reported" versus "as known now"**

- **Published metrics log.** Each release appends the values of every period it recomputed, per slice, to fact\_daily\_metrics\_published. The restatement report compares the first published value of each metric, reporting period and slice with the latest, with numerators and denominators so you can see why a rate moved.
- **Recompute as of any time.** Silver keeps ingested\_at on every event, so any metric can be rebuilt as it would have looked at time T by using only events ingested by T.
- **Read any release.** The release manifest stores each table's snapshot id, so Athena can read every table exactly as a past release published it, with FOR VERSION AS OF ([Athena docs](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-time-travel-and-version-travel-queries.html)). That history lasts only as long as the snapshots do, so published snapshots are tagged with a retention period (see Publishing) and snapshot expiry is set to cover your restatement window ([Iceberg docs](https://iceberg.apache.org/docs/1.5.1/branching/)).

## Publishing: releases and read consistency

A day's data is visible only as a release: a set of table snapshots that were audited together. Iceberg makes each table's commit atomic ([Iceberg docs](https://iceberg.apache.org/docs/1.7.1/reliability)), but publishing several tables takes one commit per table. This design therefore doesn't rely on multi-table commits; it records exactly which snapshot of each table belongs to a release, and gives each kind of reader a stated guarantee.

**Run lifecycle**

1. **Open.** For every silver and gold table the run writes, drop any leftover branch named run\_\<run\_id> and create it from main, so a rerun starts clean. Set write.wap.enabled on each table.
2. **Write.** Jobs run with spark.wap.branch = run\_\<run\_id>, so their writes, and their reads of tables the run already wrote, use the branch ([Cloudera](https://community.cloudera.com/t5/Developer-Blogs/Iceberg-WAP-Failsafe-ETL-with-Iceberg-and-CDE/m-p/413008), [Iceberg docs](https://iceberg.apache.org/docs/1.6.0/spark-writes)).
3. **Audit.** DataSentinel checks the branch state of all tables together.
4. **Publish.** If nothing blocks, fast-forward each table's main to its branch with the fast\_forward procedure ([Iceberg docs](https://iceberg.apache.org/docs/1.5.2/branching)), dimensions first and facts last. Record each table's new snapshot id, and the bronze ingestion watermark the run processed up to, in the release manifest, tag each snapshot release\_\<release\_id> with a retention period so snapshot expiry keeps it, and finally mark the release published. That last single write is the moment the release exists.
5. **Serve.** load\_serving reads each gold table at the manifest's snapshot ids, writes the rows into staging tables in PostgreSQL, then runs every MERGE plus the update of the active release id inside one PostgreSQL transaction (BEGIN ... COMMIT), so the dashboards switch releases all at once.
6. **Clean up.** Drop the run's branches once the release is served; the tags keep the published snapshots.

**What a run processes.** Bronze is append-only and written outside the run's branches, so each run needs a rule for what to pick up: every bronze event ingested after the watermark stored in the last published release, up to the run's start. A blocked run publishes nothing and doesn't move the watermark, so its events are picked up again by the next run, together with new ones. Nothing is lost, and nothing is counted twice in published data.

**Who sees what**

| Reader | What it sees | Guarantee |
| --- | --- | --- |
| Dashboards (PostgreSQL) | Exactly one release | All dashboard tables switch together in one transaction, or not at all |
| Restatement report, audits | A chosen release | Every table read at that release's snapshot ids with FOR VERSION AS OF |
| Ad-hoc Athena on main | The latest commit of each table | Each table is consistent on its own. During the seconds of a publish, two tables can come from different releases; documented, and not used for reporting |

**Why not the simpler options**

- **Views filtered to the latest published run** (this spec's first version) work for appended rows tagged with a run id, but not for rows a MERGE updates in place, such as the accumulating snapshot: once main is fast-forwarded, the old version of an updated row is no longer in main to fall back to.
- **A full copy of every table per release** gives consistency, but multiplies storage and throws away incremental MERGE. Iceberg snapshots already are immutable versions; the manifest just names them.
- **Dropping branches and writing straight to main** would expose unaudited data. Branches stay, because they are what makes "audit before anyone sees it" true for each table.

**Failure handling**

- **Crash between fast-forwards.** Some tables have moved and the release is not marked published. Rerunning publish is safe: it skips tables whose main already points at the branch head, then finishes. Dashboards stay on the previous release throughout.
- **Main moved after the branch was created.** fast\_forward can only move main along the branch's own history, so a commit to main from anywhere else (compaction, say) makes publish fail rather than silently overwrite. That is why runs are serial and maintenance shares a lock with the daily DAG (see Orchestration).

## DataSentinel: the quality gate

DataSentinel decides whether a run becomes a release. It is the audit step of the write-audit-publish pattern, which Iceberg supports by enabling write.wap.enabled, writing to a branch, and then calling fast\_forward to move main to the branch ([Iceberg docs](https://iceberg.apache.org/docs/1.5.2/branching), [AWS blog](https://aws.amazon.com/blogs/big-data/build-write-audit-publish-pattern-with-apache-iceberg-branching-and-aws-glue-data-quality)). Its central rule is to separate two different problems: a bad source row is handled row by row, and a bad run stops the release.

**Severity levels**

| Severity | Applies to | What happens | Typical cause |
| --- | --- | --- | --- |
| Block | The run | Nothing is published; the branches are kept for inspection; an alert names the failed checks | A broken pipeline or broken delivery: key violations, reconciliation mismatches, missing data, a spike in a defect rate |
| Quarantine | A row | The row is kept out of gold and stored with its reason; the run continues unless quarantined rows exceed their threshold | An unusable row: unknown schema version, missing required field, value outside its valid range |
| Flag | A row | The row stays, dq\_flags records the problem, and metrics apply the exclusions written in Data sources | Real but inconsistent source data, such as a carrier handover before payment approval |
| Warn | The run | The release is published, with the warning stored in the release manifest and shown in the console | Moderate drift, a change in null rates, a new optional field |

**Known source defects.** Each real Olist defect in Data sources has a measured baseline rate. Rows are flagged as that table says, and the run blocks only when a day's rate jumps far above its baseline, scored with the robust method below, or with its fallback when the baseline has no spread. The steady baseline is the source's normal untidiness; a sudden spike means something changed upstream or in the pipeline.

**Checks**

| Check | Applies to | Method | Severity |
| --- | --- | --- | --- |
| Schema contract | Events entering silver | Expected fields and types per known schema\_version | Quarantine events with an unknown version or a missing required field; Block if they exceed 0.5% of the day's events; Warn on a new optional field |
| Completeness and keys | Silver and gold | GX not-null and uniqueness expectations on each table's grain | Block |
| Referential integrity | Gold facts | Every dimension key exists; inferred members count as present | Block |
| Valid values | Silver | Price above 0, review score 1 to 5, installments at least 1, known payment types. The real Olist rows with 0 installments or type not\_defined are flagged as known source defects instead (see Data sources) | Quarantine rows; Block above 0.5% of the day's rows |
| Known source defects | Silver | The defect rules in Data sources | Flag rows; Block when a day's rate scores above 3.5 against its baseline |
| Reconciliation | Silver to gold | Order counts and revenue per business date match exactly, once flagged and quarantined rows are accounted for | Block |
| Volume | Each ingest day | Modified z-score against the same weekday over the previous 8 weeks (details below) | Block; Warn on known sale days |
| Freshness | Landing zone | Latest file arrival and latest event time against an agreed delay | Block publishing the day as complete; dashboards show staleness |
| Distribution drift | Payment-type mix, price buckets, category mix, review scores | Population Stability Index against a trailing baseline | Warn from 0.1; Block from 0.25 once tuned |
| Null-rate change | All columns | Share of nulls compared with baseline | Warn |
| Job health | Airflow tasks | Failures, retries, duration compared with baseline | Alert |
| Completeness | Landing zone | Compare the completion marker's file list, row counts and checksums with the file manifest | Listed file missing: Warn and publish with the date marked incomplete, and alert if it is still missing after 2 days. File not listed, or a row count or checksum that differs from the marker: Block |

The 0.5% and 3.5 thresholds are starting values. Like every threshold here, tune them on the tuning period only (see Testing and evaluation).

**Anomaly detection: methods and why**

- **Robust volume scores.** Use the modified z-score: 0.6745 times the distance from the median, divided by the median absolute deviation. Iglewicz and Hoaglin recommend flagging absolute values above 3.5 ([NIST handbook](https://itl.nist.gov/div898/handbook/eda/section3/eda35h.htm)). Medians resist being pulled by the very outliers you are looking for.
- **Compare like with like.** Saturday volume in Olist is about 33% below Monday's, so compare each day with the same weekday, not with yesterday.
- **Known sale days.** On 24 Nov 2017 orders were about 6.9 times the typical day. Keep a calendar of known events in dim\_date; on those days, widen the bands and downgrade volume alerts to warnings, rather than turning checks off.
- **Drift thresholds.** A common reading of PSI is below 0.1 no substantial change, 0.1 to 0.25 a small change, and 0.25 or more a substantial change. The same review notes PSI has known weaknesses and should be used with care ([arXiv review](https://arxiv.org/pdf/2303.01227)). Treat these as starting points and tune them on clean history.
- **When the baseline has no spread.** For rare defects, most days have zero, so the median absolute deviation is zero and the modified z-score is undefined. Then switch to a rate-based limit: take the pooled baseline rate p over the trailing window, with a floor of 3 divided by the baseline's total rows (the rule of three, a 95% upper bound when zero defects were seen), and flag a day whose defect count has a binomial tail probability below 0.001 at that rate.
- **Warm-up, and recording the method.** Until a check has 4 weeks of history it only warns, never blocks; from 4 to 8 weeks it uses the history it has. Every decision stores the method used (modified z-score, rate limit or warm-up) and the baseline it was compared with, so any alert can be explained afterwards.

**Tooling**

- **GX Core for rule checks.** It is open source, its stewardship is moving to Fivetran ([GX announcement](https://www.greatexpectations.io/blog/an-update-from-great-expectations)), and recent releases are in the 1.24 line ([release tracker](https://newreleases.io/project/pypi/great-expectations/release/1.23.2)). Pin the version you test with. Glue 5.0 and later can install extra Python libraries from a requirements.txt ([AWS docs](https://docs.aws.amazon.com/ja_jp/glue/latest/dg/migrating-version-50.html)).
- **Metrics store in PostgreSQL:** check\_results (run, table, partition, check, method, severity, observed, expected, status), profiles (table, partition, column, metric, value) and releases (release\_id, business\_date, run\_id, status, warnings, published\_at, and one snapshot id per table).
- **Console in Streamlit:** check history per table, open failures, the quarantine browser, the release history, the restatement report and alert history.
- **Alerts** from Airflow failure callbacks to email or a Slack webhook, naming the run, the failed checks and the console link.

## Orchestration: the Airflow DAGs

One daily DAG processes one business date end to end, and every task in it is safe to rerun. Airflow 3.4.0 is the current release (6 Nov 2026), and the 3.3 line reached end of support on 7 Nov 2026 ([Airflow release notes](https://airflow.apache.org/docs/apache-airflow/stable/release%5Fnotes.html), [endoflife.date](https://endoflife.date/airflow)). Run it in Docker on your laptop, pinned to an exact version: Airflow's documented way to get a reproducible install is its constraints file for your Airflow and Python versions ([Airflow docs](https://airflow.apache.org/docs/stable/installation/installing-from-pypi.html)), and the official image tag pins the same set. Pin the Amazon provider with it, and upgrade both together.

**chronos\_daily**

1. **wait\_for\_landing:** wait for the day's completion marker in S3.
2. **register\_files:** compare landing files with the bronze file manifest and with the completion marker's file list; only new files go forward, and missing or unlisted files are recorded for the completeness check.
3. **extract\_dimensions:** pull changed rows from PostgreSQL where updated\_at, the time the database wrote the row, is later than the high-water mark minus a one-day overlap. Re-reading a few rows is harmless because the MERGE is idempotent, and the overlap catches rows committed late with an earlier updated\_at, a classic way high-water marks lose data.
4. **open\_run:** recreate the run's Iceberg branches from main.
5. **land\_to\_bronze:** append the new files' events.
6. **bronze\_to\_silver:** Glue job; read the bronze events ingested after the last published watermark, then type, validate, flag, quarantine and deduplicate them.
7. **build\_dimensions:** SCD Type 2, inferred members, and re-keying for late dimension changes.
8. **build\_facts:** MERGE events into facts and record dirty periods.
9. **recompute\_aggregates:** recompute each metric's dirty periods only.
10. **sentinel\_audit:** run every check; fail the task on any blocking result.
11. **publish\_release:** fast-forward branches, write and tag the release manifest, append published metrics.
12. **load\_serving:** read the release's snapshots into PostgreSQL staging tables, then one transaction of MERGEs plus the active-release update.
13. **notify:** a success summary, or failure details with the console link.

**DAG settings.** max\_active\_runs = 1, so dates run serially (scenario 9). Infrastructure tasks (2 to 9, and 12) retry twice with backoff. sentinel\_audit never retries automatically, because a failed audit needs a person to look at it. publish\_release can retry, because it skips tables already advanced. Backfilling a date range is safe because every task is idempotent. Pass --max-active-runs 1 to an Airflow 3 backfill (it applies its own limit, not the DAG's), and give every task depends\_on\_past and wait\_for\_downstream so dates can't overlap however runs are created. Rerunning a date that is already published republishes nothing: it logs "already published in release X" and exits successfully. Logic bugs are fixed by a full rebuild instead: keep bronze, reset silver and gold, and run every date again in order (docs/runbook.md).

**maintenance\_weekly:** compact small files, expire snapshots older than the restatement window (tagged release snapshots are kept), and remove orphan files, reporting what each job changed. It shares a one-slot Airflow pool with the daily DAG, so it never commits to main while a run's branches are open; a commit there would make that run's publish fail.

**Cost note.** Run Airflow locally rather than on a managed service; it only sends commands to AWS, so it doesn't need to live there.

## Testing and evaluation

The project's credibility rests on two pieces of evidence: tests proving the pipeline converges to the truth, and a measured evaluation proving DataSentinel catches problems without crying wolf. Both use the generator's ground-truth manifest, so expected results are known in advance.

**Tests that run in GitHub Actions on every push (local Spark, small data).** CI can't see the Olist files, which are never committed, so CI runs every test on hand-built fixtures, and the tests that need the real data run locally (marked olist; `make truth` runs the real-slice truth test, and each phase's notes record when it last passed).

- **Unit tests** (pytest) for each transformation, using tiny hand-built DataFrames.
- **Scenario tests:** the nine scenarios in the late-data table, each a scripted input with an expected gold output.
- **Truth test:** three parts, run after a truth replay and its drain period. (1) Business truth: every current-state business table (fulfillment, items, payments, reviews, the dimensions and the aggregates) equals a reference build from the Olist files, compared row by row on each table's grain. Write the reference build separately, in DuckDB SQL, so the pipeline isn't checked against its own logic. (2) Event ledger: fact\_order\_events holds exactly the event versions, corrections and arrival times the ground-truth manifest predicts. (3) Publication history: each release's published metrics equal the reference build recomputed with only the events that release could have seen. CI runs them on the fixture with heavy defect rates; `make truth` runs them locally on a 60-day slice (orders purchased 2017-01-01 to 2017-03-01, truth as known at the end of day 60, then an 11-day drain); run the full replay before each tagged version. Part 1 is the project's strongest single claim: whatever arrived late, twice or wrong, the business tables ended up right.
- **Idempotency test:** process the same day twice and compare row counts and checksums of every gold table.
- **SCD Type 2 consistency test:** for every order, the customer version effective at purchase time has the order's delivery address.
- **Metric regression test:** gold's metric tables equal the CSV breakdowns from measure\_spec.py, period by period and slice by slice.
- **Release consistency test:** kill publish between two fast-forwards; the serving database still shows the previous release, and rerunning publish completes the release.

**DataSentinel evaluation (the headline result)**

- Run it on the evaluation replay, in its own namespace, never on the business tables.
- Split the replay into a tuning period and a held-out test period. The test period must include weekends and the late-November 2017 sale days.
- Tune thresholds on the tuning period only. Never adjust them after looking at test results; that is the same discipline as your ML projects.
- On the test period, inject about 30 anomalies of each type from the manifest, and leave other days clean.
- Report, per check and anomaly type: detection rate, false-alarm rate on clean days, time to detection in runs, and whether any bad data reached a published release (target: none).
- Publish a confusion matrix and a short write-up of misses and false alarms, with what you changed and why.

**Performance numbers to record**

- Runtime of a daily run, and backfill throughput in days per hour.
- Athena data scanned for the same dashboard query on raw JSON versus partitioned Iceberg tables.
- Dashboard query time in PostgreSQL before and after adding indexes on the serving tables.
- Free-plan credits used per daily run, from the AWS Billing console, not an estimate.

**Optional load test, clearly labelled.** About 100,000 orders is small. If you want scaling numbers, copy the replay K times with new ids into a separate load-test namespace, and report only runtime and cost from it, never business numbers. Say "load-tested at K times the real volume", not "processed K million orders".

## Decision log

Each row is a decision an interviewer can challenge, with the alternatives considered and the cost accepted.

| Decision | Alternatives considered | Why this one | Trade-off accepted |
| --- | --- | --- | --- |
| Olist as the only data source | TheLook, DataCo, UCI Online Retail II, REES46, TPC-DS | The only real, relational dataset found with a full order lifecycle and a promised delivery date | Small, and widely used in portfolios |
| Simulate arrival, never business facts | Invent returns and seller changes; or no simulation at all | Public data has no messy feed, so arrival must be simulated to test the pipeline. Keeping business facts real keeps every metric honest, and reversible defects make correctness provable by the truth test | The feed's timing isn't real, and some questions (returns) are out of scope |
| Batch, daily | Streaming with Kinesis or Spark Structured Streaming | Every business question here is daily, and late data is about event time, not latency | No real-time view |
| Spark on Glue for about 100,000 orders | pandas, DuckDB or PostgreSQL alone | The target platform is an S3 lake with managed Spark; the patterns that matter at scale (incremental MERGE, partition pruning, compaction) are what's being shown | More machinery than the volume needs; say so openly, and point to the optional load test |
| Iceberg format version 2 | Delta Lake, Hudi, plain Parquet; Iceberg v3 | MERGE, branches for write-audit-publish, snapshots for releases and time travel, and readable by Athena, which can't read v3 | Fewer v3 features |
| Glue 5.1, pinned | Glue 6.0; EMR; the Auto version setting | Matches local Spark 3.5; avoids the Spark 4 ANSI-mode migration while learning; EMR adds a cluster to run | A 30% higher DPU price than 6.0 |
| Accumulating snapshot plus an event fact | A status table overwritten in place | Lifecycle milestones and lags in one row per order, with the full event trail beside it, as Kimball recommends | MERGE updates on every milestone |
| Regional metrics through geography keys on the facts | Region stored in dim\_customer; an outrigger from dim\_customer to dim\_geography | The order's address is observed exactly; Kimball prefers demoting dimension correlations to the fact table | One more key per fact table |
| dim\_customer as SCD Type 2 with inferred dates | Type 1 | The source master overwrites addresses, so history is lost unless the warehouse keeps it; the dates are labelled inferred | Effective dates are approximate |
| dim\_seller as Type 1 | Simulated seller history | No real history exists | No Type 2 demonstration on sellers |
| Category satisfaction on single-category orders | Spreading each order's score across its items | Reviews belong to orders; 99.2% of orders have one category | 786 orders excluded from satisfaction |
| Release manifest with per-table branches and a one-transaction serving load | Views over published runs; a full copy per release; multi-table catalog transactions | Views fail for MERGE-updated rows; copies waste storage and lose incremental loads; the design doesn't depend on catalog features | Ad-hoc readers of main can see a mixed state for seconds during a publish |
| Four severity levels | Block on any failure | Separates bad source rows from bad runs, so real data isn't blocked forever | Thresholds need tuning and justification |
| Median-based anomaly scores per weekday | Mean and standard deviation; a machine-learning model | Outliers can't distort their own baseline, and every alert can be explained | Slow drift is missed by volume checks, so PSI covers it |
| Bronze append plus a file manifest | Overwriting the ingest-hour partition | Keeps every event as received and still makes retries idempotent | One more table to maintain |
| PostgreSQL as the dashboard database | Redshift Serverless; Athena only | The project must cost nothing and Redshift needs a paid AWS plan; PostgreSQL still gives MERGE and transactional release loads | No hands-on Redshift distribution and sort keys; explain them instead |
| Airflow on the laptop | Amazon MWAA | It only sends commands to AWS; MWAA costs money while idle | Not production-hosted |
| Dimension extraction by polling a high-water mark on updated\_at | Change data capture from the database log; a full snapshot every day | Simple to build and explain. updated\_at is written by the database, never copied from business data, and a one-day overlap catches late commits | Two changes to one row between extracts collapse into one |

## Known limitations

Saying these before an interviewer finds them is a strength.

- **Arrival is simulated.** Real feeds also fail in ways this generator doesn't model, such as partial files and clock skew between systems.
- **The data is small and old.** About 100,000 orders from 2016 to 2018, replayed on a simulated clock. Scaling claims come only from the labelled load test.
- **Customer history is inferred.** Address versions start at the first order seen with the new address, and a delivery address is not necessarily a home.
- **Some questions are out of scope:** returns, seller history and cancellation timing aren't in the data, so the project doesn't answer them.
- **No time zones.** Timestamps are treated as Brasília local time; Brazil spans several zones, so cross-region comparisons at the hour level would be unreliable.
- **Two versions of the reviews file.** The GitHub mirror's reviews file (100,000 rows) is older than Kaggle's (99,224). This spec uses Kaggle's, so anyone reproducing it from the mirror will see slightly different review numbers.
- **Observational findings.** The link between late delivery and low scores is an association; region, category and carrier could all play a part.
- **Ad-hoc reads of main** aren't consistent across tables during a publish; reporting reads releases instead.
- **The evaluation measures known anomaly types.** A detection rate on injected anomalies says nothing about kinds of failure you didn't inject.
- **Polling extraction can miss intermediate states.** If a customer's row changes twice between two daily extracts, only the second state is seen. The risk is small here, because only 6 people change address more than once; change data capture would remove it.
- **No Redshift.** To keep the project free, dashboards are served from PostgreSQL. The release-consistency design is the same, but there is no hands-on Redshift tuning; be ready to explain distribution keys, sort keys and COPY from S3 in words.

## Phased build plan

Build in six phases over roughly 15 weeks alongside college; each phase ends in something that runs and can be demonstrated. The week counts are estimates, not commitments. You can list the project on your resume after Phase 2, and use the full bullets after Phase 4.

| Phase | Weeks | Build | Done when |
| --- | --- | --- | --- |
| 0. Foundations | 1 | Kaggle download, profiling scripts rerun, metrics.yml, repository skeleton, local Docker stack (PostgreSQL, Spark with Iceberg, Airflow) | Your numbers match or update Data sources, and the shared and all eight metric definitions are written |
| 1. Happy path | 2 to 4 | Generator without defects; bronze with its file manifest, silver and gold locally; fact\_order\_items, fact\_order\_fulfillment, dim\_customer as SCD Type 2, dim\_geography; first dashboard | The truth test passes on 60 replayed days, and rerunning any day gives identical output |
| 2. Late data | 5 to 7 | Reversible defects and the manifest; deduplication; milestone rules; corrections; inferred members; late dimension changes; dirty-date recompute; published metrics and the restatement report | The custom Airflow image and chronos\_daily run a serial backfill; all nine scenario tests and the fixture truth test pass in CI with defects on; the real-slice truth test passes locally (`make truth`) |
| 3. DataSentinel and releases | 8 to 11 | Branch-based write-audit-publish; release manifest and tags; severity levels; GX checks; volume, freshness and drift detection; metrics store; Streamlit console; alerts; evaluation replay | The evaluation report on the held-out period is written, the release consistency test passes, scenario 8's main-unchanged check passes (its expected-failure marker removed), and rerunning a published date republishes nothing |
| 4. AWS | 12 to 14 | S3, Glue 5.1, Glue Data Catalog, Athena; Airflow driving AWS; release loads into PostgreSQL; Terraform; everything on the AWS free plan | A full truth replay runs on AWS, with runtime and credits used recorded, and $0 charged |
| 5. Finish | 15 | README, architecture and data-model diagrams, decision log, short write-up, demo video; optional load test; optional Glue 6.0 migration | A stranger can run it from the README and understand the results in five minutes |

If an interview arrives mid-build, talk about the phase you have finished and what comes next. A correct local pipeline beats a half-working cloud one.

## Getting started: week 1

Week 1 ends with the data profiled, the metrics defined, and a local Spark-plus-Iceberg stack proven able to MERGE, branch and publish. Don't create an AWS account yet: the free plan's six-month clock starts at sign-up (see Zero cost), so open it at Phase 4.

- [ ] **Accounts:** a GitHub repository and a Kaggle account.
- [ ] **Install:** Docker Desktop; Python 3.11, to match Glue 5.1; Java 17 for local Spark; VS Code; a SQL client such as DBeaver.
- [ ] **Download Olist** from Kaggle into `data/raw/olist/`, and keep it out of Git with `.gitignore`.
- [ ] **Profile it:** run `python profile_olist.py data/raw/olist`, then run measure\_spec.py on the same folder, and compare both outputs with the Data sources and Business questions sections. Note any differences in the README.
- [ ] **Define metrics:** write the shared definitions and all eight metric definitions from the Business questions section into `docs/metrics.yml`.
- [ ] **Repository skeleton:** create the folders from the Architecture section, plus `pyproject.toml`, `ruff` for linting, `pytest`, and a GitHub Actions workflow that runs both.
- [ ] **Local services:** a `docker-compose.yml` with PostgreSQL; add Airflow 3.4.0 from its official Docker Compose quick start, keeping the image tag pinned.
- [ ] **Spark with Iceberg:** install PySpark 3.5.6 (pip install pyspark==3.5.6) and configure a local Iceberg catalog (below).
- [ ] **Smoke test:** create a table, MERGE into it, write to a branch, read the branch three ways, and fast-forward main (below). If this works, every hard mechanism in the project is available to you.

**Local Spark session.** Configuration keys follow Iceberg's Spark guide ([Iceberg docs](https://www.mintlify.com/apache/iceberg/engines/spark/getting-started)); the jar matches Glue 5.1's Spark 3.5.6, Scala 2.12 and Iceberg 1.10.0.

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("chronos-local")
    .config("spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "warehouse")
    .getOrCreate()
)
```

**Smoke test.** Branch writes use `spark.wap.branch` ([Iceberg docs](https://iceberg.apache.org/docs/1.6.0/spark-writes)), and publishing uses the `fast_forward` procedure ([Iceberg docs](https://iceberg.apache.org/docs/1.5.2/branching)).

```sql
CREATE NAMESPACE IF NOT EXISTS local.db;

CREATE TABLE local.db.smoke (id INT, status STRING) USING iceberg
  TBLPROPERTIES ('format-version' = '2', 'write.wap.enabled' = 'true');

INSERT INTO local.db.smoke VALUES (1, 'placed');

MERGE INTO local.db.smoke t
USING (SELECT 1 AS id, 'delivered' AS status) s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET t.status = s.status
WHEN NOT MATCHED THEN INSERT *;

ALTER TABLE local.db.smoke CREATE BRANCH audit;
SET spark.wap.branch = audit;
INSERT INTO local.db.smoke VALUES (2, 'placed');
SELECT count(*) FROM local.db.smoke;                        -- expect 2: reads follow spark.wap.branch
RESET spark.wap.branch;
SELECT count(*) FROM local.db.smoke;                        -- expect 1: main is untouched
SELECT count(*) FROM local.db.smoke VERSION AS OF 'audit';  -- expect 2: the branch by name
SELECT count(*) FROM local.db.smoke.branch_audit;           -- expect 2: branch-qualified table name
CALL local.system.fast_forward('db.smoke', 'main', 'audit');
SELECT count(*) FROM local.db.smoke;                        -- expect 2: main now matches the branch
```

This test hasn't been run: the workspace where this spec was written couldn't download the Iceberg jar, so the syntax comes from the linked docs. Reading a branch by name uses VERSION AS OF with the branch name, or a branch\_ suffix on the table name ([Iceberg docs](https://iceberg.apache.org/docs/1.5.0/spark-queries), [Iceberg docs](https://iceberg.apache.org/docs/1.6.0/branching)). Check every count yourself. The first one matters most: the pipeline relies on jobs reading the run's branch while spark.wap.branch is set. If it returns 1, have every job read its branch explicitly instead, with spark.read.option("branch", ...) or VERSION AS OF. If any step fails, fix the setup now; every later phase depends on these operations.

## Zero cost: AWS free plan and safety

The whole project is built to cost nothing. Phases 0 to 3 and 5 run on your laptop with free, open-source tools. Phase 4 uses an AWS account on the free account plan, which AWS describes as ensuring you won't incur any charges while you explore AWS services ([AWS](https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/free-tier-plans.html)). The plan ends after 6 months or when its credits run out, whichever comes first, and it won't charge you until you upgrade to a paid plan ([AWS blog](https://aws.amazon.com/pt/blogs/aws/aws-free-tier-update-new-customers-can-get-started-and-explore-aws-with-up-to-200-in-credits/)).

**What runs where**

| Part | Where | Why |
| --- | --- | --- |
| Spark, Iceberg, Airflow, PostgreSQL, DuckDB, Streamlit | Your laptop, in Docker | Free and open source |
| S3, Glue jobs, Glue Data Catalog | AWS free plan (Phase 4) | AWS lists Glue among the services available on the free plan ([AWS](https://docs.aws.amazon.com/accounts/latest/reference/supported-services-sign-up-new.md)); usage draws on the plan's credits, never on your card |
| Athena | AWS free plan if it is available to your account; otherwise DuckDB on your laptop | AWS's list includes Athena, but one hands-on report saw it restricted on the free plan ([Classmethod](https://dev.classmethod.jp/en/articles/try-new-aws-free-tier-2025/)). Check in the console when Phase 4 starts |
| Dashboard database | PostgreSQL on your laptop | Redshift is listed for paid-plan accounts only ([AWS](https://docs.amazonaws.cn/en_us/accounts/latest/reference/supported-services-sign-up-new.md)), so it is out of a zero-cost project |

**Rules that keep it at zero**

- [ ] Open the AWS account only when Phase 4 starts, because the six-month clock begins at sign-up.
- [ ] Choose the **Free plan** at sign-up, never the Paid plan, and never accept a prompt to upgrade.
- [ ] Don't join AWS Organizations or set up Control Tower: AWS says doing so upgrades a free plan automatically ([AWS](https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/free-tier-plans.html)).
- [ ] Keep Glue jobs at 2 DPUs and avoid crawlers, so the credits last ([pricing summary](https://www.eon.io/blog/aws-glue-pricing), [Hevo](https://hevodata.com/learn/aws-glue-pricing/)).
- [ ] Watch the remaining credits in the AWS Billing console. If they run low, stop and keep the rest of the work local.
- [ ] Give each job its own least-privilege IAM role; never use root credentials.
- [ ] Block public access on every S3 bucket.
- [ ] Define everything in Terraform, so `terraform destroy` removes it all.
- [ ] Keep credentials out of the repository; use environment variables or AWS profiles.
- [ ] Set glue\_version = "5.1" and the S3A region explicitly on every Glue job (see Architecture).

**If the credits ever run out,** the free plan ends rather than billing you. Everything you need to demonstrate the project still runs locally, so keep the local stack working through Phase 5.

## Resume bullets and interview questions

Use three bullets, and replace every \[N\] with a number you measured. Don't add the project to your resume until Phase 2 is done, and claim scale only if you ran the labelled load test.

- Built Chronos, an AWS lakehouse (S3, Glue PySpark, Apache Iceberg, Athena, PostgreSQL, Airflow, Terraform) that replays \~100K real Olist marketplace orders as a daily feed with late, duplicate, out-of-order and corrected events; a truth test proves its business tables converge exactly to the source.
- Modelled a star schema with an order-fulfilment accumulating snapshot, role-playing date and geography dimensions, SCD Type 2 customers and inferred members; late events and late dimension changes update only affected dates, and reruns are idempotent (\[N\] scenario tests in CI).
- Built DataSentinel, a write-audit-publish gate on Iceberg branches that publishes cross-table-consistent releases to the serving database in one transaction; its seasonality-aware volume and drift checks caught \[N\]% of \[N\] injected anomalies with \[N\]% false alarms on held-out days, including Black Friday.

**Questions this project prepares you for, and where the answer lives**

| Likely question | Where your answer is |
| --- | --- |
| Why this dataset, and why not invent what it lacks? | Why Olist; Decision log |
| What in your data is real, and what is simulated? | What's real and what's simulated |
| How do you know the pipeline is correct? | Testing and evaluation (truth test) |
| What is the grain of each fact table? | Data model |
| Why customer\_unique\_id and not customer\_id? | Data sources, Data model |
| How did you attribute order reviews to categories? | Business questions (why questions 4 and 7 are defined this way) |
| How did you handle customers who joined too late to measure retention? | Business questions, question 5 |
| A delivery event arrives three days late. What exactly changes? | Late-arriving data, scenario 2 |
| An address change arrives after the orders that used it. What then? | Data model; Late-arriving data, scenario 6 |
| The job fails halfway. What state is the data in, and how do you rerun it? | Late-arriving data, scenarios 7 and 8; Publishing |
| Two tables publish separately. Can a dashboard see half a release? | Publishing |
| How do you avoid loading a file twice? | Architecture (bronze); Late-arriving data, scenario 7 |
| How do you handle duplicates? | Late-arriving data, scenario 1 |
| Why an accumulating snapshot? | Data model |
| Why not put region in the customer dimension? | Data model; Decision log |
| Why not block every bad row? | DataSentinel (severity levels) |
| Why Iceberg, and why PostgreSQL instead of Redshift? | Architecture; Decision log |
| Why Spark for 100,000 orders? | Decision log |
| How did you choose anomaly thresholds, and what is your false-alarm rate? | DataSentinel; Testing and evaluation |
| Wouldn't Black Friday trigger your volume check? | DataSentinel |
| What are the weaknesses of this project? | Known limitations |
| What would you change at 100 times the volume? | Your Phase 4 performance numbers, and the load test if you ran it |
| An address update reaches the source three days late. Does your high-water mark miss it? | Event model (source database); Orchestration, step 3; Decision log |
| Your event ledger keeps simulated arrival times. How can it pass a truth test? | Testing and evaluation (the three-part truth test) |
| The 10 AM file never arrives, but the 11 AM file does. How do you notice? | Event model (completion marker); DataSentinel (completeness check) |
| A late event changes June. Which numbers are recomputed? | Late-arriving data (dirty periods) |
| A defect almost never happens. How does your anomaly check score it? | DataSentinel (when the baseline has no spread) |

## What changed in revision 5

- **Zero cost.** Dashboards are served from PostgreSQL instead of Redshift Serverless, because Redshift needs a paid AWS plan. Releases still load in one transaction.
- **Phase 4 on the AWS free plan,** which never charges: S3, Glue and the Glue Data Catalog, plus Athena if the free plan allows it on your account (DuckDB locally if not). The cost section is now a list of rules that keep it at zero.
- Updated the architecture, publishing, orchestration, tests, decision log, limitations, resume bullets and diagram to match.

## What changed in revision 4

- **Published metrics grain.** A single business date couldn't hold monthly, quarterly and cohort metrics. The grain is now metric, period type, period start, slice and release, with numerator and denominator.
- **Dirty periods.** Each metric now has a written rule for which periods a changed order dirties, and periods are recomputed whole, because ranks and shares can't be patched row by row.
- **What a run processes.** Runs read bronze from the last published release's watermark, so a blocked run's events are picked up again, never lost.
- **PostgreSQL trigger.** updated\_at comes from a BEFORE INSERT OR UPDATE trigger on the replay clock; a column default only fires on INSERT.
- **Completeness.** The producer's completion marker lists every file it sent, so DataSentinel can notice a missing file, not only a late one.
- **Zero-spread baselines.** Rare defects fall back to a rate-based limit when the median absolute deviation is zero; checks warn only during warm-up; every decision records its method.
- **Stable hashing.** event\_id and surrogate keys use SHA-256, never Python's hash().
- **measure\_spec.py** is now a reference implementation that writes every breakdown to CSV. Categories are ranked before review statistics are joined, retention uses an inclusive boundary, and sellers need 6 ranked months before being classified (9 sellers, not 13).
- **Smoke test** creates its namespace and checks that reads really come from the branch.

## What changed in revision 3

- **Truth testing split into three.** Business truth for current-state tables against an independent reference build, an event-ledger test and a publication-history test, all after a drain period. The old wording compared every gold table with the source, which can't hold for the event ledger or the published-metrics log.
- **Late dimension changes now reach the warehouse.** updated\_at is set by the database when a row is written, and a separate address\_effective\_from carries business time. Before, updated\_at held the purchase time, so an update arriving three days late fell outside the high-water mark and was missed.
- **Payments travel with order\_placed.** All 160 never-approved orders have payment rows, which would never have arrived on payment\_approved.
- **Event identity.** event\_id is built from each event type's logical key and version, so two reviews on one order can't collide; a correction is the next version of the same event.
- **Source schema made explicit:** the customer master, its two timestamps, and the rule that the pipeline never reads the generator's ground truth.
- **Metrics and numbers.** Added a qualifying-purchase definition and review tie-breakers; cohorts exclude people who first bought in 2016; every number is now computed with measure\_spec.py on the reliable window, exactly as each metric is defined, including quarterly category flags and seller rankings.
- **Platform.** New Glue jobs now default to 6.0, so pinning 5.1 matters more; local PySpark is pinned to 3.5.6.
- **New source defects:** 14 delivered orders without an approval and 1 order without payments.
- **Added** a decision-log row and a known limitation on polling extraction.

## What changed in revision 2

- **Real data only for business facts.** Removed simulated returns, seller moves, seller tiers, cancellation times and scaled-up volume. Added the provenance classes, the reversible-defect rule, the truth test and a separate evaluation namespace.
- **Metrics.** Added shared definitions (revenue, business date, reliable window, open orders). Replaced the returns question with delivery-promise accuracy and late-delivery-versus-satisfaction, both real. Category satisfaction now uses single-category orders; retention excludes cohorts too recent to observe; seller ranking defines ties and minimums.
- **Model.** Regional metrics use geography keys on the facts, and region lives only in dim\_geography. dim\_customer's effective dates are labelled inferred, and late dimension changes re-key facts. Sellers and products are Type 1. The published-metrics grain now includes the slice and the release.
- **Publishing.** Replaced views over published runs with a release manifest, tagged snapshots and a one-transaction Redshift load, with a stated guarantee for each kind of reader.
- **Quality.** Four severity levels, with baselines for the real Olist defects, so a bad source row is flagged or quarantined while only a bad run blocks.
- **Bronze.** Immutable landing files plus a file manifest, which removes the earlier contradiction between "append-only" and "overwrite the partition".
- **Platform.** Clarified what the Glue Data Catalog serves; pinned the Glue version and S3A region; moved to Airflow 3.4 with pinned constraints.
- **New sections:** What's real and what's simulated, Publishing, Decision log and Known limitations.

## Glossary

Plain-language meanings of the terms this doc uses, each explained as it applies to Chronos. Practise saying each one aloud in a sentence; interviewers test whether you can explain them, not whether you recognise them.

| Term | Meaning here |
| --- | --- |
| Lakehouse | Data stored as open-format files in S3, with a table format on top that adds updates, transactions and history, so it can be queried like a warehouse |
| Bronze, silver, gold | Raw as received; cleaned and deduplicated; modelled for business use. Also called the medallion layers |
| Observed, derived, simulated | This project's three classes of data: as Olist recorded it; computed from it by a written rule; created by the generator to test the pipeline |
| Truth test | The check that, once every simulated delay has drained, the business tables equal an independent build from the source; companion tests check the event ledger and the publication history against the ground-truth manifest |
| Star schema | Fact tables of measurable events in the middle, joined to dimension tables that describe them (who, what, where, when) |
| Grain | What exactly one row of a fact table represents, such as one order item. Decide it before anything else |
| Fact table | Rows of measurements at a fixed grain: prices, counts, durations |
| Dimension table | Descriptive attributes used to filter and group facts: customer city, product category, date |
| Role-playing dimension | One dimension table used in several roles by one fact table, such as dim\_date as purchase date and delivery date |
| Outrigger dimension | A dimension referenced from another dimension instead of from the fact table; allowed by Kimball, but sparingly |
| Accumulating snapshot | A fact table with one row per process instance (here, per order) that is updated as each milestone happens |
| SCD Type 1 | A slowly changing dimension that overwrites old values; history is lost |
| SCD Type 2 | A slowly changing dimension that adds a new row per change, with effective dates, so facts can join to the value true at the time |
| Natural key, surrogate key | The business identifier (customer\_unique\_id), and the warehouse's own key for one version of that row |
| Inferred member | A placeholder dimension row created when a fact references something not yet loaded; filled in when the real row arrives |
| Event time, ingestion time | When something happened, and when the pipeline received it. Late data is the gap between them |
| Late-arriving data | Events that turn up after the period they belong to was already processed |
| Restatement | A previously published number changing because late or corrected data arrived |
| Censoring | Leaving out people who haven't been observed long enough, so a retention rate isn't dragged down by customers who simply had no time to return |
| Idempotent | Running the same job twice gives exactly the same result as running it once |
| Backfill | Rerunning a pipeline for past dates, for example after fixing a bug |
| High-water mark | The latest updated\_at value already loaded, used to extract only newer changes |
| File manifest | A table of every landing file already loaded, so a retry never loads a file twice |
| MERGE (upsert) | One statement that updates matching rows and inserts new ones |
| Apache Iceberg | An open table format that adds MERGE, snapshots, branches and schema changes to files in S3 |
| Snapshot, time travel | Each Iceberg commit creates a snapshot; time travel queries the table as of an earlier snapshot |
| Branch, tag | A branch is a named line of snapshots written to without changing main; a tag is a name pinned to one snapshot, which can carry a retention period |
| Fast-forward | Moving main to the head of a branch, possible only when main hasn't moved since the branch was created |
| Write-audit-publish | Write to a branch, check it, and only then make it visible on main |
| Release, release manifest | A set of table snapshots audited and published together, and the record of which snapshot of each table it contains |
| Severity: block, quarantine, flag, warn | Stop the release; hold a row aside with its reason; keep a row but mark it; publish with a note |
| Partition | Splitting a table by a column, such as date, so queries and rewrites touch only the relevant files |
| Modified z-score, MAD | An outlier score based on the median and the median absolute deviation, so outliers can't distort their own baseline |
| PSI | Population Stability Index: a single number for how much a distribution has shifted from a baseline |
| DPU, RPU | AWS billing units of compute: Data Processing Units for Glue, and Redshift Processing Units for Redshift Serverless (not used here, but worth knowing) |
| Reporting period, dirty period | The month, quarter or cohort month a metric value covers; a period that a late or corrected event has changed, and that must be recomputed whole |
| Processing watermark | The bronze ingestion time up to which the last published release processed events; the next run starts from it |
| Completion marker | The producer's list of every file it sent for a date, with row counts and checksums, so a missing file can be noticed |

## Sources

Checked 21 Nov 2026. Dataset numbers come from running profile\_olist.py and measure\_spec.py on the data, not from any web page.

**Dataset**

- [Brazilian E-Commerce Public Dataset by Olist on Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce): the official download; verify its licence and file versions there
- [GitHub mirror of the dataset](https://github.com/spdrio/Brazilian-E-Commerce-Public-Dataset-by-Olist): the files the first revisions were profiled on (all but the reviews file match Kaggle)
- [Licence and file overview](https://scripts-and-tables.github.io/daa/data/) (scripts-and-tables)
- [Published file row counts](https://aioconquer.aivietnam.edu.vn/posts/why-customers-never-return-the-dark-side-of-olist-s-logistics) (AIO Conquer)
- [Olist data dictionary](https://pavelgrigoryevds.github.io/olist-deep-dive/contents/01_data_description_and_exploration/01_data_description.html)
- [Black Friday Brasil 2017 date](https://www.guiadasemana.com.br/compras/evento/black-friday-brasil-2017-2017-11-24) (Guia da Semana)
- [IBGE: Brazil's five major regions](https://biblioteca.ibge.gov.br/visualizacao/periodicos/2/bn_2022_v30_art02.pdf)

**Alternative datasets**

- [TheLook eCommerce tables](https://vanna.ai/docs/streamlit.html) (Vanna) and [TheLook as a fictitious store](https://cloud.google.com/gemini/docs/conversational-analytics-api/data-agent-authored-context-bq) (Google Cloud)
- [DataCo dataset size](https://arxiv.org/pdf/2604.05254) (arXiv), [described as simulated](https://medium.com/@ansharahinayat/exploratory-data-analysis-dataco-smart-supply-chain-dataset-da6cc74c2bee), [described as real](https://tech-for-dev.vercel.app/articles/3739995), [late-delivery rate](https://freda-erinmwingbovo.github.io/projects/dataco/dataco_report.html)
- [UCI Online Retail II](https://archive.ics.uci.edu/dataset/502/online+retail+ii) and [its licence](https://archive-beta.ics.uci.edu/dataset/502/online+retail+ii)
- [REES46 dataset size](https://arxiv.org/pdf/2505.19643) (arXiv) and [event types](https://github.com/IIAndrewII/Customer_Behavior)
- [TPC-DS schema](https://datacadamia.com/data/type/relation/benchmark/tpcds/schema) (Datacadamia)
- [Simulated "Amazon Sales Report" upload](https://baselight.app/u/kaggle/dataset/arpit2712_amazonsalesreport) (Baselight)

**Data modeling**

- [Accumulating snapshot fact table](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/accumulating-snapshot-fact-table/) (Kimball Group)
- [Design Tip #145: Time Stamping Accumulating Snapshot Fact Tables](https://www.kimballgroup.com/2012/05/design-tip-145-time-stamping-accumulating-snapshot-fact-tables/) (Kimball Group)
- [Outrigger dimensions](https://www.kimballgroup.com/?p=19262) and [role-playing dimensions](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/role-playing-dimension/) (Kimball Group)
- [Design Tip #78: Late Arriving Dimension Rows](https://decisionworks.com/2006/04/design-tip-78-late-arriving-dimension-rows/) (DecisionWorks)
- [Early arriving facts and late arriving dimensions](https://www.leapfrogbi.com/?p=13291) (LeapFrogBI)

**Iceberg, Spark and AWS**

- Iceberg Spark writes: [MERGE INTO](https://iceberg.apache.org/docs/1.8.0/spark-writes), [branch writes](https://iceberg.apache.org/docs/1.6.0/spark-writes), [one source row per target row](https://www.mintlify.com/apache/iceberg/engines/spark/writes), [Spark catalog setup](https://www.mintlify.com/apache/iceberg/engines/spark/getting-started)
- [Iceberg reliability: atomic commits per table](https://iceberg.apache.org/docs/1.7.1/reliability)
- [Iceberg branching and write-audit-publish](https://iceberg.apache.org/docs/1.5.2/branching) and [branch and tag retention](https://iceberg.apache.org/docs/1.5.1/branching/)
- [Write-audit-publish with Iceberg branches](https://aws.amazon.com/blogs/big-data/build-write-audit-publish-pattern-with-apache-iceberg-branching-and-aws-glue-data-quality) (AWS Big Data Blog) and [reads and writes on the WAP branch](https://community.cloudera.com/t5/Developer-Blogs/Iceberg-WAP-Failsafe-ETL-with-Iceberg-and-CDE/m-p/413008) (Cloudera)
- [AWS Glue versions and default version](https://docs.aws.amazon.com/glue/latest/dg/release-notes.html), [Glue 6.0 migration guide](https://docs.aws.amazon.com/glue/latest/dg/migrating-version-60.html), [Introducing Glue 6.0](https://aws.amazon.com/blogs/big-data/introducing-aws-glue-6-0-for-apache-spark/), [Upgrading to Glue 6.0](https://aws.amazon.com/blogs/big-data/upgrade-aws-glue-jobs-to-glue-6-0-with-ai-powered-spark-upgrades/), [Glue 5.0 migration guide](https://docs.aws.amazon.com/ja_jp/glue/latest/dg/migrating-version-50.html)
- [Glue Auto version setting](https://dev.classmethod.jp/en/articles/20261106-aws-glue-auto-upgread-version/) (Classmethod) and [Glue 5.1 S3A region default](https://www.repost.aws/articles/ARjELjm9_jRxejMb1havX8xg/breaking-changes-checklist-migrating-aws-glue-jobs-to-version-5-0-5-1) (AWS re:Post)
- [Athena time travel and version travel queries](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-time-travel-and-version-travel-queries.html) and [Athena MERGE on Iceberg](https://aws.amazon.com/blogs/big-data/perform-upserts-in-a-data-lake-using-amazon-athena-and-apache-iceberg)
- [Redshift: updating and inserting with MERGE](https://docs.aws.amazon.com/redshift/latest/dg/t_updating-inserting-using-staging-tables-.html), [Redshift with Iceberg tables in the Data Catalog](https://docs.aws.amazon.com/redshift/latest/dg/querying-iceberg.html), [Redshift Data API batch statements run as one transaction](https://docs.aws.amazon.com/botocore/latest/reference/services/redshift-data/client/batch_execute_statement.html)
- [MWAA Serverless supported operators](https://docs.aws.amazon.com/it_it/mwaa/latest/mwaa-serverless-userguide/operators.html)
- [Airflow release notes (3.4.0)](https://airflow.apache.org/docs/apache-airflow/stable/release%5Fnotes.html), [Airflow support dates](https://endoflife.date/airflow) (endoflife.date) and [reproducible installs with constraints](https://airflow.apache.org/docs/stable/installation/installing-from-pypi.html)

**Data quality**

- [GX Spark execution](https://greatexpectations.io/blog/great-expectations-now-supports-execution-in-spark), [GX stewardship update](https://www.greatexpectations.io/blog/an-update-from-great-expectations), [GX releases](https://newreleases.io/project/pypi/great-expectations/release/1.23.2)
- [NIST: modified z-score for outliers](https://itl.nist.gov/div898/handbook/eda/section3/eda35h.htm)
- [Review of population stability testing, including PSI thresholds](https://arxiv.org/pdf/2303.01227) (arXiv)

**Pricing and credits**

- [Glue pricing summary](https://www.eon.io/blog/aws-glue-pricing) (Eon) and [Glue minimums](https://hevodata.com/learn/aws-glue-pricing/) (Hevo)
- [Athena pricing](https://docs.aws.amazon.com/whitepapers/latest/big-data-analytics-options/amazon-athena.md) (AWS whitepaper)
- [Redshift Serverless at 4 RPUs](https://aws.amazon.com/blogs/big-data/amazon-redshift-serverless-at-4-rpus-high-value-analytics-at-low-cost) (AWS), [Redshift free trial](https://aws.amazon.com/redshift/free-trial), [default base capacity](https://www.issoh.co.jp/md/tech/15469) (issoh, Japanese)
- [AWS Free Tier credits and free plan](https://aws.amazon.com/about-aws/whats-new/2025/07/aws-free-tier-credits-month-free-plan) (AWS), [free-plan restrictions](https://aws.amazon.com/ar/blogs/aws/aws-free-tier-update-new-customers-can-get-started-and-explore-aws-with-up-to-200-in-credits/) (AWS), [free-plan expiry](https://i-programmer.info/news/141-cloud-computing/18176-understanding-aws-free-tier-for-new-subscribers.html) (i-programmer)
