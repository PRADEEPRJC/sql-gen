Here is a concise, professional Markdown structure for Project SnowPulse. This format is designed to be copy-pasted directly into a README.md or a project documentation tool.
❄️ Project SnowPulse: Lineage & Quality Command Center
1. Project Overview
SnowPulse is a specialized data observability application built in Streamlit. it provides end-to-end visibility into Snowflake environments by mapping complex data flows, extracting hidden transformation logic from stored procedures, and overlaying real-time data quality metrics.
2. High-Level Requirements
🔍 Discovery & Lineage
 * Recursive Trace: Map dependencies for Tables, Views, and Stored Procedures.
 * Dynamic SQL Support: Utilize Snowflake ACCESS_HISTORY to capture runtime-generated lineage.
 * Column-Level Granularity: Trace specific fields from source to final consumption.
🧠 Logic Extraction
 * Transformation Parsing: Extract JOIN conditions, WHERE filters, and CASE statements using sqlglot.
 * Procedure Profiling: Link parent procedures to the child queries they execute.
✅ Data Quality & Health
 * Quality Overlay: Display pass/fail status of DQ tests (dbt, Great Expectations, or Snowflake Metrics) on lineage nodes.
 * Freshness Monitoring: Visual indicators for data latency/staleness.
 * Impact Analysis: Highlight downstream "blast radius" when a source quality check fails.
3. Technical Design
🏗️ Architecture
 * Backend: Python 3.9+ / Snowpark.
 * SQL Parser: sqlglot (Snowflake dialect).
 * Frontend: Streamlit in Snowflake (SiS).
 * Graph Engine: streamlit-agraph (React-based interactive canvas).
📊 Data Strategy
| Component | Source of Truth | Purpose |
|---|---|---|
| Lineage Skeleton | SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY | Captures actual data movement (Static & Dynamic). |
| Logic/Filters | SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY | Provides the SQL text for parsing transformations. |
| Health Metrics | SNOWFLAKE.CORE.DATA_METRIC_RESULTS | Provides DQ scores and test outcomes. |
| Metadata | INFORMATION_SCHEMA | Fetches table schemas and object owners. |
4. UI/UX Features
 * The Infinity Canvas: A zoomable, draggable graph with a minimap.
 * Pathlighting: Clicking a column dims the rest of the graph and highlights the specific data path.
 * The Logic Drawer: A slide-out panel showing the "Business Logic" and SQL snippets for any selected edge.
 * Traffic Light Nodes: Nodes change color (Green/Yellow/Red) based on the current health score.
5. Implementation Roadmap
Phase 1: Foundation (Weeks 1-2)
 * [ ] Configure Snowflake RBAC for metadata access.
 * [ ] Build the Python TraceEngine to query and flatten ACCESS_HISTORY.
Phase 2: Logic & Parsing (Weeks 3-4)
 * [ ] Integrate sqlglot to parse extracted SQL text.
 * [ ] Map JOIN/WHERE predicates to the graph edges.
Phase 3: Quality & UI (Weeks 5-6)
 * [ ] Connect DQ result tables to lineage nodes.
 * [ ] Develop the Streamlit interactive dashboard and "Impact Analysis" mode.
Would you like me to provide the Python boilerplate code for the "TraceEngine" that queries the ACCESS_HISTORY view?
