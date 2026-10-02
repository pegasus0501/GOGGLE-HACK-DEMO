# DE Swarm
## AI On-Call Incident Response for Data Pipelines

DE Swarm is a multi-agent incident-response system for modern data platforms.

The goal is to build an AI-assisted on-call layer that observes data pipelines, identifies failures or data-quality incidents, diagnoses the root cause, proposes a safe remediation, independently reviews that remediation, and either executes a low-risk fix or escalates the incident to a human engineer.

The system is designed around a typical Google Cloud data platform:

```text
GCS
  ↓
Cloud Composer / Airflow
  ↓
Dataproc / PySpark + Dataform / BigQuery SQL
  ↓
BigQuery
  ↓
Dashboards / Downstream Consumers
```

DE Swarm sits beside the production platform as an **incident-response control plane**. It does not replace Airflow or the data platform itself.

---

# 1. Problem

Data platforms often contain many scheduled jobs, DAGs, transformations, and downstream dependencies.

A single issue can cause:

- Airflow task failures
- schema drift
- stale tables
- null spikes
- duplicate records
- unexpected row-count changes
- delayed upstream files
- Dataproc failures
- BigQuery errors
- downstream dashboards serving invalid data

Traditional monitoring detects these problems, but an engineer still has to:

1. inspect the logs,
2. determine the root cause,
3. understand the blast radius,
4. search previous incidents,
5. determine the safest remediation,
6. validate the remediation,
7. execute the fix,
8. verify the pipeline,
9. document the incident.

DE Swarm automates as much of this workflow as is safely possible.

---

# 2. Core Idea

The system follows this control loop:

```text
OBSERVE
   ↓
DETECT INCIDENT
   ↓
TRIAGE
   ↓
SPECIALIST ANALYSIS
   ↓
RETRIEVE HISTORICAL KNOWLEDGE
   ↓
PROPOSE REMEDIATION
   ↓
VALIDATE IN SANDBOX
   ↓
INDEPENDENT REVIEW
   ↓
CONFIDENCE + POLICY GATE
   ↓
AUTO-FIX OR HUMAN APPROVAL
   ↓
VERIFY
   ↓
STORE INCIDENT KNOWLEDGE
```

The core design principle is:

> Agents reason and propose actions. Deterministic code controls execution, permissions, confidence gates, and production safety.

---

# 3. High-Level Architecture

```text
┌─────────────────────────────────────────────────────────────┐
│                       DATA PLATFORM                         │
│                                                             │
│ GCS → Composer/Airflow → Dataproc/Dataform → BigQuery      │
│                                            ↓                │
│                                   Dashboards / Consumers    │
└──────────────────────────┬──────────────────────────────────┘
                           │
                    Failure / DQ Alert
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                 INCIDENT DETECTION                          │
│                                                             │
│ Cloud Logging / Monitoring                                  │
│          ↓                                                  │
│ Pub/Sub                                                     │
│          ↓                                                  │
│ Incident Created                                            │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│               GOOGLE ADK MULTI-AGENT SYSTEM                 │
│                                                             │
│                    Orchestrator                             │
│                         ↓                                   │
│                    Triage Agent                             │
│                         ↓                                   │
│           ┌─────────────┼─────────────┐                     │
│           ▼             ▼             ▼                     │
│      Schema Agent   Data Quality   Infra / Job              │
│                       Agent        Failure Agent             │
│           └─────────────┼─────────────┘                     │
│                         ↓                                   │
│                Incident Knowledge Base                     │
│                         ↓                                   │
│                 Problem Solver Agent                       │
│                         ↓                                   │
│                  Sandbox Validation                        │
│                         ↓                                   │
│                   Reviewer Agent                           │
│                         ↓                                   │
│                Confidence / Policy Gate                    │
│                    /              \                         │
│                   ▼                ▼                        │
│              Auto Execute      Human Approval               │
│                    \              /                         │
│                         ↓                                   │
│                 Verification Agent                         │
│                         ↓                                   │
│                  Incident Closed                           │
│                         ↓                                   │
│           Store Resolution in Knowledge Base               │
└─────────────────────────────────────────────────────────────┘
```

---

# 4. Main Components

## 4.1 Data Platform

The demo platform uses:

- **Google Cloud Storage** for raw files
- **Cloud Composer / Airflow** for pipeline orchestration
- **Dataproc Serverless / PySpark** for batch ETL
- **Dataform / BigQuery SQL** for warehouse transformations
- **BigQuery** for raw, staging, core, and mart tables
- dashboards or downstream systems as consumers

A possible warehouse layout is:

```text
raw
  ├── orders
  ├── customers
  └── payments

staging
  ├── stg_orders
  ├── stg_customers
  └── stg_payments

core
  ├── fact_orders
  ├── dim_customer
  └── fact_payments

marts
  ├── daily_revenue
  ├── customer_360
  └── order_metrics
```

---

# 5. Incident Detection

DE Swarm is event-driven.

Agents do not continuously poll the data platform.

Failures and alerts come from:

- Airflow task callbacks
- Cloud Logging
- Cloud Monitoring
- BigQuery job failures
- Dataproc failures
- data-quality metric checks
- freshness checks
- scheduled validation jobs

These events are sent to a Pub/Sub topic such as:

```text
pipeline-incidents
```

Example incident payload:

```json
{
  "incident_id": "INC-SCHEMA-001",
  "environment": "production",
  "dag_id": "orders_daily",
  "task_id": "transform_orders",
  "source": "dataproc",
  "error_type": "AnalysisException",
  "error_message": "Column customer_id cannot be resolved"
}
```

---

# 6. Google ADK Multi-Agent Design

Google ADK is used to coordinate the incident-response agents.

For the first version, the agents can live inside one ADK application deployed as one Cloud Run service.

```text
Cloud Run
└── DE Swarm ADK App
    ├── Orchestrator
    ├── Triage Agent
    ├── Schema Agent
    ├── Data Quality Agent
    ├── Infra Agent
    ├── Problem Solver Agent
    ├── Reviewer Agent
    └── Verification Agent
```

A2A is not required for the first version.

A2A can be introduced later if each specialist becomes an independently deployed agent service.

---

# 7. Agent Responsibilities

## Orchestrator

Coordinates the incident lifecycle.

Responsibilities:

- receives the normalized incident
- creates or resumes incident state
- invokes the Triage Agent
- routes to the correct specialist
- invokes knowledge retrieval
- invokes the Problem Solver
- runs validation
- invokes the Reviewer
- calls the confidence/policy gate
- executes or escalates
- invokes post-fix verification

The orchestrator should be primarily deterministic workflow code.

---

## Triage Agent

Classifies the incident.

Possible categories:

```text
SCHEMA_DRIFT
DATA_QUALITY
UPSTREAM_DELAY
INFRASTRUCTURE
PERMISSION
SQL_ERROR
UNKNOWN
```

Tools may include:

- Cloud Logging search
- Airflow task metadata
- Dataproc job metadata
- BigQuery job metadata

Example output:

```json
{
  "category": "SCHEMA_DRIFT",
  "affected_component": "transform_orders",
  "evidence": [
    "customer_id unresolved",
    "input object exists",
    "Dataproc service healthy"
  ]
}
```

---

## Schema Resolution Agent

Handles schema-related incidents.

Examples:

- renamed columns
- removed columns
- additional nullable columns
- incompatible type changes
- schema ordering changes
- missing required fields

Tools:

- current GCS schema
- previous source schema
- BigQuery INFORMATION_SCHEMA
- schema history
- pipeline mapping
- downstream dependency graph

Example output:

```json
{
  "failure_type": "COLUMN_RENAME",
  "old_column": "customer_id",
  "new_column": "cust_id",
  "breaking_change": true,
  "affected_assets": [
    "staging.orders",
    "core.fact_orders",
    "marts.daily_revenue"
  ]
}
```

---

## Data Quality Agent

Handles incidents where the pipeline may technically succeed but the resulting data is invalid.

Checks may include:

- null percentage
- duplicate primary keys
- row-count anomaly
- freshness
- distribution changes
- referential integrity
- threshold violations

Example:

```text
customer_id null rate

Expected: < 2%
Actual:   27%
```

Possible output:

```json
{
  "failure_type": "NULL_RATE_SPIKE",
  "metric": "customer_id_null_rate",
  "expected": "< 0.02",
  "actual": 0.27,
  "severity": "HIGH"
}
```

---

## Infrastructure / Job Failure Agent

Handles:

- Dataproc transient failures
- BigQuery job failures
- Airflow execution failures
- quotas
- timeouts
- resource exhaustion
- permission failures

It primarily diagnoses and proposes whether the correct action is:

```text
RETRY
WAIT
ESCALATE
RESOURCE_CHANGE
PERMISSION_REVIEW
```

---

# 8. Incident Knowledge Base

The Knowledge Base contains previous operational knowledge:

- past incidents
- successful fixes
- failed fixes
- runbooks
- schema history
- common error signatures
- postmortems
- pipeline ownership
- downstream dependencies

A simple first version can use BigQuery tables:

```text
ops.incidents
ops.incident_events
ops.incident_knowledge
ops.schema_history
ops.agent_decisions
```

Later, semantic retrieval can be added using vector search or a RAG system.

Historical incidents are evidence, not authorization.

A previous incident should never automatically trigger the same production fix.

---

# 9. Problem Solver Agent

The Problem Solver receives:

- incident details
- triage result
- specialist diagnosis
- downstream blast radius
- similar incidents
- runbook information

It produces a constrained remediation plan.

Example:

```json
{
  "root_cause": "Upstream renamed customer_id to cust_id",
  "action_type": "UPDATE_SCHEMA_MAPPING",
  "parameters": {
    "source": "cust_id",
    "canonical": "customer_id"
  },
  "risk": "LOW",
  "rollback": "restore previous mapping version"
}
```

Agents should not produce arbitrary shell commands or arbitrary production SQL.

---

# 10. Sandbox Validation

Before a proposed fix is executed, the system validates it in a safe environment.

Checks can include:

```text
transform succeeds
schema matches expectation
row count is valid
null rate is acceptable
duplicate count is acceptable
downstream SQL dry run passes
```

Example result:

```json
{
  "execution_success": true,
  "schema_valid": true,
  "row_count_valid": true,
  "null_check": true,
  "downstream_dry_run": true
}
```

---

# 11. Reviewer Agent

The Reviewer Agent independently challenges the proposed solution.

Its job is not to agree with the Problem Solver.

It checks:

- whether the root cause is supported by evidence
- whether alternative explanations exist
- whether the proposed action addresses the root cause
- whether the blast radius is complete
- whether the action can corrupt data
- whether rollback exists
- whether sandbox validation passed

Possible response:

```json
{
  "approved": true,
  "risk": "LOW",
  "root_cause_supported": true,
  "action_addresses_root_cause": true,
  "requires_human": false
}
```

---

# 12. Confidence Model

Confidence should not be based only on an LLM saying:

```text
"I am 95% confident."
```

DE Swarm calculates a decision score using actual evidence.

Example weighting:

```text
Sandbox validation       35%
Evidence completeness    25%
Reviewer validation      20%
Historical support       10%
Blast-radius confidence  10%
```

Example:

```text
sandbox             1.00
evidence            0.95
reviewer            1.00
history             0.90
blast radius        0.85

final decision score ≈ 0.96
```

The exact weights should later be calibrated against the evaluation dataset.

---

# 13. Policy Gate

Confidence does not equal permission.

Even if confidence is high, the action must be permitted by policy.

Example allowlist:

```text
RETRY_TASK
RETRY_DAG
WAIT_FOR_UPSTREAM
QUARANTINE_FILE
UPDATE_SCHEMA_MAPPING
BLOCK_MART_PUBLISH
```

Actions that should require human approval or remain blocked:

```text
DROP TABLE
DELETE production data
rewrite large historical datasets
change IAM
disable security controls
unknown schema modification
arbitrary SQL
arbitrary shell commands
```

Example decision logic:

```text
High confidence
+ Low risk
+ Sandbox passed
+ Reviewer approved
+ Action is allowlisted
        ↓
AUTO EXECUTE
```

Otherwise:

```text
HUMAN APPROVAL / ESCALATE
```

---

# 14. Executor

The Executor should be deterministic code, not a free-form LLM.

Bad design:

```text
execute_sql(agent_generated_sql)
```

Preferred design:

```text
retry_task(...)
quarantine_partition(...)
update_schema_mapping(...)
block_mart_publish(...)
```

The agent selects an allowed action.

The Executor controls how that action is actually performed.

---

# 15. Verification Agent

After remediation, the system verifies that the incident is truly resolved.

Possible checks:

- Airflow task state
- DAG state
- BigQuery table freshness
- expected row count
- null percentage
- duplicate count
- downstream table health
- dashboard publication status

Only then is the incident closed.

---

# 16. Learning Loop

After every incident, DE Swarm stores:

- incident type
- root cause
- evidence
- proposed action
- reviewer decision
- confidence score
- human approval or override
- execution result
- verification result
- rollback information
- final resolution

This becomes future knowledge and evaluation data.

---

# 17. Security Model

Security is built around least privilege.

Suggested service identities:

```text
de-swarm-control-plane
    read logs
    read BigQuery metadata/data
    read GCS metadata
    inspect Airflow / Dataproc

de-swarm-executor
    narrow remediation permissions only
```

Reasoning agents should be read-only.

The executor should be a separate security boundary with only the privileges needed for approved actions.

The system should not expose tools such as:

```text
run_shell()
execute_arbitrary_sql()
change_iam()
drop_table()
```

to reasoning agents.

---

# 18. Deployment

Initial deployment:

```text
Pub/Sub
   ↓
Cloud Run
   ↓
Google ADK Application
   ↓
Triage / Specialist / Solver / Reviewer / Verification
```

A separate Cloud Run service can host the Executor.

State can be stored in:

```text
Cloud SQL / PostgreSQL
```

Operational and evaluation data can be stored in:

```text
BigQuery
```

---

# 19. Evaluation Strategy

The system should be tested against labeled incidents.

Example evaluation scenarios:

```text
SCHEMA-001   renamed column
SCHEMA-002   added nullable column
SCHEMA-003   incompatible type change

DQ-001       null spike
DQ-002       duplicate keys
DQ-003       row-count anomaly

UPSTREAM-001 missing file
INFRA-001    transient Dataproc failure
IAM-001      permission failure
AMBIG-001    unclear schema mapping
```

Each scenario contains the expected:

- incident classification
- root cause
- action
- required tools
- blast radius
- whether auto-remediation is allowed

Important metrics:

```text
Root Cause Accuracy
Incident Classification Accuracy
Correct Action Rate
Blast Radius Recall
Successful Remediation Rate
Human Override Rate
Escalation Accuracy
False Auto-Remediation Rate
Unsafe Action Count
Mean Time To Diagnose
Mean Time To Recovery
```

The most important safety metric is:

```text
Unsafe autonomous actions = 0
```

---

# 20. Demo Workflow 1
## Pipeline Failure Caused by Schema Change

![Incident 1 - Schema Drift](docs/incident-1-schema-drift.png)

### Scenario

Previous input schema:

```text
order_id
customer_id
amount
order_date
```

New input schema:

```text
order_id
cust_id
amount
order_date
source_version
```

The transform expects:

```text
customer_id
```

Airflow result:

```text
extract_orders      SUCCESS
transform_orders    FAILED
load_orders         NOT RUN
revenue_mart        NOT RUN
```

---

## Flow

### 1. Failure Detection

Dataproc / Airflow reports:

```text
AnalysisException:
Column customer_id cannot be resolved
```

Cloud Logging / Monitoring creates an event.

Pub/Sub publishes:

```text
INC-SCHEMA-001
```

---

### 2. Triage Agent

Tools:

```text
Cloud Logging
Airflow metadata
Dataproc job metadata
```

Output:

```text
Category:
SCHEMA_DRIFT
```

---

### 3. Schema Resolution Agent

Tools:

```text
current source schema
previous schema history
BigQuery INFORMATION_SCHEMA
pipeline dependency graph
```

It discovers:

```text
customer_id → cust_id
```

and determines the blast radius:

```text
staging.orders
core.fact_orders
marts.daily_revenue
```

---

### 4. Knowledge Retrieval

Searches previous incidents.

Example result:

```text
INC-183

Root cause:
upstream column rename

Successful remediation:
canonical schema mapping
```

---

### 5. Problem Solver

Proposed action:

```text
UPDATE_SCHEMA_MAPPING

cust_id
    ↓
customer_id
```

Then:

```text
RETRY_TASK
```

---

### 6. Sandbox Validator

Runs the proposed mapping against test data.

Checks:

```text
PySpark transformation      PASS
schema validation           PASS
row count                   PASS
null checks                 PASS
downstream SQL dry run      PASS
```

---

### 7. Reviewer Agent

The Reviewer independently confirms:

```text
rename supported by evidence
blast radius identified
sandbox passed
rollback exists
risk = LOW
```

Result:

```text
APPROVED
```

---

### 8. Confidence / Policy Gate

Evidence:

```text
specialist diagnosis    PASS
historical evidence     PASS
sandbox validation      PASS
reviewer                PASS
low-risk action          PASS
```

Decision:

```text
AUTO EXECUTE
```

---

### 9. Executor

Executes only allowlisted actions:

```text
UPDATE_SCHEMA_MAPPING
cust_id → customer_id
```

Then:

```text
RETRY transform_orders
```

---

### 10. Verification

Verification Agent checks:

```text
Airflow task          SUCCESS
DAG                   SUCCESS
BigQuery freshness    PASS
row counts            PASS
null checks           PASS
downstream mart       HEALTHY
```

---

### End Result

```text
Root cause identified
        ↓
Safe fix applied
        ↓
Pipeline recovered
        ↓
Incident closed
        ↓
Resolution stored in knowledge base
```

---

# 21. Demo Workflow 2
## Data Quality Incident After Successful Pipeline

![Incident 2 - Data Quality](docs/incident-2-data-quality.png)

### Scenario

The pipeline completes successfully:

```text
extract_orders       SUCCESS
transform_orders     SUCCESS
load_orders          SUCCESS
revenue_mart         SUCCESS
```

But the data-quality checks detect:

```text
customer_id NULL rate

Expected:
< 2%

Actual:
27%
```

This demonstrates that:

> Pipeline success does not necessarily mean data success.

---

## Flow

### 1. Pipeline Completes

Airflow shows:

```text
orders_daily SUCCESS
```

No pipeline execution failure occurs.

---

### 2. Data Quality Metrics Run

BigQuery validation query measures:

```text
NULL_RATE(customer_id) = 27%
```

Allowed threshold:

```text
< 2%
```

---

### 3. Data Quality Alert

Cloud Monitoring receives the failed metric.

Pub/Sub publishes:

```text
INC-DQ-001
```

---

### 4. Triage Agent

The incident contains:

```text
DAG state = SUCCESS
DQ threshold = FAILED
```

Output:

```text
Category:
DATA_QUALITY
```

---

### 5. Data Quality Agent

Tools:

```text
BigQuery read-only queries
row-count checks
null metrics
duplicate checks
freshness metrics
historical baselines
```

Findings:

```text
customer_id null rate = 27%

historical average ≈ 0.7%

affected partition:
2026-10-01

severity:
HIGH
```

---

### 6. Knowledge Retrieval

Searches for previous incidents involving:

```text
NULL_RATE_SPIKE
orders
customer_id
```

Returns relevant historical incidents and runbooks.

---

### 7. Problem Solver Agent

The important difference from Demo 1:

The Solver should **not invent missing customer IDs**.

It proposes containment:

```text
QUARANTINE_BAD_PARTITION
```

and:

```text
BLOCK_MART_PUBLISH
```

This protects consumers from bad data.

---

### 8. Sandbox / Impact Validation

Checks:

```text
Can affected partition be isolated?      YES
Can good historical partitions remain?   YES
Will downstream dashboards use bad data? YES
Will blocking publish prevent impact?    YES
```

---

### 9. Reviewer Agent

Reviewer checks:

```text
Is the DQ failure real?
YES

Would automatic value correction be unsafe?
YES

Is quarantine reversible?
YES

Does blocking publish protect consumers?
YES
```

Result:

```text
APPROVED
```

---

### 10. Confidence / Policy Gate

Because:

```text
metric failure is deterministic
affected partition identified
containment is reversible
reviewer approved
action is allowlisted
```

the system may auto-execute containment.

---

### 11. Executor

Executes:

```text
QUARANTINE affected partition
```

and:

```text
BLOCK current mart/dashboard publish
```

It does **not** modify individual customer values.

The upstream/source team is notified.

---

### 12. Verification Agent

Checks:

```text
bad data isolated
dashboard publish stopped
previous valid data remains available
incident state recorded
```

---

### End Result

```text
Pipeline technically succeeded
             ↓
Data quality failed
             ↓
Bad data identified
             ↓
Affected partition quarantined
             ↓
Bad data prevented from reaching consumers
             ↓
Source team notified
             ↓
Incident stored for future learning
```

---

# 22. Why the Two Demos Matter

The demos intentionally test two very different situations.

## Demo 1

```text
Pipeline Failure
      ↓
Diagnose
      ↓
Fix
      ↓
Recover Pipeline
```

This proves the system can perform **incident diagnosis and remediation**.

## Demo 2

```text
Pipeline Success
      ↓
Data Quality Failure
      ↓
Contain Bad Data
      ↓
Protect Consumers
```

This proves the system understands that **operational success and data correctness are different things**.

Together, these two scenarios demonstrate the real value of DE Swarm.

---

# 23. Future Enhancements

Possible future extensions:

- A2A between independently deployed specialist agents
- MCP-based standardized tool interfaces
- Slack human-approval workflow
- automatic postmortem generation
- RAG over runbooks and historical incidents
- lineage-aware blast-radius detection
- anomaly detection using historical baselines
- automatic incident correlation
- cost-aware remediation
- model/prompt version evaluation
- shadow-mode deployment on real pipeline incidents
- agent performance dashboard

---

# 24. Recommended MVP

Build the first version around:

```text
1 GCS bucket
1 Airflow DAG
1 Dataproc/PySpark job
3–5 BigQuery tables
2 incident scenarios
```

Supported incidents:

```text
1. Schema drift
2. Data quality threshold violation
```

Agents:

```text
Orchestrator
Triage Agent
Schema Agent
Data Quality Agent
Problem Solver
Reviewer
Verification Agent
```

This is enough to demonstrate the complete lifecycle:

```text
Detect
→ Diagnose
→ Retrieve Knowledge
→ Propose
→ Validate
→ Review
→ Decide
→ Execute/Escalate
→ Verify
→ Learn
```

---

# 25. Project Goal

DE Swarm is not intended to blindly auto-fix every data pipeline incident.

The goal is to create a trustworthy AI-assisted on-call system where:

- diagnosis is evidence-based,
- remediation is constrained,
- production actions are policy-controlled,
- uncertain incidents are escalated,
- every decision is auditable,
- and automation is earned separately for each incident category.

That is the foundation for safely applying multi-agent AI to data-platform operations.
