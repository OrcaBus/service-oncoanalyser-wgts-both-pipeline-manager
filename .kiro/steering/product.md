# Product: Oncoanalyser WGTS Both Pipeline Manager

## Summary

This is an OrcaBus microservice that manages the lifecycle of the **Oncoanalyser WGTS Both pipeline** — a combined somatic DNA+RNA analysis pipeline that integrates results from both DNA and RNA Oncoanalyser workflows, performing joint variant interpretation, fusion prioritisation, and comprehensive reporting using the Oncoanalyser/LINX/PURPLE/CUPPA/ORANGE toolchain on ICAv2.

The service handles orchestration on ICAv2 (Illumina Connected Analytics v2) via CWL workflows. It follows the standard ICAv2-centric Pipeline Architecture used across OrcaBus. This is a downstream service — it depends on the successful completion of both the Oncoanalyser WGTS DNA and Oncoanalyser WGTS RNA pipelines (via glue state machines) to obtain their analysis outputs as inputs.

## Core Responsibilities

- Accept `WorkflowRunStateChange` DRAFT events and validate/populate them into READY events
- Submit READY events to ICAv2 as `Icav2WesRequest` events via a Step Functions state machine
- Monitor ICAv2 analysis state changes and convert them to `WorkflowRunUpdate` events
- Validate draft schemas against a registered JSON schema before promotion
- React to upstream Oncoanalyser WGTS DNA and Oncoanalyser WGTS RNA SUCCEEDED events and update existing DRAFT runs with new upstream data (glue pattern)
- Perform post-schema validation of engine parameters and URI formats

## Event Flow

```
DRAFT event (WorkflowRunStateChange)
  → populate draft data (Step Functions)
  → validate draft schema
  → post-schema validation (engine params, URIs)
  → emit READY event
  → submit to ICAv2 WES
  → monitor ICAv2 state changes
  → emit WorkflowRunUpdate events

Upstream SUCCEEDED event (oncoanalyser-wgts-dna OR oncoanalyser-wgts-rna)
  → glue state machine
  → find matching DRAFT runs
  → merge upstream outputs into DRAFT payload
  → emit WorkflowRunUpdate DRAFT event (if changed)
```

## Upstream / Downstream

- **Upstream**: Oncoanalyser WGTS DNA, Oncoanalyser WGTS RNA (provides analysis outputs via glue state machines)
- **Downstream**: Sash, RNAsum
- **Key dependencies**: ICAv2 WES Manager, Workflow Manager

## Environments

Deploys to `beta`, `gamma`, and `prod` via AWS CodePipeline. The toolchain account hosts the CodePipeline; application stacks deploy cross-account.
