# Running Workflow Validations

- Version: 1.0
- Contact: Alexis Lucattini, [alexisl@unimelb.edu.au](mailto:alexisl@unimelb.edu.au)

This SOP describes how to run workflow validations for the Oncoanalyser WGTS Both pipeline.

- [Introduction](#introduction)
- [Requirements](#requirements)
- [Procedure](#procedure)
- [Expected Outputs](#expected-outputs)
- [Validation Criteria](#validation-criteria)


## Introduction

When deploying a new version of the Oncoanalyser WGTS Both pipeline (new workflow version or parameter changes),
validation runs should be performed against known test datasets to confirm expected behaviour.

This pipeline requires outputs from both the Oncoanalyser WGTS DNA and Oncoanalyser WGTS RNA upstream pipelines.
Ensure both upstream workflows have completed successfully before initiating a validation run.

## Requirements

- AWS credentials for the beta/gamma environment
- Access to the OrcaBus Portal
- A known test dataset with expected outcomes (e.g. SEQC-II HCC1395 tumor DNA/normal DNA/tumor RNA triple)
- Both upstream pipelines must have SUCCEEDED:
  - Oncoanalyser WGTS DNA (provides DNA somatic analysis outputs)
  - Oncoanalyser WGTS RNA (provides RNA expression/fusion outputs)
- The pipeline version to validate has been deployed (see [PM.OWB.2][sop_2_rel_path])

## Procedure

1. **Identify test libraries** — Use the standard validation libraries (e.g. L2300943 tumor DNA / L2300950 normal DNA / L2300951 tumor RNA for SEQC-II).

2. **Confirm upstream completion** — Verify that both the Oncoanalyser WGTS DNA and Oncoanalyser WGTS RNA workflow runs have reached SUCCEEDED status for the test libraries in the OrcaBus Portal.

3. **Submit a validation DRAFT event** — Follow [PM.OWB.1][sop_1_rel_path] to submit a DRAFT event targeting the new pipeline version:
   ```json5
   {
     "payload": {
       "version": "<PAYLOAD_VERSION>",
       "data": {
         "engineParameters": {
           "pipelineId": "<NEW_PIPELINE_ID>"
         }
       }
     }
   }
   ```

4. **Monitor execution** — Track the workflow run through the OrcaBus Portal or AWS Step Functions console. Ensure it transitions through DRAFT → READY → SUBMITTED → SUCCEEDED.

5. **Compare outputs** — Compare the analysis outputs against the expected reference outputs for the test dataset.

## Expected Outputs

The Oncoanalyser WGTS Both pipeline produces integrated DNA+RNA reporting:
- PURPLE results with RNA expression support (purity, ploidy, copy number, gene copy numbers)
- LINX annotations with RNA fusion evidence (structural variant clustering, gene fusions)
- CUPPA cancer-of-origin predictions (using both DNA and RNA features)
- ORANGE clinical report (comprehensive PDF report integrating all results)

## Validation Criteria

A validation run is considered successful when:
1. The workflow run reaches SUCCEEDED status without manual intervention.
2. All expected output files are present in the output URI.
3. Key metrics (purity estimates, fusion counts, CUPPA top predictions) are within acceptable ranges of the reference run.
4. No unexpected errors or warnings appear in the execution logs.

If validation fails, consult [PM.OWB.5 - Troubleshooting][sop_5_rel_path] for guidance.


[sop_1_rel_path]: ../PM.OWB.1/PM.OWB.1-ManualPipelineExecution.md
[sop_2_rel_path]: ../PM.OWB.2/PM.OWB.2-NewPipelineDeployment.md
[sop_5_rel_path]: ../PM.OWB.5/PM.OWB.5-TroubleShooting.md
