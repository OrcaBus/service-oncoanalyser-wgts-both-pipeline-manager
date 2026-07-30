# New Oncoanalyser WGTS Both Pipeline Deployment

- Version: 1.0
- Contact: Alexis Lucattini, [alexisl@unimelb.edu.au](mailto:alexisl@unimelb.edu.au)

There may be times where we need to deploy a new version of the Oncoanalyser WGTS Both pipeline.

In the SOP below we discuss the following scenarios:
* User wants to deploy a new version of the pipeline for testing purposes.
* User wants to make a new release of the pipeline for production use.

Throughout the SOP we make the following expectations:
* User is familiar with UMCCR's [cwl-ica repository][cwl_ica_repo] and has a working knowledge of CWL/Nextflow.
* User has access to the ICAv2 platform with at minimum 'Contributor level' permissions in at least one project.
* User has access to the appropriate AWS Account tied to the ICAv2 project.

- [Pipeline Summary](#pipeline-summary)
- [Setup](#setup)
- [Development Deployment](#development-deployment)
  - [Pipeline Creation](#pipeline-creation)
  - [Running the Pipeline](#running-the-pipeline)
- [Production Deployment](#production-deployment)
  - [GitHub Releases](#github-releases)
  - [Infrastructure Constants Updates](#infrastructure-constants-updates)
  - [Workflow Manager Updates](#workflow-manager-updates)
  - [Analysis Glue Updates](#analysis-glue-updates)


## Pipeline Summary

The Oncoanalyser WGTS Both pipeline runs on ICAv2 using CWL/Nextflow. It performs combined somatic DNA+RNA analysis including:
- Joint variant interpretation (PURPLE with RNA support)
- Fusion prioritisation (LINX with RNA evidence)
- Cancer-of-origin prediction (CUPPA)
- Comprehensive clinical reporting (ORANGE)

This pipeline integrates results from both upstream Oncoanalyser WGTS DNA and Oncoanalyser WGTS RNA workflows. It requires the analysis outputs from both pipelines as inputs.

## Setup

Ensure you have:
- ICAv2 CLI installed and configured
- AWS credentials for the target environment
- Access to the OrcaBus Portal

## Development Deployment

### Pipeline Creation

1. Package the workflow into a ZIP file for deployment into ICA.
2. Deploy into the development ICAv2 project:
   ```shell
   icav2 projects enter development
   icav2 projectpipelines create-cwl-pipeline-from-zip <workflow-zip>
   ```
3. Keep note of the pipeline ID.

### Running the Pipeline

Run the pipeline on a test dataset using [SOP 1][sop_1_rel_path], providing the new `pipelineId` in the engine parameters:

```json5
{
  "payload": {
    "version": "<DEFAULT_PAYLOAD_VERSION>",
    "data": {
      "engineParameters": {
        "pipelineId": "<THE PIPELINE ID YOU JUST CREATED>"
      }
    }
  }
}
```

Note: Both upstream pipelines (Oncoanalyser WGTS DNA and Oncoanalyser WGTS RNA) must have completed successfully before the DRAFT event can be fully populated and promoted to READY.

## Production Deployment

### GitHub Releases

1. Push CWL changes to a branch, get reviewed and merged to main.
2. Create a new workflow release via the cwl-ica CLI.

### Infrastructure Constants Updates

Update `infrastructure/stage/constants.ts` to include the new pipeline ID in `WORKFLOW_VERSION_TO_DEFAULT_ICAV2_PIPELINE_ID_MAP`.

### Workflow Manager Updates

Register the new workflow version with the Workflow Manager:

```shell
make-new-workflow.sh \
  --workflow-name 'oncoanalyser-wgts-dna-rna' \
  --workflow-version "<version>" \
  --executionEngine "ICA" \
  --executionEnginePipelineId "<pipeline-id>" \
  --codeVersion "$(cd <cwl-ica-repo> && git rev-parse --short=7 HEAD)" \
  --validationState "VALIDATED"
```

### Analysis Glue Updates

Update the [analysis-glue repository][analysis_glue_repo_link] constants to include the new workflow version.


[cwl_ica_repo]: https://github.com/umccr/cwl-ica
[sop_1_rel_path]: ../PM.OWB.1/PM.OWB.1-ManualPipelineExecution.md
[analysis_glue_repo_link]: https://github.com/OrcaBus/service-analysis-glue
