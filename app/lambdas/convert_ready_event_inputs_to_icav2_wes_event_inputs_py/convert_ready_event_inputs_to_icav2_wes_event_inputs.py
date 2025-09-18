#!/usr/bin/env python3

"""
Given inputs from a Ready Event, convert them to the format expected by the ICAV2 WES Event Inputs.

Workflow inputs:
  * groupId: tags.idvID
  * subjectId: tags.subjectId
  * tumorDnaSampleId:
  * normalDnaSampleId:
  * tumorRnaSampleId:
  * processesList:
    * lilac
    * neo
    * cuppa
    * orange
  * genome: "GRCh38_umccr"
  * genomeVersion: "GRCh38"
  * genomeType: "alt"
  * forceGenome: true
  * refDataHmfDataPath: "s3://hmf-data/GRCh38_umccr"
  * genomes
    * GRCh38_umccr:
      * fasta
      * fai
      * dict
      * img
      * bwamem2Index
      * gridssIndex
      * starIndex
  * tumorDnaInputs:
    * bamRedux
    * reduxJitterTsv
    * reduxMsTsv
    * bamtoolsDir
    * sageDir
    * linxAnnoDir
    * linxPlotDir
    * purpleDir
    * virusinterpreterDir
    * chordDir
    * sigsDir
  * normalDnaInputs:
    * bamRedux
    * reduxJitterTsv
    * reduxMsTsv
    * bamtoolsDir
    * sageDir
    * linxAnnoDir
  * tumorRnaInputs:
    * bam
    * isofoxDir


WES Input Payload:
{
    "mode": "wgts",
    "monochrome_logs": True,
    "processes_manual": True,
    "processes_include": "lilac,neo,cuppa,orange",
    "samplesheet": [
        {
            "group_id": "SBJ05828",
            "subject_id": "SBJ05828",
            "sample_id": "L2401541",
            "sample_type": "tumor",
            "sequence_type": "dna",
            "filetype": "bam_redux",
            "filepath": "s3://pipeline-dev-cache-503977275616-ap-southeast-2/byob-icav2/development/analysis/oncoanalyser-wgts-dna/202508052e398fe8/SBJ05828/alignments/dna/L2401541.redux.bam"
        },
        {
            "group_id": "SBJ05828",
            "subject_id": "SBJ05828",
            "sample_id": "L2401541",
            "sample_type": "tumor",
            "sequence_type": "dna",
            "filetype": "bai",
            "filepath": "s3://pipeline-dev-cache-503977275616-ap-southeast-2/byob-icav2/development/analysis/oncoanalyser-wgts-dna/202508052e398fe8/SBJ05828/alignments/dna/L2401541.redux.bam.bai"
        },
        {
            "group_id": "SBJ05828",
            "subject_id": "SBJ05828",
            "sample_id": "L2401541",
            "sample_type": "tumor",
            "sequence_type": "dna",
            "filetype": "redux_jitter_tsv",
            "filepath": "s3://pipeline-dev-cache-503977275616-ap-southeast-2/byob-icav2/development/analysis/oncoanalyser-wgts-dna/202508052e398fe8/SBJ05828/alignments/dna/L2401541.jitter_params.tsv"
        },
        {
            "group_id": "SBJ05828",
            "subject_id": "SBJ05828",
            "sample_id": "L2401541",
            "sample_type": "tumor",
            "sequence_type": "dna",
            "filetype": "redux_ms_tsv",
            "filepath": "s3://pipeline-dev-cache-503977275616-ap-southeast-2/byob-icav2/development/analysis/oncoanalyser-wgts-dna/202508052e398fe8/SBJ05828/alignments/dna/L2401541.ms_table.tsv.gz"
        },
        {
            "group_id": "SBJ05828",
            "subject_id": "SBJ05828",
            "sample_id": "L2401540",
            "sample_type": "normal",
            "sequence_type": "dna",
            "filetype": "bam_redux",
            "filepath": "s3://pipeline-dev-cache-503977275616-ap-southeast-2/byob-icav2/development/analysis/oncoanalyser-wgts-dna/202508052e398fe8/SBJ05828/alignments/dna/L2401540.redux.bam"
        },
        {
            "group_id": "SBJ05828",
            "subject_id": "SBJ05828",
            "sample_id": "L2401540",
            "sample_type": "normal",
            "sequence_type": "dna",
            "filetype": "bai",
            "filepath": "s3://pipeline-dev-cache-503977275616-ap-southeast-2/byob-icav2/development/analysis/oncoanalyser-wgts-dna/202508052e398fe8/SBJ05828/alignments/dna/L2401540.redux.bam.bai"
        },
        {
            "group_id": "SBJ05828",
            "subject_id": "SBJ05828",
            "sample_id": "L2401540",
            "sample_type": "normal",
            "sequence_type": "dna",
            "filetype": "redux_jitter_tsv",
            "filepath": "s3://pipeline-dev-cache-503977275616-ap-southeast-2/byob-icav2/development/analysis/oncoanalyser-wgts-dna/202508052e398fe8/SBJ05828/alignments/dna/L2401540.jitter_params.tsv"
        },
        {
            "group_id": "SBJ05828",
            "subject_id": "SBJ05828",
            "sample_id": "L2401540",
            "sample_type": "normal",
            "sequence_type": "dna",
            "filetype": "redux_ms_tsv",
            "filepath": "s3://pipeline-dev-cache-503977275616-ap-southeast-2/byob-icav2/development/analysis/oncoanalyser-wgts-dna/202508052e398fe8/SBJ05828/alignments/dna/L2401540.ms_table.tsv.gz"
        },
        {
            "group_id": "SBJ05828",
            "subject_id": "SBJ05828",
            "sample_id": "L2401533",
            "sample_type": "tumor",
            "sequence_type": "rna",
            "filetype": "bam",
            "filepath": "s3://pipeline-dev-cache-503977275616-ap-southeast-2/byob-icav2/development/analysis/oncoanalyser-wgts-rna/202508093e7596dc/SBJ00595/alignments/rna/L2401533.md.bam"
        },
        {
            "group_id": "SBJ05828",
            "subject_id": "SBJ05828",
            "sample_id": "L2401533",
            "sample_type": "tumor",
            "sequence_type": "rna",
            "filetype": "bai",
            "filepath": "s3://pipeline-dev-cache-503977275616-ap-southeast-2/byob-icav2/development/analysis/oncoanalyser-wgts-rna/202508093e7596dc/SBJ00595/alignments/rna/L2401533.md.bam.bai"
        },
        {
            "group_id": "SBJ05828",
            "subject_id": "SBJ05828",
            "sample_id": "L2401541",
            "sample_type": "tumor",
            "sequence_type": "dna",
            "filetype": "bamtools_dir",
            "filepath": "s3://pipeline-dev-cache-503977275616-ap-southeast-2/byob-icav2/development/analysis/oncoanalyser-wgts-dna/202508052e398fe8/SBJ05828/bamtools/SBJ05828_L2401541_bamtools/"
        },
        {
            "group_id": "SBJ05828",
            "subject_id": "SBJ05828",
            "sample_id": "L2401540",
            "sample_type": "normal",
            "sequence_type": "dna",
            "filetype": "bamtools_dir",
            "filepath": "s3://pipeline-dev-cache-503977275616-ap-southeast-2/byob-icav2/development/analysis/oncoanalyser-wgts-dna/202508052e398fe8/SBJ05828/bamtools/SBJ05828_L2401540_bamtools/"
        },
        {
            "group_id": "SBJ05828",
            "subject_id": "SBJ05828",
            "sample_id": "L2401541",
            "sample_type": "tumor",
            "sequence_type": "dna",
            "filetype": "sage_dir",
            "filepath": "s3://pipeline-dev-cache-503977275616-ap-southeast-2/byob-icav2/development/analysis/oncoanalyser-wgts-dna/202508052e398fe8/SBJ05828/sage/somatic/"
        },
        {
            "group_id": "SBJ05828",
            "subject_id": "SBJ05828",
            "sample_id": "L2401540",
            "sample_type": "normal",
            "sequence_type": "dna",
            "filetype": "sage_dir",
            "filepath": "s3://pipeline-dev-cache-503977275616-ap-southeast-2/byob-icav2/development/analysis/oncoanalyser-wgts-dna/202508052e398fe8/SBJ05828/sage/germline/"
        },
        {
            "group_id": "SBJ05828",
            "subject_id": "SBJ05828",
            "sample_id": "L2401541",
            "sample_type": "tumor",
            "sequence_type": "dna",
            "filetype": "linx_anno_dir",
            "filepath": "s3://pipeline-dev-cache-503977275616-ap-southeast-2/byob-icav2/development/analysis/oncoanalyser-wgts-dna/202508052e398fe8/SBJ05828/linx/somatic_annotations/"
        }, {
            "group_id": "SBJ05828",
            "subject_id": "SBJ05828",
            "sample_id": "L2401541",
            "sample_type": "tumor",
            "sequence_type": "dna",
            "filetype": "linx_plot_dir",
            "filepath": "s3://pipeline-dev-cache-503977275616-ap-southeast-2/byob-icav2/development/analysis/oncoanalyser-wgts-dna/202508052e398fe8/SBJ05828/linx/somatic_plots/"
        },
        {
            "group_id": "SBJ05828",
            "subject_id": "SBJ05828",
            "sample_id": "L2401540",
            "sample_type": "normal",
            "sequence_type": "dna",
            "filetype": "linx_anno_dir",
            "filepath": "s3://pipeline-dev-cache-503977275616-ap-southeast-2/byob-icav2/development/analysis/oncoanalyser-wgts-dna/202508052e398fe8/SBJ05828/linx/germline_annotations/"
        },
        {
            "group_id": "SBJ05828",
            "subject_id": "SBJ05828",
            "sample_id": "L2401541",
            "sample_type": "tumor",
            "sequence_type": "dna",
            "filetype": "purple_dir",
            "filepath": "s3://pipeline-dev-cache-503977275616-ap-southeast-2/byob-icav2/development/analysis/oncoanalyser-wgts-dna/202508052e398fe8/SBJ05828/purple/"
        },
        {
            "group_id": "SBJ05828",
            "subject_id": "SBJ05828",
            "sample_id": "L2401541",
            "sample_type": "tumor",
            "sequence_type": "dna",
            "filetype": "virusinterpreter_dir",
            "filepath": "s3://pipeline-dev-cache-503977275616-ap-southeast-2/byob-icav2/development/analysis/oncoanalyser-wgts-dna/202508052e398fe8/SBJ05828/virusinterpreter/"
        },
        {
            "group_id": "SBJ05828",
            "subject_id": "SBJ05828",
            "sample_id": "L2401541",
            "sample_type": "tumor",
            "sequence_type": "dna",
            "filetype": "chord_dir",
            "filepath": "s3://pipeline-dev-cache-503977275616-ap-southeast-2/byob-icav2/development/analysis/oncoanalyser-wgts-dna/202508052e398fe8/SBJ05828/chord/"
        },
        {
            "group_id": "SBJ05828",
            "subject_id": "SBJ05828",
            "sample_id": "L2401541",
            "sample_type": "tumor",
            "sequence_type": "dna",
            "filetype": "sigs_dir",
            "filepath": "s3://pipeline-dev-cache-503977275616-ap-southeast-2/byob-icav2/development/analysis/oncoanalyser-wgts-dna/202508052e398fe8/SBJ05828/sigs/"
        },
        {
            "group_id": "SBJ05828",
            "subject_id": "SBJ05828",
            "sample_id": "L2401533",
            "sample_type": "tumor",
            "sequence_type": "rna",
            "filetype": "isofox_dir",
            "filepath": "s3://pipeline-dev-cache-503977275616-ap-southeast-2/byob-icav2/development/analysis/oncoanalyser-wgts-rna/202508093e7596dc/SBJ00595/isofox/"
        }
    ],
    "genome": "GRCh38_umccr",
    "genome_version": "38",
    "genome_type": "alt",
    "force_genome": True,
    "ref_data_hmf_data_path": "s3://pipeline-prod-cache-503977275616-ap-southeast-2/byob-icav2/reference-data/hartwig/hmf-reference-data/hmftools/hmf_pipeline_resources.38_v2.1.0--1/",
    "genomes": {
        "GRCh38_umccr": {
            "fasta": "s3://pipeline-prod-cache-503977275616-ap-southeast-2/byob-icav2/reference-data/genomes/GRCh38_umccr/GRCh38_full_analysis_set_plus_decoy_hla.fa",
            "fai": "s3://pipeline-prod-cache-503977275616-ap-southeast-2/byob-icav2/reference-data/genomes/GRCh38_umccr/samtools_index/1.16/GRCh38_full_analysis_set_plus_decoy_hla.fa.fai",
            "dict": "s3://pipeline-prod-cache-503977275616-ap-southeast-2/byob-icav2/reference-data/genomes/GRCh38_umccr/samtools_index/1.16/GRCh38_full_analysis_set_plus_decoy_hla.fa.dict",
            "img": "s3://pipeline-prod-cache-503977275616-ap-southeast-2/byob-icav2/reference-data/genomes/GRCh38_umccr/bwa_index_image/0.7.17-r1188/GRCh38_full_analysis_set_plus_decoy_hla.fa.img",
            "bwamem2_index": "s3://pipeline-prod-cache-503977275616-ap-southeast-2/byob-icav2/reference-data/genomes/GRCh38_umccr/bwa-mem2_index/2.2.1/",
            "gridss_index": "s3://pipeline-prod-cache-503977275616-ap-southeast-2/byob-icav2/reference-data/genomes/GRCh38_umccr/gridss_index/2.13.2/",
            "star_index": "s3://pipeline-prod-cache-503977275616-ap-southeast-2/byob-icav2/reference-data/genomes/GRCh38_umccr/star_index/gencode_38/2.7.3a/"
        }
    }
}
"""

from typing import Dict, List

# Globals
DEFAULT_MODE = "wgts"
DEFAULT_MONOCHROME_LOGS = True
DEFAULT_GENOME = "GRCh38_hmf"
DEFAULT_GENOME_VERSION = "38"
DEFAULT_GENOME_TYPE = "no_alt"

TUMOR_PHENOTYPE = "tumor"
NORMAL_PHENOTYPE = "normal"
DNA_SAMPLE_TYPE = "dna"
RNA_SAMPLE_TYPE = "rna"

# Default columns for the samplesheet
DEFAULT_SAMPLESHEET_COLUMNS = [
    "group_id",
    "subject_id",
    "sample_id",
    "sample_type",
    "sequence_type",
    "filetype",
    "filepath",
]

TUMOR_DNA_INPUT_PARAMS = [
    "bamRedux"
    "reduxJitterTsv"
    "reduxMsTsv"
    "bamtoolsDir"
    "sageDir"
    "linxAnnoDir"
    "linxPlotDir"
    "purpleDir"
    "virusinterpreterDir"
    "chordDir"
    "sigsDir"
]

NORMAL_DNA_INPUT_PARAMS = [
    "bamRedux",
    "reduxJitterTsv",
    "reduxMsTsv",
    "bamtoolsDir",
    "sageDir",
    "linxAnnoDir",
]

TUMOR_RNA_INPUT_PARAMS = [
    "bam",
    "isofoxDir",
]


def convert_params_to_samplesheet_row(
        group_id: str,
        subject_id: str,
        sample_id: str,
        sample_type: str,
        sequence_type: str,
        filetype: str,
        filepath: str
) -> Dict[str, str]:
    """
    Convert parameters to a samplesheet row.
    """
    return {
        "group_id": group_id,
        "subject_id": subject_id,
        "sample_id": sample_id,
        "sample_type": sample_type,
        "sequence_type": sequence_type,
        "filetype": camel_case_to_snake_case(filetype),
        "filepath": filepath
    }


def map_tumor_dna_inputs_to_samplesheet_rows(
        group_id: str,
        subject_id: str,
        tumor_dna_sample_id: str,
        tumor_dna_inputs_dict: Dict[str, str]
) -> List[Dict[str, str]]:
    """
    Map tumor DNA inputs to samplesheet rows.
    :param group_id: Group ID.
    :param subject_id: Subject ID.
    :param tumor_dna_sample_id: Sample ID for tumor DNA.
    :param tumor_dna_inputs_dict: Dictionary containing tumor DNA inputs.
    :return: List of samplesheet rows.
    """
    return list(map(
        lambda key_value_iter: convert_params_to_samplesheet_row(
            group_id=group_id,
            subject_id=subject_id,
            sample_id=tumor_dna_sample_id,
            sample_type=TUMOR_PHENOTYPE,
            sequence_type=DNA_SAMPLE_TYPE,
            filetype=camel_case_to_snake_case(key_value_iter[0]),
            filepath=key_value_iter[1]
        ),
        tumor_dna_inputs_dict.items()
    ))


def map_normal_dna_inputs_to_samplesheet_rows(
        group_id: str,
        subject_id: str,
        normal_dna_sample_id: str,
        normal_dna_inputs_dict: Dict[str, str]
)-> List[Dict[str, str]]:
    """
    Map normal DNA inputs to samplesheet rows.
    :param group_id: Group ID.
    :param subject_id: Subject ID.
    :param normal_dna_sample_id: Sample ID for normal DNA.
    :param normal_dna_inputs_dict: Dictionary containing normal DNA inputs.
    :return: List of samplesheet rows.
    """
    return list(map(
        lambda key_value_iter: convert_params_to_samplesheet_row(
            group_id=group_id,
            subject_id=subject_id,
            sample_id=normal_dna_sample_id,
            sample_type=NORMAL_PHENOTYPE,
            sequence_type=DNA_SAMPLE_TYPE,
            filetype=camel_case_to_snake_case(key_value_iter[0]),
            filepath=key_value_iter[1]
        ),
        normal_dna_inputs_dict.items()
    ))

def map_tumor_rna_inputs_to_samplesheet_rows(
        group_id: str,
        subject_id: str,
        tumor_rna_sample_id: str,
        tumor_rna_inputs_dict: Dict[str, str]
)-> List[Dict[str, str]]:
    """
    Map normal DNA inputs to samplesheet rows.
    :param group_id: Group ID.
    :param subject_id: Subject ID.
    :param tumor_rna_sample_id: Sample ID for normal DNA.
    :param tumor_rna_inputs_dict: Dictionary containing normal DNA inputs.
    :return: List of samplesheet rows.
    """
    return list(map(
        lambda key_value_iter: convert_params_to_samplesheet_row(
            group_id=group_id,
            subject_id=subject_id,
            sample_id=tumor_rna_sample_id,
            sample_type=TUMOR_PHENOTYPE,
            sequence_type=RNA_SAMPLE_TYPE,
            filetype=camel_case_to_snake_case(key_value_iter[0]),
            filepath=key_value_iter[1]
        ),
        tumor_rna_inputs_dict.items()
    ))


def get_bai_from_bam_row(bam_row: Dict[str, str]) -> Dict[str, str]:
    bai_row = bam_row.copy()

    bai_row['filetype'] = 'bai'
    bai_row['filepath'] = f"{bam_row['filepath']}.bai"

    return bai_row


def genome_keys_to_snake_case(genome: Dict[str, str]) -> Dict[str, str]:
    """
    Input genome keys are in camelCase, this function converts them to snake_case.
    :param genome:
    :return:
    """
    return dict(map(
        lambda kv_iter_: (kv_iter_[0].replace("Index", "_index").lower(), kv_iter_[1]),
        genome.items()
    ))

def camel_case_to_snake_case(name: str) -> str:
    """Convert camelCase to snake_case."""
    return ''.join(['_' + i.lower() if i.isupper() else i for i in name]).lstrip('_')


def convert_ready_event_inputs_to_icav2_wes_event_inputs(
        inputs: Dict[str, any]
) -> Dict[str, any]:
    """
    Convert the ready event inputs to ICAv2 WES event inputs.
    """
    samplesheet = (
        map_tumor_dna_inputs_to_samplesheet_rows(
            group_id=inputs["groupId"],
            subject_id=inputs["subjectId"],
            tumor_dna_sample_id=inputs["tumorDnaSampleId"],
            tumor_dna_inputs_dict=inputs.get("tumorDnaInputs", {})
        ) +
        map_normal_dna_inputs_to_samplesheet_rows(
            group_id=inputs["groupId"],
            subject_id=inputs["subjectId"],
            normal_dna_sample_id=inputs["normalDnaSampleId"],
            normal_dna_inputs_dict=inputs.get("normalDnaInputs", {})
        ) +
        map_tumor_rna_inputs_to_samplesheet_rows(
            group_id=inputs["groupId"],
            subject_id=inputs["subjectId"],
            tumor_rna_sample_id=inputs["tumorRnaSampleId"],
            tumor_rna_inputs_dict=inputs.get("tumorRnaInputs", {})
        )
    )

    # Extend samplesheet with bam indexes for bam_redux and bam filetypes
    samplesheet_bams = list(filter(
        lambda row: row["filetype"] in ["bam_redux", "bam"],
        samplesheet
    ))

    # Add bai files for each bam file
    samplesheet.extend(
        list(map(
            lambda row: get_bai_from_bam_row(row),
            samplesheet_bams
        ))
    )

    # Return the dictionary of inputs
    return dict(filter(
        lambda kv_iter_: kv_iter_[1] is not None,
        {
            "mode": inputs.get("mode", DEFAULT_MODE),
            "monochrome_logs": inputs.get("monochromeLogs", DEFAULT_MONOCHROME_LOGS),
            "samplesheet": samplesheet,
            "genome": inputs.get("genome", DEFAULT_GENOME),
            "genome_version": inputs.get("genomeVersion", DEFAULT_GENOME_VERSION),
            "genome_type": inputs.get("genomeType", DEFAULT_GENOME_TYPE),
            "force_genome": inputs.get("forceGenome", None),
            "ref_data_hmf_data_path": inputs["refDataHmfDataPath"],
            "genomes": (
                dict(map(
                    lambda kv_iter_: (kv_iter_[0], genome_keys_to_snake_case(kv_iter_[1])),
                    inputs.get("genomes").items()
                ))
                if inputs.get("genomes") is not None
                else None
            ),
        }.items()
    ))


def handler(event, context):
    """
    Convert the ready event inputs to ICAv2 WES event inputs.
    :param event:
    :param context:
    :return:
    """

    return {
        "inputs": convert_ready_event_inputs_to_icav2_wes_event_inputs(
            event['inputs']
        )
    }
