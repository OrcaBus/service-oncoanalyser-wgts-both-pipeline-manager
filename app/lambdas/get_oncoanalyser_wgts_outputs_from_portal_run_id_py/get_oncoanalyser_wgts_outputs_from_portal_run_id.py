#!/usr/bin/env python3

"""
1 Get the latest succeeded workflow for a given library id
2 Get the BAM file from that workflow
"""

# Standard imports
from typing import Optional, Literal, List, TypedDict, Dict, cast, Union
from pathlib import Path
from urllib.parse import urlparse, urlunparse

# Layer imports
from orcabus_api_tools.filemanager import list_files_from_portal_run_id

# Globals
ONCOANALYSER_WGTS_DNA_WORKFLOW_RUN_NAME = "oncoanalyser-wgts-dna"
ONCOANALYSER_WGTS_RNA_WORKFLOW_RUN_NAME = "oncoanalyser-wgts-rna"
Phenotype = Literal["TUMOR", "NORMAL"]
SampleType = Literal["DNA", "RNA"]
PHENOTYPE_LIST: List[Phenotype] = ["TUMOR", "NORMAL"]
SAMPLE_LIST: List[SampleType] = ["DNA", "RNA"]

# RELATIVE OUTPUTS
DNA_MIDFIX = "{TUMOR_DNA_LIBRARY_ID}__{NORMAL_DNA_LIBRARY_ID}"
RNA_MIDFIX = "{TUMOR_RNA_LIBRARY_ID}"


class TumorDnaInputs(TypedDict):
    bamRedux: str
    reduxJitterTsv: str
    reduxMsTsv: str
    bamtoolsDir: str
    sageDir: str
    linxAnnoDir: str
    linxPlotDir: str
    purpleDir: str
    virusinterpreterDir: str
    chordDir: str
    sigsDir: str


class NormalDnaInputs(TypedDict):
    bamRedux: str
    reduxJitterTsv: str
    reduxMsTsv: str
    bamtoolsDir: str
    sageDir: str
    linxAnnoDir: str


class TumorRnaInputs(TypedDict):
    bam: str
    isofoxDir: str


TUMOR_DNA: TumorDnaInputs = {
    "bamRedux": f"{DNA_MIDFIX}/alignments/dna/{{TUMOR_DNA_LIBRARY_ID}}.redux.bam",
    "reduxJitterTsv": f"{DNA_MIDFIX}/alignments/dna/{{TUMOR_DNA_LIBRARY_ID}}.jitter_params.tsv",
    "reduxMsTsv": f"{DNA_MIDFIX}/alignments/dna/{{TUMOR_DNA_LIBRARY_ID}}.ms_table.tsv.gz",
    "bamtoolsDir": f"{DNA_MIDFIX}/bamtools/{{TUMOR_DNA_LIBRARY_ID}}__{{NORMAL_DNA_LIBRARY_ID}}_{{TUMOR_DNA_LIBRARY_ID}}_bamtools/",
    "sageDir": f"{DNA_MIDFIX}/sage_calling/somatic/",
    "linxAnnoDir": f"{DNA_MIDFIX}/linx/somatic_annotations/",
    "linxPlotDir": f"{DNA_MIDFIX}/linx/somatic_plots/",
    "purpleDir": f"{DNA_MIDFIX}/purple/",
    "virusinterpreterDir": f"{DNA_MIDFIX}/virusinterpreter/",
    "chordDir": f"{DNA_MIDFIX}/chord/",
    "sigsDir": f"{DNA_MIDFIX}/sigs/",
}

NORMAL_DNA: NormalDnaInputs = {
    "bamRedux": f"{DNA_MIDFIX}/alignments/dna/{{NORMAL_DNA_LIBRARY_ID}}.redux.bam",
    "reduxJitterTsv": f"{DNA_MIDFIX}/alignments/dna/{{NORMAL_DNA_LIBRARY_ID}}.jitter_params.tsv",
    "reduxMsTsv": f"{DNA_MIDFIX}/alignments/dna/{{NORMAL_DNA_LIBRARY_ID}}.ms_table.tsv.gz",
    "bamtoolsDir": f"{DNA_MIDFIX}/bamtools/{{TUMOR_DNA_LIBRARY_ID}}__{{NORMAL_DNA_LIBRARY_ID}}_{{NORMAL_DNA_LIBRARY_ID}}_bamtools/",
    "sageDir": f"{DNA_MIDFIX}/sage_calling/germline/",
    "linxAnnoDir": f"{DNA_MIDFIX}/linx/germline_annotations/",
}

TUMOR_RNA: TumorRnaInputs = {
    "bam": f"{RNA_MIDFIX}/alignments/rna/{{TUMOR_RNA_LIBRARY_ID}}.md.bam",
    "isofoxDir": f"{RNA_MIDFIX}/isofox/",
}


def extend_s3_uri_path(analysis_root_prefix: str, path: str) -> str:
    s3_obj = urlparse(analysis_root_prefix)

    return str(urlunparse((
        s3_obj.scheme, s3_obj.netloc,
        str(Path(s3_obj.path) / path) + ("/" if path.endswith("/") else ""),
        None, None, None
    )))


def get_path_prefix_from_path_key(
        object_: Union[TUMOR_DNA, NORMAL_DNA, TUMOR_RNA],
        key: str,
        tumor_dna_library_id: Optional[str] = None,
        normal_dna_library_id: Optional[str] = None,
        tumor_rna_library_id: Optional[str] = None,
) -> str:
    return str(
        object_[key].format(
            **dict(filter(
                lambda kv_iter: kv_iter[1] is not None,
                {
                    "TUMOR_DNA_LIBRARY_ID": tumor_dna_library_id,
                    "NORMAL_DNA_LIBRARY_ID": normal_dna_library_id,
                    "TUMOR_RNA_LIBRARY_ID": tumor_rna_library_id,
                    "DNA_MIDFIX": f"{tumor_dna_library_id}__{normal_dna_library_id}",
                    "RNA_MIDFIX": f"{tumor_rna_library_id}",
                }.items()
            ))
        )
    )


def get_tumor_dna_inputs(
        portal_run_id_analysis_root_prefix: str,
        tumor_dna_library_id: str,
        normal_dna_library_id: str,
) -> TumorDnaInputs:
    return cast(
        TumorDnaInputs,
        dict(map(
            lambda kv_iter_: (
                kv_iter_[0],
                extend_s3_uri_path(
                    portal_run_id_analysis_root_prefix,
                    get_path_prefix_from_path_key(
                        object_=TUMOR_DNA,
                        key=kv_iter_[0],
                        tumor_dna_library_id=tumor_dna_library_id,
                        normal_dna_library_id=normal_dna_library_id,
                    )
                )
            ),
            TUMOR_DNA.copy().items()
        ))
    )


def get_normal_dna_inputs(
        portal_run_id_analysis_root_prefix: str,
        tumor_dna_library_id: str,
        normal_dna_library_id: str,
) -> TumorDnaInputs:
    return cast(
        TumorDnaInputs,
        dict(map(
            lambda kv_iter_: (
                kv_iter_[0],
                extend_s3_uri_path(
                    portal_run_id_analysis_root_prefix,
                    get_path_prefix_from_path_key(
                        object_=NORMAL_DNA,
                        key=kv_iter_[0],
                        tumor_dna_library_id=tumor_dna_library_id,
                        normal_dna_library_id=normal_dna_library_id,
                    )
                )
            ),
            NORMAL_DNA.copy().items()
        ))
    )


def get_tumor_rna_inputs(
        portal_run_id_analysis_root_prefix: str,
        tumor_rna_library_id: str,
) -> TumorRnaInputs:
    return cast(
        TumorRnaInputs,
        dict(map(
            lambda kv_iter_: (
                kv_iter_[0],
                extend_s3_uri_path(
                    portal_run_id_analysis_root_prefix,
                    get_path_prefix_from_path_key(
                        object_=TUMOR_RNA,
                        key=kv_iter_[0],
                        tumor_rna_library_id=tumor_rna_library_id,
                    )
                )
            ),
            TUMOR_RNA.copy().items()
        ))
    )


def get_portal_run_id_root_prefix(portal_run_id: str) -> str:
    # Get portal run id midfix from portal_run_id
    all_portal_run_id_files = list_files_from_portal_run_id(
        portal_run_id
    )

    if len(all_portal_run_id_files) == 0:
        raise ValueError(f"No files found for portal run id {portal_run_id}")
    portal_run_id_analysis_file = all_portal_run_id_files[0]

    # Get root for the portal run id
    parts_list = []
    for idx, part in enumerate(Path(portal_run_id_analysis_file['key']).parts):
        if part == portal_run_id:
            parts_list.append(part)
            break
        else:
            parts_list.append(part)
    return str(urlunparse((
        "s3", portal_run_id_analysis_file['bucket'], str("/".join(parts_list)), None, None, None
    )))


def get_inputs(
        portal_run_id: str,
        phenotype: Phenotype,
        sample_type: SampleType,
        tumor_dna_library_id: Optional[str] = None,
        normal_dna_library_id: Optional[str] = None,
        tumor_rna_library_id: Optional[str] = None,
) -> Dict[str, Union[TumorDnaInputs, NormalDnaInputs, TumorRnaInputs]]:

    # Portal run id prefix
    portal_run_id_analysis_root_prefix = get_portal_run_id_root_prefix(portal_run_id)

    # DNA TUMOR
    if sample_type == "DNA" and phenotype == "TUMOR":
        if tumor_dna_library_id is None or normal_dna_library_id is None:
            raise ValueError(
                "Both tumor_dna_library_id and normal_dna_library_id must be provided for DNA TUMOR sample type")
        return {
            "tumorDnaInputs": get_tumor_dna_inputs(
                portal_run_id_analysis_root_prefix=portal_run_id_analysis_root_prefix,
                tumor_dna_library_id=tumor_dna_library_id,
                normal_dna_library_id=normal_dna_library_id,
            )
        }
    # DNA NORMAL
    elif sample_type == "DNA" and phenotype == "NORMAL":
        if tumor_dna_library_id is None or normal_dna_library_id is None:
            raise ValueError(
                "Both tumor_dna_library_id and normal_dna_library_id must be provided for DNA NORMAL sample type")
        return {
            "normalDnaInputs": get_normal_dna_inputs(
                portal_run_id_analysis_root_prefix=portal_run_id_analysis_root_prefix,
                tumor_dna_library_id=tumor_dna_library_id,
                normal_dna_library_id=normal_dna_library_id,
            )
        }
    # RNA TUMOR
    elif sample_type == "RNA" and phenotype == "TUMOR":
        if tumor_rna_library_id is None:
            raise ValueError("tumor_rna_library_id must be provided for RNA TUMOR sample type")
        return {
            "tumorRnaInputs": get_tumor_rna_inputs(
                portal_run_id_analysis_root_prefix=portal_run_id_analysis_root_prefix,
                tumor_rna_library_id=tumor_rna_library_id,
            )
        }
    raise ValueError("Invalid combination of sample_type and phenotype")


def handler(event, context):
    """
    Given a normal and tumor library id, get the latest dragen workflow and return the bam files
    :param event:
    :param context:
    :return:
    """

    # Get the library ids from the event
    portal_run_id = event.get('portalRunId', None)
    phenotype: Phenotype = event.get('phenotype', None)
    sample_type: SampleType = event.get('sampleType', None)
    tumor_dna_library_id: Optional[str] = event.get('tumorDnaLibraryId', None)
    normal_dna_library_id: Optional[str] = event.get('normalDnaLibraryId', None)
    tumor_rna_library_id: Optional[str] = event.get('tumorRnaLibraryId', None)

    return get_inputs(
        portal_run_id=portal_run_id,
        phenotype=phenotype,
        sample_type=sample_type,
        tumor_dna_library_id=tumor_dna_library_id,
        normal_dna_library_id=normal_dna_library_id,
        tumor_rna_library_id=tumor_rna_library_id,
    )
