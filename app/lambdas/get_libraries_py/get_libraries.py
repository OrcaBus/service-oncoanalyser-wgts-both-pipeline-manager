#!/usr/bin/env python3

"""
Get the libraries from the input, check their metadata,
"""
# Standard imports
from typing import List, Literal

# Layer imports
from orcabus_api_tools.metadata import get_library_from_library_orcabus_id
from orcabus_api_tools.metadata.models import LibraryBase

Phenotype = Literal['tumor', 'normal']
SampleType = Literal['WGS', 'WTS']

def handler(event, context):
    """
    Get the libraries from the input, check their metadata,
    :param event:
    :param context:
    :return:
    """
    libraries: List[LibraryBase] = event.get("libraries", [])
    if not libraries:
        raise ValueError("No libraries provided in the input")

    # Get library metadata for both libraries
    library_obj_list = list(map(
        lambda library_iter_: get_library_from_library_orcabus_id(library_iter_['orcabusId']),
        libraries
    ))

    tumor_dna_library_obj = next(filter(
        lambda library_iter_: (
                library_iter_['phenotype'] == 'tumor' and
                library_iter_['type'] == 'WGS'
        ),
        library_obj_list
    ))

    normal_dna_library_obj = next(filter(
        lambda library_iter_: (
                library_iter_['phenotype'] == 'normal' and
                library_iter_['type'] == 'WGS'
        ),
        library_obj_list
    ))

    tumor_rna_library_obj = next(filter(
        lambda library_iter_: (
                library_iter_['phenotype'] == 'tumor' and
                library_iter_['type'] == 'WTS'
        ),
        library_obj_list
    ))

    # If both libraries are provided, return their IDs
    return {
        "tumorDnaLibraryId": tumor_dna_library_obj['libraryId'],
        "normalDnaLibraryId": normal_dna_library_obj['libraryId'],
        "tumorRnaLibraryId": tumor_rna_library_obj['libraryId']
    }
