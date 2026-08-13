import { PythonUvFunction } from '@orcabus/platform-cdk-constructs/lambda';

export type LambdaName =
  // Shared pre-ready lambdas
  | 'comparePayload'
  | 'getDraftPayload'
  | 'findLatestWorkflow'
  | 'getOncoanalyserWgtsOutputsFromPortalRunId'
  | 'getWorkflowRunObject'
  | 'generateWruEventObjectWithMergedData'
  | 'getMissingSchemaFields'
  | 'getLatestPayloadFromPortalRunId'
  // Glue lambdas
  // Draft Builder lambdas
  | 'getFastqIdListFromRgidList'
  | 'getFastqRgidsFromLibraryId'
  | 'getLibraries'
  | 'getMetadataTags'
  // Validation lambdas
  | 'postSchemaValidation'
  | 'validateDraftDataCompleteSchema'
  // Commentary lambdas
  | 'addPopulateDraftComment'
  | 'addReadyComment'
  // Ready to ICAv2 WES lambdas
  | 'convertReadyEventInputsToIcav2WesEventInputs'
  // ICAv2 WES to WRSC Event lambdas
  | 'convertIcav2WesEventToWrscEvent'
  | 'addWesFailureComment';

export const lambdaNameList: LambdaName[] = [
  // Shared pre-ready lambdas
  'comparePayload',
  'getDraftPayload',
  'findLatestWorkflow',
  'getOncoanalyserWgtsOutputsFromPortalRunId',
  'getWorkflowRunObject',
  'generateWruEventObjectWithMergedData',
  'getMissingSchemaFields',
  'getLatestPayloadFromPortalRunId',
  // Glue lambdas
  // Draft Builder lambdas
  'getFastqIdListFromRgidList',
  'getFastqRgidsFromLibraryId',
  'getLibraries',
  'getMetadataTags',
  // Validation lambdas
  'postSchemaValidation',
  'validateDraftDataCompleteSchema',
  // Commentary lambdas
  'addPopulateDraftComment',
  'addReadyComment',
  // Ready to ICAv2 WES lambdas
  'convertReadyEventInputsToIcav2WesEventInputs',
  // ICAv2 WES to WRSC Event lambdas
  'convertIcav2WesEventToWrscEvent',
  'addWesFailureComment',
];

// Requirements interface for Lambda functions
export interface LambdaRequirements {
  needsOrcabusApiTools?: boolean;
  needsIcav2Tools?: boolean;
  needsHigherMemory?: boolean;
  needsSsmParametersAccess?: boolean;
  needsSchemaRegistryAccess?: boolean;
  needsExternalBucketInfo?: boolean;
  needsWorkflowInfo?: boolean;
  needsRepoUrl?: boolean;
}

// Lambda requirements mapping
export const lambdaRequirementsMap: Record<LambdaName, LambdaRequirements> = {
  // Shared pre-ready lambdas
  comparePayload: {},
  getDraftPayload: {
    needsOrcabusApiTools: true,
  },
  findLatestWorkflow: {
    needsOrcabusApiTools: true,
  },
  getOncoanalyserWgtsOutputsFromPortalRunId: {
    needsOrcabusApiTools: true,
  },
  getWorkflowRunObject: {
    needsOrcabusApiTools: true,
  },
  generateWruEventObjectWithMergedData: {
    needsOrcabusApiTools: true,
  },
  getMissingSchemaFields: {
    needsSchemaRegistryAccess: true,
    needsSsmParametersAccess: true,
  },
  getLatestPayloadFromPortalRunId: {
    needsOrcabusApiTools: true,
  },
  // Glue lambdas
  // Draft Builder lambdas
  getFastqIdListFromRgidList: {
    needsOrcabusApiTools: true,
  },
  getFastqRgidsFromLibraryId: {
    needsOrcabusApiTools: true,
  },
  getLibraries: {
    needsOrcabusApiTools: true,
  },
  getMetadataTags: {
    needsOrcabusApiTools: true,
  },
  // Validation lambdas
  postSchemaValidation: {
    needsOrcabusApiTools: true,
    needsWorkflowInfo: true,
    needsExternalBucketInfo: true,
    needsIcav2Tools: true,
  },
  validateDraftDataCompleteSchema: {
    needsOrcabusApiTools: true,
    needsSchemaRegistryAccess: true,
    needsSsmParametersAccess: true,
    needsWorkflowInfo: true,
  },
  // Commentary lambdas
  addPopulateDraftComment: {
    needsOrcabusApiTools: true,
    needsWorkflowInfo: true,
    needsRepoUrl: true,
  },
  addReadyComment: {
    needsOrcabusApiTools: true,
    needsWorkflowInfo: true,
  },
  // Ready to ICAv2 WES lambdas
  convertReadyEventInputsToIcav2WesEventInputs: {},
  // ICAv2 WES to WRSC Event lambdas
  convertIcav2WesEventToWrscEvent: {
    needsOrcabusApiTools: true,
    needsWorkflowInfo: true,
  },
  addWesFailureComment: {
    needsOrcabusApiTools: true,
    needsWorkflowInfo: true,
  },
};

export interface LambdaInput {
  lambdaName: LambdaName;
}

export interface LambdaObject extends LambdaInput {
  lambdaFunction: PythonUvFunction;
}
