import { IEventBus } from 'aws-cdk-lib/aws-events';
import { StateMachine } from 'aws-cdk-lib/aws-stepfunctions';

import { LambdaName, LambdaObject } from '../lambda/interfaces';
import { SsmParameterPaths } from '../ssm/interfaces';

/**
 * Step Function Interfaces
 */
export type StateMachineName =
  // Glue code
  | 'glueSucceededEventsToDraftUpdate'
  // Draft populator
  | 'populateDraftData'
  // Validate draft data and put ready event
  | 'validateDraftDataAndPutReadyEvent'
  // Ready-to-Submitted
  | 'readyEventToIcav2WesRequestEvent'
  // Post-submission event conversion
  | 'icav2WesEventToWrscEvent';

export const stateMachineNameList: StateMachineName[] = [
  // Glue code
  'glueSucceededEventsToDraftUpdate',
  // Draft populator
  'populateDraftData',
  // Validate draft data and put ready event
  'validateDraftDataAndPutReadyEvent',
  // Ready-to-Submitted
  'readyEventToIcav2WesRequestEvent',
  // Post-submission event conversion
  'icav2WesEventToWrscEvent',
];

// Requirements interface for Step Functions
export interface StepFunctionRequirements {
  // Event stuff
  needsEventPutPermission?: boolean;
  // SSM Stuff
  needsSsmParameterStoreAccess?: boolean;
}

export interface BuildStepFunctionsProps {
  lambdaObjects: LambdaObject[];
  eventBus: IEventBus;
  ssmParameterPaths: SsmParameterPaths;
}

export interface BuildStepFunctionProps extends BuildStepFunctionsProps {
  stateMachineName: StateMachineName;
}

export interface StepFunctionObject {
  stateMachineName: StateMachineName;
  sfnObject: StateMachine;
}

export type WireUpPermissionsProps = BuildStepFunctionProps & StepFunctionObject;

export const stepFunctionsRequirementsMap: Record<StateMachineName, StepFunctionRequirements> = {
  // Glue code
  glueSucceededEventsToDraftUpdate: {
    needsEventPutPermission: true,
  },
  // Draft populator
  populateDraftData: {
    needsEventPutPermission: true,
    needsSsmParameterStoreAccess: true,
  },
  // Validate draft data and put ready event
  validateDraftDataAndPutReadyEvent: {
    needsEventPutPermission: true,
  },
  // Ready-to-Submitted
  readyEventToIcav2WesRequestEvent: {
    needsEventPutPermission: true,
  },
  // Post-submission event conversion
  icav2WesEventToWrscEvent: {
    needsEventPutPermission: true,
  },
};

export const stepFunctionToLambdasMap: Record<StateMachineName, LambdaName[]> = {
  glueSucceededEventsToDraftUpdate: [
    // Shared pre-ready lambdas
    'getOncoanalyserWgtsOutputsFromPortalRunId',
    'generateWruEventObjectWithMergedData',
    'comparePayload',
    'getWorkflowRunObject',
    'findLatestWorkflow',
    'getDraftPayload',
  ],
  populateDraftData: [
    // Shared pre-ready lambdas
    'getOncoanalyserWgtsOutputsFromPortalRunId',
    'generateWruEventObjectWithMergedData',
    'comparePayload',
    'getMissingSchemaFields',
    'getWorkflowRunObject',
    'findLatestWorkflow',
    'getDraftPayload',
    // Draft to ready
    'getLibraries',
    'getFastqRgidsFromLibraryId',
    'getMetadataTags',
    'getFastqIdListFromRgidList',
    // Commentary
    'addPopulateDraftComment',
    // Validation
    'validateDraftDataCompleteSchema',
  ],
  validateDraftDataAndPutReadyEvent: [
    // Validation
    'validateDraftDataCompleteSchema',
    'postSchemaValidation',
  ],
  readyEventToIcav2WesRequestEvent: [
    // Commentary lambdas
    'addReadyComment',
    // Ready to ICAv2 WES lambdas
    'convertReadyEventInputsToIcav2WesEventInputs',
  ],
  icav2WesEventToWrscEvent: [
    // ICAv2 WES to WRSC Event lambdas
    'convertIcav2WesEventToWrscEvent',
    'addWesFailureComment',
  ],
};
