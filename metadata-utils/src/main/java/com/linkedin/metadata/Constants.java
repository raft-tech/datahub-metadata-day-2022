package com.linkedin.metadata;

/**
 * Static class containing commonly-used constants across DataHub services.
 */
public class Constants {
  public static final String INTERNAL_DELEGATED_FOR_ACTOR_HEADER_NAME = "X-DataHub-Delegated-For";
  public static final String INTERNAL_DELEGATED_FOR_ACTOR_TYPE = "X-DataHub-Delegated-For-";

  public static final String DATAHUB_ACTOR = "urn:li:corpuser:datahub"; // Super user.
  public static final String SYSTEM_ACTOR = "urn:li:corpuser:__datahub_system"; // DataHub internal service principal.
  public static final String UNKNOWN_ACTOR = "urn:li:corpuser:UNKNOWN"; // Unknown principal.
  public static final Long ASPECT_LATEST_VERSION = 0L;
  public static final String UNKNOWN_DATA_PLATFORM = "urn:li:dataPlatform:unknown";

  public static final String DEFAULT_RUN_ID = "no-run-id-provided";

  /**
   * Entities
   */
  public static final String CORP_USER_ENTITY_NAME = "corpuser";
  public static final String CORP_GROUP_ENTITY_NAME = "corpGroup";
  public static final String DATASET_ENTITY_NAME = "dataset";
  public static final String CHART_ENTITY_NAME = "chart";
  public static final String DASHBOARD_ENTITY_NAME = "dashboard";
  public static final String DATA_FLOW_ENTITY_NAME = "dataFlow";
  public static final String DATA_JOB_ENTITY_NAME = "dataJob";
  public static final String DATA_PLATFORM_ENTITY_NAME = "dataPlatform";
  public static final String GLOSSARY_TERM_ENTITY_NAME = "glossaryTerm";
  public static final String ML_FEATURE_ENTITY_NAME = "mlFeature";
  public static final String ML_FEATURE_TABLE_ENTITY_NAME = "mlFeatureTable";
  public static final String ML_MODEL_ENTITY_NAME = "mlModel";
  public static final String ML_MODEL_GROUP_ENTITY_NAME = "mlModelGroup";
  public static final String ML_PRIMARY_KEY_ENTITY_NAME = "mlPrimaryKey";
  public static final String POLICY_ENTITY_NAME = "dataHubPolicy";
  public static final String TAG_ENTITY_NAME = "tag";
  public static final String CONTAINER_ENTITY_NAME = "container";
  public static final String DOMAIN_ENTITY_NAME = "domain";
  public static final String ASSERTION_ENTITY_NAME = "assertion";
  public static final String INGESTION_SOURCE_ENTITY_NAME = "dataHubIngestionSource";
  public static final String SECRETS_ENTITY_NAME = "dataHubSecret";
  public static final String EXECUTION_REQUEST_ENTITY_NAME = "dataHubExecutionRequest";
  public static final String NOTEBOOK_ENTITY_NAME = "notebook";
  public static final String DATA_PLATFORM_INSTANCE_ENTITY_NAME = "dataPlatformInstance";


  /**
   * Aspects
   */
  // Common
  public static final String OWNERSHIP_ASPECT_NAME = "ownership";
  public static final String INSTITUTIONAL_MEMORY_ASPECT_NAME = "institutionalMemory";
  public static final String DATA_PLATFORM_INSTANCE_ASPECT_NAME = "dataPlatformInstance";
  public static final String BROWSE_PATHS_ASPECT_NAME = "browsePaths";
  public static final String GLOBAL_TAGS_ASPECT_NAME = "globalTags";
  public static final String GLOSSARY_TERMS_ASPECT_NAME = "glossaryTerms";
  public static final String STATUS_ASPECT_NAME = "status";
  public static final String SUB_TYPES_ASPECT_NAME = "subTypes";
  public static final String DEPRECATION_ASPECT_NAME = "deprecation";

  // User
  public static final String CORP_USER_KEY_ASPECT_NAME = "corpUserKey";
  public static final String CORP_USER_EDITABLE_INFO_NAME = "corpUserEditableInfo";
  public static final String GROUP_MEMBERSHIP_ASPECT_NAME = "groupMembership";
  public static final String CORP_USER_EDITABLE_INFO_ASPECT_NAME = "corpUserEditableInfo";
  public static final String CORP_USER_INFO_ASPECT_NAME = "corpUserInfo";
  public static final String CORP_USER_STATUS_ASPECT_NAME = "corpUserStatus";

  // Group
  public static final String CORP_GROUP_KEY_ASPECT_NAME = "corpGroupKey";
  public static final String CORP_GROUP_INFO_ASPECT_NAME = "corpGroupInfo";
  public static final String CORP_GROUP_EDITABLE_INFO_ASPECT_NAME = "corpGroupEditableInfo";

  // Dataset
  public static final String DATASET_KEY_ASPECT_NAME = "datasetKey";
  public static final String DATASET_PROPERTIES_ASPECT_NAME = "datasetProperties";
  public static final String EDITABLE_DATASET_PROPERTIES_ASPECT_NAME = "editableDatasetProperties";
  public static final String DATASET_DEPRECATION_ASPECT_NAME = "datasetDeprecation";
  public static final String DATASET_UPSTREAM_LINEAGE_ASPECT_NAME = "datasetUpstreamLineage";
  public static final String UPSTREAM_LINEAGE_ASPECT_NAME = "upstreamLineage";
  public static final String SCHEMA_METADATA_ASPECT_NAME = "schemaMetadata";
  public static final String EDITABLE_SCHEMA_METADATA_ASPECT_NAME = "editableSchemaMetadata";
  public static final String VIEW_PROPERTIES_ASPECT_NAME = "viewProperties";
  public static final String DATASET_PROFILE_ASPECT_NAME = "datasetProfile";

  // Chart
  public static final String CHART_KEY_ASPECT_NAME = "chartKey";
  public static final String CHART_INFO_ASPECT_NAME = "chartInfo";
  public static final String EDITABLE_CHART_PROPERTIES_ASPECT_NAME = "editableChartProperties";
  public static final String CHART_QUERY_ASPECT_NAME = "chartQuery";

  // Dashboard
  public static final String DASHBOARD_KEY_ASPECT_NAME = "dashboardKey";
  public static final String DASHBOARD_INFO_ASPECT_NAME = "dashboardInfo";
  public static final String EDITABLE_DASHBOARD_PROPERTIES_ASPECT_NAME = "editableDashboardProperties";

  // Notebook
  public static final String NOTEBOOK_KEY_ASPECT_NAME = "notebookKey";
  public static final String NOTEBOOK_INFO_ASPECT_NAME = "notebookInfo";
  public static final String NOTEBOOK_CONTENT_ASPECT_NAME = "notebookContent";
  public static final String EDITABLE_NOTEBOOK_PROPERTIES_ASPECT_NAME = "editableNotebookProperties";

  // DataFlow
  public static final String DATA_FLOW_KEY_ASPECT_NAME = "dataFlowKey";
  public static final String DATA_FLOW_INFO_ASPECT_NAME = "dataFlowInfo";
  public static final String EDITABLE_DATA_FLOW_PROPERTIES_ASPECT_NAME = "editableDataFlowProperties";

  // DataJob
  public static final String DATA_JOB_KEY_ASPECT_NAME = "dataJobKey";
  public static final String DATA_JOB_INFO_ASPECT_NAME = "dataJobInfo";
  public static final String DATA_JOB_INPUT_OUTPUT_ASPECT_NAME = "dataJobInputOutput";
  public static final String EDITABLE_DATA_JOB_PROPERTIES_ASPECT_NAME = "editableDataJobProperties";

  // DataPlatform
  public static final String DATA_PLATFORM_KEY_ASPECT_NAME = "dataPlatformKey";
  public static final String DATA_PLATFORM_INFO_ASPECT_NAME = "dataPlatformInfo";

  // DataPlatformInstance
  public static final String DATA_PLATFORM_INSTANCE_KEY_ASPECT_NAME = "dataPlatformInstanceKey";


  // ML Feature
  public static final String ML_FEATURE_KEY_ASPECT_NAME = "mlFeatureKey";
  public static final String ML_FEATURE_PROPERTIES_ASPECT_NAME = "mlFeatureProperties";
  public static final String ML_FEATURE_EDITABLE_PROPERTIES_ASPECT_NAME = "editableMlFeatureProperties";

  // ML Feature Table
  public static final String ML_FEATURE_TABLE_KEY_ASPECT_NAME = "mlFeatureTableKey";
  public static final String ML_FEATURE_TABLE_PROPERTIES_ASPECT_NAME = "mlFeatureTableProperties";
  public static final String ML_FEATURE_TABLE_EDITABLE_PROPERTIES_ASPECT_NAME = "editableMlFeatureTableProperties";

  //ML Model
  public static final String ML_MODEL_KEY_ASPECT_NAME = "mlModelKey";
  public static final String ML_MODEL_PROPERTIES_ASPECT_NAME = "mlModelProperties";
  public static final String ML_MODEL_EDITABLE_PROPERTIES_ASPECT_NAME = "editableMlModelProperties";
  public static final String INTENDED_USE_ASPECT_NAME = "intendedUse";
  public static final String ML_MODEL_FACTOR_PROMPTS_ASPECT_NAME = "mlModelFactorPrompts";
  public static final String METRICS_ASPECT_NAME = "metrics";
  public static final String EVALUATION_DATA_ASPECT_NAME = "evaluationData";
  public static final String TRAINING_DATA_ASPECT_NAME = "trainingData";
  public static final String QUANTITATIVE_ANALYSES_ASPECT_NAME = "quantitativeAnalyses";
  public static final String ETHICAL_CONSIDERATIONS_ASPECT_NAME = "ethicalConsiderations";
  public static final String CAVEATS_AND_RECOMMENDATIONS_ASPECT_NAME = "caveatsAndRecommendations";
  public static final String SOURCE_CODE_ASPECT_NAME = "sourceCode";
  public static final String COST_ASPECT_NAME = "cost";

  // ML Model Group
  public static final String ML_MODEL_GROUP_KEY_ASPECT_NAME = "mlModelGroupKey";
  public static final String ML_MODEL_GROUP_PROPERTIES_ASPECT_NAME = "mlModelGroupProperties";
  public static final String ML_MODEL_GROUP_EDITABLE_PROPERTIES_ASPECT_NAME = "editableMlModelGroupProperties";

  // ML Primary Key
  public static final String ML_PRIMARY_KEY_KEY_ASPECT_NAME = "mlPrimaryKeyKey";
  public static final String ML_PRIMARY_KEY_PROPERTIES_ASPECT_NAME = "mlPrimaryKeyProperties";
  public static final String ML_PRIMARY_KEY_EDITABLE_PROPERTIES_ASPECT_NAME = "editableMlPrimaryKeyProperties";

  // Policy
  public static final String DATAHUB_POLICY_INFO_ASPECT_NAME = "dataHubPolicyInfo";

  // Tag
  public static final String TAG_KEY_ASPECT_NAME = "tagKey";
  public static final String TAG_PROPERTIES_ASPECT_NAME = "tagProperties";

  // Container
  public static final String CONTAINER_KEY_ASPECT_NAME = "containerKey";
  public static final String CONTAINER_PROPERTIES_ASPECT_NAME = "containerProperties";
  public static final String CONTAINER_EDITABLE_PROPERTIES_ASPECT_NAME = "editableContainerProperties";
  public static final String CONTAINER_ASPECT_NAME = "container"; // parent container

 // Glossary term
  public static final String GLOSSARY_TERM_KEY_ASPECT_NAME = "glossaryTermKey";
  public static final String GLOSSARY_TERM_INFO_ASPECT_NAME = "glossaryTermInfo";
  public static final String GLOSSARY_RELATED_TERM_ASPECT_NAME = "glossaryRelatedTerms";

  // Domain
  public static final String DOMAIN_KEY_ASPECT_NAME = "domainKey";
  public static final String DOMAIN_PROPERTIES_ASPECT_NAME = "domainProperties";
  public static final String DOMAINS_ASPECT_NAME = "domains";

  // Assertion
  public static final String ASSERTION_KEY_ASPECT_NAME = "assertionKey";
  public static final String ASSERTION_INFO_ASPECT_NAME = "assertionInfo";
  public static final String ASSERTION_RUN_EVENT_ASPECT_NAME = "assertionRunEvent";
  public static final String ASSERTION_RUN_EVENT_STATUS_COMPLETE = "COMPLETE";

  // DataHub Ingestion Source
  public static final String INGESTION_SOURCE_KEY_ASPECT_NAME = "dataHubIngestionSourceKey";
  public static final String INGESTION_INFO_ASPECT_NAME = "dataHubIngestionSourceInfo";

  // DataHub Secret
  public static final String SECRET_VALUE_ASPECT_NAME = "dataHubSecretValue";

  // DataHub Execution Request
  public static final String EXECUTION_REQUEST_INPUT_ASPECT_NAME = "dataHubExecutionRequestInput";
  public static final String EXECUTION_REQUEST_SIGNAL_ASPECT_NAME = "dataHubExecutionRequestSignal";
  public static final String EXECUTION_REQUEST_RESULT_ASPECT_NAME = "dataHubExecutionRequestResult";

  // acryl-main only
  public static final String CHANGE_EVENT_PLATFORM_EVENT_NAME = "entityChangeEvent";

  /**
   * User Status
   */
  public static final String CORP_USER_STATUS_ACTIVE = "ACTIVE";

  /**
   * Task Runs
   */
  public static final String DATA_PROCESS_INSTANCE_ENTITY_NAME = "dataProcessInstance";
  public static final String DATA_PROCESS_INSTANCE_PROPERTIES_ASPECT_NAME = "dataProcessInstanceProperties";
  public static final String DATA_PROCESS_INSTANCE_RUN_EVENT_ASPECT_NAME = "dataProcessInstanceRunEvent";

  private Constants() {
  }
}
