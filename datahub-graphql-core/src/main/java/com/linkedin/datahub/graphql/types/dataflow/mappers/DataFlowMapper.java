package com.linkedin.datahub.graphql.types.dataflow.mappers;

import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.Deprecation;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.generated.DataFlow;
import com.linkedin.datahub.graphql.generated.DataFlowEditableProperties;
import com.linkedin.datahub.graphql.generated.DataFlowInfo;
import com.linkedin.datahub.graphql.generated.DataFlowProperties;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.DataPlatformInstanceAspectMapper;
import com.linkedin.datahub.graphql.types.common.mappers.DeprecationMapper;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StringMapMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.datajob.EditableDataFlowProperties;
import com.linkedin.domain.Domains;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.key.DataFlowKey;
import com.linkedin.metadata.key.DataPlatformKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.Constants.*;


public class DataFlowMapper implements ModelMapper<EntityResponse, DataFlow> {

    public static final DataFlowMapper INSTANCE = new DataFlowMapper();

    public static DataFlow map(@Nonnull final EntityResponse entityResponse) {
        return INSTANCE.apply(entityResponse);
    }

    @Override
    public DataFlow apply(@Nonnull final EntityResponse entityResponse) {
        final DataFlow result = new DataFlow();
        result.setUrn(entityResponse.getUrn().toString());
        result.setType(EntityType.DATA_FLOW);
        EnvelopedAspectMap aspectMap = entityResponse.getAspects();
        MappingHelper<DataFlow> mappingHelper = new MappingHelper<>(aspectMap, result);
        mappingHelper.mapToResult(DATA_FLOW_KEY_ASPECT_NAME, this::mapKey);
        mappingHelper.mapToResult(DATA_FLOW_INFO_ASPECT_NAME, this::mapInfo);
        mappingHelper.mapToResult(EDITABLE_DATA_FLOW_PROPERTIES_ASPECT_NAME, this::mapEditableProperties);
        mappingHelper.mapToResult(OWNERSHIP_ASPECT_NAME, (dataFlow, dataMap) ->
            dataFlow.setOwnership(OwnershipMapper.map(new Ownership(dataMap))));
        mappingHelper.mapToResult(STATUS_ASPECT_NAME, (dataFlow, dataMap) ->
            dataFlow.setStatus(StatusMapper.map(new Status(dataMap))));
        mappingHelper.mapToResult(GLOBAL_TAGS_ASPECT_NAME, this::mapGlobalTags);
        mappingHelper.mapToResult(INSTITUTIONAL_MEMORY_ASPECT_NAME, (dataFlow, dataMap) ->
            dataFlow.setInstitutionalMemory(InstitutionalMemoryMapper.map(new InstitutionalMemory(dataMap))));
        mappingHelper.mapToResult(GLOSSARY_TERMS_ASPECT_NAME, (dataFlow, dataMap) ->
            dataFlow.setGlossaryTerms(GlossaryTermsMapper.map(new GlossaryTerms(dataMap))));
        mappingHelper.mapToResult(DOMAINS_ASPECT_NAME, this::mapDomains);
        mappingHelper.mapToResult(DEPRECATION_ASPECT_NAME, (dataFlow, dataMap) ->
            dataFlow.setDeprecation(DeprecationMapper.map(new Deprecation(dataMap))));
        mappingHelper.mapToResult(DATA_PLATFORM_INSTANCE_ASPECT_NAME, (dataset, dataMap) ->
            dataset.setDataPlatformInstance(DataPlatformInstanceAspectMapper.map(new DataPlatformInstance(dataMap))));

        return mappingHelper.getResult();
    }

    private void mapKey(@Nonnull DataFlow dataFlow, @Nonnull DataMap dataMap) {
        final DataFlowKey gmsKey = new DataFlowKey(dataMap);
        dataFlow.setOrchestrator(gmsKey.getOrchestrator());
        dataFlow.setFlowId(gmsKey.getFlowId());
        dataFlow.setCluster(gmsKey.getCluster());
        dataFlow.setPlatform(DataPlatform.builder()
            .setType(EntityType.DATA_PLATFORM)
            .setUrn(EntityKeyUtils
                .convertEntityKeyToUrn(new DataPlatformKey()
                    .setPlatformName(gmsKey.getOrchestrator()), DATA_PLATFORM_ENTITY_NAME).toString()).build());
    }

    private void mapInfo(@Nonnull DataFlow dataFlow, @Nonnull DataMap dataMap) {
        final com.linkedin.datajob.DataFlowInfo gmsDataFlowInfo = new com.linkedin.datajob.DataFlowInfo(dataMap);
        dataFlow.setInfo(mapDataFlowInfo(gmsDataFlowInfo));
        dataFlow.setProperties(mapDataFlowInfoToProperties(gmsDataFlowInfo));
    }

    /**
     * Maps GMS {@link com.linkedin.datajob.DataFlowInfo} to deprecated GraphQL {@link DataFlowInfo}
     */
    private DataFlowInfo mapDataFlowInfo(final com.linkedin.datajob.DataFlowInfo info) {
        final DataFlowInfo result = new DataFlowInfo();
        result.setName(info.getName());
        result.setDescription(info.getDescription());
        result.setProject(info.getProject());
        if (info.hasExternalUrl()) {
            result.setExternalUrl(info.getExternalUrl().toString());
        }
        if (info.hasCustomProperties()) {
            result.setCustomProperties(StringMapMapper.map(info.getCustomProperties()));
        }
        return result;
    }

    /**
     * Maps GMS {@link com.linkedin.datajob.DataFlowInfo} to new GraphQL {@link DataFlowProperties}
     */
    private DataFlowProperties mapDataFlowInfoToProperties(final com.linkedin.datajob.DataFlowInfo info) {
        final DataFlowProperties result = new DataFlowProperties();
        result.setName(info.getName());
        result.setDescription(info.getDescription());
        result.setProject(info.getProject());
        if (info.hasExternalUrl()) {
            result.setExternalUrl(info.getExternalUrl().toString());
        }
        if (info.hasCustomProperties()) {
            result.setCustomProperties(StringMapMapper.map(info.getCustomProperties()));
        }
        return result;
    }

    private void mapEditableProperties(@Nonnull DataFlow dataFlow, @Nonnull DataMap dataMap) {
        final EditableDataFlowProperties editableDataFlowProperties = new EditableDataFlowProperties(dataMap);
        final DataFlowEditableProperties dataFlowEditableProperties = new DataFlowEditableProperties();
        dataFlowEditableProperties.setDescription(editableDataFlowProperties.getDescription());
        dataFlow.setEditableProperties(dataFlowEditableProperties);
    }

    private void mapGlobalTags(@Nonnull DataFlow dataFlow, @Nonnull DataMap dataMap) {
        com.linkedin.datahub.graphql.generated.GlobalTags globalTags = GlobalTagsMapper.map(new GlobalTags(dataMap));
        dataFlow.setGlobalTags(globalTags);
        dataFlow.setTags(globalTags);
    }

    private void mapDomains(@Nonnull DataFlow dataFlow, @Nonnull DataMap dataMap) {
        final Domains domains = new Domains(dataMap);
        // Currently we only take the first domain if it exists.
        if (domains.getDomains().size() > 0) {
            dataFlow.setDomain(Domain.builder()
                .setType(EntityType.DOMAIN)
                .setUrn(domains.getDomains().get(0).toString()).build());
        }
    }
}
