package com.linkedin.datahub.graphql.types.corpgroup;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.authorization.ConjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.authorization.DisjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.CorpGroup;
import com.linkedin.datahub.graphql.generated.CorpGroupUpdateInput;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.types.MutableType;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.corpgroup.mappers.CorpGroupMapper;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpGroupEditableInfo;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.execution.DataFetcherResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.linkedin.metadata.Constants.*;

public class CorpGroupType implements SearchableEntityType<CorpGroup, String>, MutableType<CorpGroupUpdateInput, CorpGroup> {

    private final EntityClient _entityClient;

    public CorpGroupType(final EntityClient entityClient) {
        _entityClient = entityClient;
    }

    @Override
    public Class<CorpGroup> objectClass() {
        return CorpGroup.class;
    }

    public Class<CorpGroupUpdateInput> inputClass() {
        return CorpGroupUpdateInput.class;
    }

    @Override
    public EntityType type() {
        return EntityType.CORP_GROUP;
    }

    @Override
    public Function<Entity, String> getKeyProvider() {
        return Entity::getUrn;
    }

    @Override
    public List<DataFetcherResult<CorpGroup>> batchLoad(final List<String> urns, final QueryContext context) {
        try {
            final List<Urn> corpGroupUrns = urns
                    .stream()
                    .map(UrnUtils::getUrn)
                    .collect(Collectors.toList());

            final Map<Urn, EntityResponse> corpGroupMap = _entityClient.batchGetV2(CORP_GROUP_ENTITY_NAME,
                new HashSet<>(corpGroupUrns), null, context.getAuthentication());

            final List<EntityResponse> results = new ArrayList<>();
            for (Urn urn : corpGroupUrns) {
                results.add(corpGroupMap.getOrDefault(urn, null));
            }
            return results.stream()
                    .map(gmsCorpGroup -> gmsCorpGroup == null ? null
                        : DataFetcherResult.<CorpGroup>newResult().data(CorpGroupMapper.map(gmsCorpGroup)).build())
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch load CorpGroup", e);
        }
    }

    @Override
    public SearchResults search(@Nonnull String query,
                                @Nullable List<FacetFilterInput> filters,
                                int start,
                                int count,
                                @Nonnull final QueryContext context) throws Exception {
        final SearchResult
            searchResult = _entityClient.search("corpGroup", query, Collections.emptyMap(), start, count,
            context.getAuthentication());
        return UrnSearchResultsMapper.map(searchResult);
    }

    @Override
    public AutoCompleteResults autoComplete(@Nonnull String query,
                                            @Nullable String field,
                                            @Nullable List<FacetFilterInput> filters,
                                            int limit,
                                            @Nonnull final QueryContext context) throws Exception {
        final AutoCompleteResult result = _entityClient.autoComplete("corpGroup", query, Collections.emptyMap(), limit,
            context.getAuthentication());
        return AutoCompleteResultsMapper.map(result);
    }

    @Override
    public CorpGroup update(@Nonnull String urn, @Nonnull CorpGroupUpdateInput input, @Nonnull QueryContext context) throws Exception {
        if (isAuthorizedToUpdate(urn, input, context)) {
            // Get existing editable info to merge with
            Urn groupUrn = Urn.createFromString(urn);
            Map<Urn, EntityResponse> gmsResponse =
                _entityClient.batchGetV2(CORP_GROUP_ENTITY_NAME, ImmutableSet.of(groupUrn), ImmutableSet.of(
                    CORP_GROUP_EDITABLE_INFO_ASPECT_NAME),
                    context.getAuthentication());

            CorpGroupEditableInfo existingCorpGroupEditableInfo = null;
            if (gmsResponse.containsKey(groupUrn) && gmsResponse.get(groupUrn).getAspects().containsKey(CORP_GROUP_EDITABLE_INFO_ASPECT_NAME)) {
                existingCorpGroupEditableInfo = new CorpGroupEditableInfo(gmsResponse.get(groupUrn).getAspects()
                    .get(CORP_GROUP_EDITABLE_INFO_ASPECT_NAME).getValue().data());
            }

            // Create the MCP
            final MetadataChangeProposal proposal = new MetadataChangeProposal();
            proposal.setEntityUrn(Urn.createFromString(urn));
            proposal.setEntityType(CORP_GROUP_ENTITY_NAME);
            proposal.setAspectName(CORP_GROUP_EDITABLE_INFO_ASPECT_NAME);
            proposal.setAspect(
                GenericRecordUtils.serializeAspect(mapCorpGroupEditableInfo(input, existingCorpGroupEditableInfo)));
            proposal.setChangeType(ChangeType.UPSERT);
            _entityClient.ingestProposal(proposal, context.getAuthentication());

            return load(urn, context).getData();
        }
        throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
    }

    private boolean isAuthorizedToUpdate(String urn, CorpGroupUpdateInput input, QueryContext context) {
        // Decide whether the current principal should be allowed to update the Dataset.
        final DisjunctivePrivilegeGroup orPrivilegeGroups = getAuthorizedPrivileges(input);
        return AuthorizationUtils.isAuthorized(
            context.getAuthorizer(),
            context.getAuthentication().getActor().toUrnStr(),
            PoliciesConfig.CORP_GROUP_PRIVILEGES.getResourceType(),
            urn,
            orPrivilegeGroups);
    }

    private DisjunctivePrivilegeGroup getAuthorizedPrivileges(final CorpGroupUpdateInput updateInput) {
        final ConjunctivePrivilegeGroup allPrivilegesGroup = new ConjunctivePrivilegeGroup(ImmutableList.of(
            PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()
        ));

        List<String> specificPrivileges = new ArrayList<>();
        if (updateInput.getDescription() != null) {
            // Requires the Update Docs privilege.
            specificPrivileges.add(PoliciesConfig.EDIT_ENTITY_DOCS_PRIVILEGE.getType());
        } else if (updateInput.getSlack() != null || updateInput.getEmail() != null) {
            // Requires the Update Contact info privilege.
            specificPrivileges.add(PoliciesConfig.EDIT_CONTACT_INFO_PRIVILEGE.getType());
        }

        final ConjunctivePrivilegeGroup specificPrivilegeGroup = new ConjunctivePrivilegeGroup(specificPrivileges);

        // If you either have all entity privileges, or have the specific privileges required, you are authorized.
        return new DisjunctivePrivilegeGroup(ImmutableList.of(
            allPrivilegesGroup,
            specificPrivilegeGroup
        ));
    }

    private RecordTemplate mapCorpGroupEditableInfo(CorpGroupUpdateInput input, @Nullable CorpGroupEditableInfo existing) {
        CorpGroupEditableInfo result = existing != null ? existing : new CorpGroupEditableInfo();

        if (input.getDescription() != null) {
            result.setDescription(input.getDescription());
        }
        if (input.getSlack() != null) {
            result.setSlack(input.getSlack());
        }
        if (input.getEmail() != null) {
            result.setEmail(input.getEmail());
        }
        return result;
    }
}
