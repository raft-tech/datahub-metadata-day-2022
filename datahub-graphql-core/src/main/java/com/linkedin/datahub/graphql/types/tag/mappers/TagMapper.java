package com.linkedin.datahub.graphql.types.tag.mappers;

import com.linkedin.common.Ownership;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.GetMode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Tag;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.key.TagKey;
import com.linkedin.tag.TagProperties;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.Constants.*;


/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * To be replaced by auto-generated mappers implementations
 */
public class TagMapper implements ModelMapper<EntityResponse, Tag> {

    public static final TagMapper INSTANCE = new TagMapper();

    public static Tag map(@Nonnull final EntityResponse entityResponse) {
        return INSTANCE.apply(entityResponse);
    }

    @Override
    public Tag apply(@Nonnull final EntityResponse entityResponse) {
        final Tag result = new Tag();
        result.setUrn(entityResponse.getUrn().toString());
        result.setType(EntityType.TAG);

        final String legacyName = entityResponse.getUrn().getId();
        result.setName(legacyName);

        EnvelopedAspectMap aspectMap = entityResponse.getAspects();
        MappingHelper<Tag> mappingHelper = new MappingHelper<>(aspectMap, result);
        mappingHelper.mapToResult(TAG_KEY_ASPECT_NAME, this::mapTagKey);
        mappingHelper.mapToResult(TAG_PROPERTIES_ASPECT_NAME, this::mapTagProperties);
        mappingHelper.mapToResult(OWNERSHIP_ASPECT_NAME, (tag, dataMap) ->
            tag.setOwnership(OwnershipMapper.map(new Ownership(dataMap))));

        if (result.getProperties() != null && result.getProperties().getName() == null) {
            result.getProperties().setName(legacyName);
        }

        return mappingHelper.getResult();
    }

    private void mapTagKey(@Nonnull Tag tag, @Nonnull DataMap dataMap) {
        TagKey tagKey = new TagKey(dataMap);
        tag.setName(tagKey.getName());
    }

    private void mapTagProperties(@Nonnull Tag tag, @Nonnull DataMap dataMap) {
        final TagProperties properties = new TagProperties(dataMap);
        final com.linkedin.datahub.graphql.generated.TagProperties graphQlProperties =
            new com.linkedin.datahub.graphql.generated.TagProperties.Builder()
                .setColorHex(properties.getColorHex(GetMode.DEFAULT))
                .setName(properties.getName(GetMode.DEFAULT))
                .setDescription(properties.getDescription(GetMode.DEFAULT))
                .build();
        tag.setProperties(graphQlProperties);
        // Set deprecated top-level description field.
        if (properties.hasDescription()) {
            tag.setDescription(properties.getDescription());
        }
    }
}
