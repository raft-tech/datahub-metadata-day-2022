package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.datahub.graphql.generated.MLFeature;
import com.linkedin.datahub.graphql.generated.MLFeatureTableProperties;
import com.linkedin.datahub.graphql.generated.MLPrimaryKey;
import com.linkedin.datahub.graphql.types.common.mappers.StringMapMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import lombok.NonNull;

import java.util.stream.Collectors;

public class MLFeatureTablePropertiesMapper implements ModelMapper<com.linkedin.ml.metadata.MLFeatureTableProperties, MLFeatureTableProperties> {

    public static final MLFeatureTablePropertiesMapper INSTANCE = new MLFeatureTablePropertiesMapper();

    public static MLFeatureTableProperties map(@NonNull final com.linkedin.ml.metadata.MLFeatureTableProperties mlFeatureTableProperties) {
        return INSTANCE.apply(mlFeatureTableProperties);
    }

    @Override
    public MLFeatureTableProperties apply(@NonNull final com.linkedin.ml.metadata.MLFeatureTableProperties mlFeatureTableProperties) {
        final MLFeatureTableProperties result = new MLFeatureTableProperties();

        result.setDescription(mlFeatureTableProperties.getDescription());
        if (mlFeatureTableProperties.getMlFeatures() != null) {
            result.setMlFeatures(mlFeatureTableProperties.getMlFeatures().stream().map(urn -> {
                final MLFeature mlFeature = new MLFeature();
                mlFeature.setUrn(urn.toString());
                return mlFeature;
            }).collect(Collectors.toList()));
        }

        if (mlFeatureTableProperties.getMlPrimaryKeys() != null) {
            result.setMlPrimaryKeys(mlFeatureTableProperties
                .getMlPrimaryKeys()
                .stream()
                .map(urn -> {
                    final MLPrimaryKey mlPrimaryKey = new MLPrimaryKey();
                    mlPrimaryKey.setUrn(urn.toString());
                    return mlPrimaryKey;
                })
                .collect(Collectors.toList()));
        }

        if (mlFeatureTableProperties.hasCustomProperties()) {
            result.setCustomProperties(StringMapMapper.map(mlFeatureTableProperties.getCustomProperties()));
        }

        return result;
    }
}
