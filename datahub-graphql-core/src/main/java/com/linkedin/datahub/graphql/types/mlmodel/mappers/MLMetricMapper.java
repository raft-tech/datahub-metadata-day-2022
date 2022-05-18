package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.datahub.graphql.generated.MLMetric;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import lombok.NonNull;


public class MLMetricMapper implements ModelMapper<com.linkedin.ml.metadata.MLMetric, MLMetric> {

    public static final MLMetricMapper INSTANCE = new MLMetricMapper();

    public static MLMetric map(@NonNull final com.linkedin.ml.metadata.MLMetric metric) {
        return INSTANCE.apply(metric);
    }

    @Override
    public MLMetric apply(@NonNull final com.linkedin.ml.metadata.MLMetric metric) {
        final MLMetric result = new MLMetric();
        result.setDescription(metric.getDescription());
        result.setValue(metric.getValue());
        result.setCreatedAt(metric.getCreatedAt());
        result.setName(metric.getName());
        return result;
    }
}
