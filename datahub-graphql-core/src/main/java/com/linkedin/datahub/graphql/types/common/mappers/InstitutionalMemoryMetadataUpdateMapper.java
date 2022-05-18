package com.linkedin.datahub.graphql.types.common.mappers;

import javax.annotation.Nonnull;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.InstitutionalMemoryMetadata;
import com.linkedin.common.url.Url;
import com.linkedin.datahub.graphql.generated.InstitutionalMemoryMetadataUpdate;
import com.linkedin.datahub.graphql.types.corpuser.CorpUserUtils;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;

public class InstitutionalMemoryMetadataUpdateMapper implements ModelMapper<InstitutionalMemoryMetadataUpdate, InstitutionalMemoryMetadata> {

    private static final InstitutionalMemoryMetadataUpdateMapper INSTANCE = new InstitutionalMemoryMetadataUpdateMapper();

    public static InstitutionalMemoryMetadata map(@Nonnull final InstitutionalMemoryMetadataUpdate input) {
        return INSTANCE.apply(input);
    }

    @Override
    public InstitutionalMemoryMetadata apply(@Nonnull final InstitutionalMemoryMetadataUpdate input) {
        final InstitutionalMemoryMetadata metadata = new InstitutionalMemoryMetadata();
        metadata.setDescription(input.getDescription());
        metadata.setUrl(new Url(input.getUrl()));
        metadata.setCreateStamp(new AuditStamp()
            .setActor(CorpUserUtils.getCorpUserUrn(input.getAuthor()))
            .setTime(input.getCreatedAt() == null ? System.currentTimeMillis() : input.getCreatedAt())
        );
        return metadata;
    }
}
