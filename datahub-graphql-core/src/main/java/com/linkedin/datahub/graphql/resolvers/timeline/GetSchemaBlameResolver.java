package com.linkedin.datahub.graphql.resolvers.timeline;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.GetSchemaBlameInput;
import com.linkedin.datahub.graphql.generated.GetSchemaBlameResult;
import com.linkedin.datahub.graphql.types.timeline.mappers.SchemaFieldBlameMapper;
import com.linkedin.metadata.timeline.TimelineService;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


/*
Returns the most recent changes made to each column in a dataset at each dataset version.
 */
@Slf4j
public class GetSchemaBlameResolver implements DataFetcher<CompletableFuture<GetSchemaBlameResult>> {
  private final TimelineService _timelineService;

  public GetSchemaBlameResolver(TimelineService timelineService) {
    _timelineService = timelineService;
  }

  @Override
  public CompletableFuture<GetSchemaBlameResult> get(final DataFetchingEnvironment environment) throws Exception {
    final GetSchemaBlameInput input = bindArgument(environment.getArgument("input"), GetSchemaBlameInput.class);

    final String datasetUrnString = input.getDatasetUrn();
    final long startTime = 0;
    final long endTime = 0;
    final String version = input.getVersion() == null ? null : input.getVersion();

    return CompletableFuture.supplyAsync(() -> {
      try {
        final Set<ChangeCategory> changeCategorySet = new HashSet<>();
        changeCategorySet.add(ChangeCategory.TECHNICAL_SCHEMA);
        Urn datasetUrn = Urn.createFromString(datasetUrnString);
        List<ChangeTransaction> changeTransactionList =
            _timelineService.getTimeline(datasetUrn, changeCategorySet, startTime, endTime, null, null, false);
        return SchemaFieldBlameMapper.map(changeTransactionList, version);
      } catch (URISyntaxException u) {
        log.error(
            String.format("Failed to list schema blame data, likely due to the Urn %s being invalid", datasetUrnString),
            u);
        return null;
      } catch (Exception e) {
        log.error("Failed to list schema blame data", e);
        return null;
      }
    });
  }
}