package com.linkedin.metadata.recommendation.candidatesource;

import com.codahale.metrics.Timer;
import com.datahub.util.exception.ESQueryException;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.datahubusage.DataHubUsageEventConstants;
import com.linkedin.metadata.datahubusage.DataHubUsageEventType;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.recommendation.EntityProfileParams;
import com.linkedin.metadata.recommendation.RecommendationContent;
import com.linkedin.metadata.recommendation.RecommendationParams;
import com.linkedin.metadata.recommendation.RecommendationRenderType;
import com.linkedin.metadata.recommendation.RecommendationRequestContext;
import com.linkedin.metadata.recommendation.ScenarioType;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.opentelemetry.extension.annotations.WithSpan;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.builder.SearchSourceBuilder;


@Slf4j
@RequiredArgsConstructor
public class RecentlyViewedSource implements RecommendationSource {
  private final RestHighLevelClient _searchClient;
  private final IndexConvention _indexConvention;
  private final EntityService _entityService;

  private static final String DATAHUB_USAGE_INDEX = "datahub_usage_event";
  private static final String ENTITY_AGG_NAME = "entity";
  private static final int MAX_CONTENT = 5;

  @Override
  public String getTitle() {
    return "Recently Viewed";
  }

  @Override
  public String getModuleId() {
    return "RecentlyViewedEntities";
  }

  @Override
  public RecommendationRenderType getRenderType() {
    return RecommendationRenderType.ENTITY_NAME_LIST;
  }

  @Override
  public boolean isEligible(@Nonnull Urn userUrn, @Nonnull RecommendationRequestContext requestContext) {
    boolean analyticsEnabled = false;
    try {
      analyticsEnabled = _searchClient.indices()
          .exists(new GetIndexRequest(_indexConvention.getIndexName(DATAHUB_USAGE_INDEX)), RequestOptions.DEFAULT);
    } catch (IOException e) {
      log.error("Failed to check whether DataHub usage index exists");
    }
    return requestContext.getScenario() == ScenarioType.HOME && analyticsEnabled;
  }

  @Override
  @WithSpan
  public List<RecommendationContent> getRecommendations(@Nonnull Urn userUrn,
      @Nonnull RecommendationRequestContext requestContext) {
    SearchRequest searchRequest = buildSearchRequest(userUrn);
    try (Timer.Context ignored = MetricUtils.timer(this.getClass(), "getRecentlyViewed").time()) {
      final SearchResponse searchResponse = _searchClient.search(searchRequest, RequestOptions.DEFAULT);
      // extract results
      ParsedTerms parsedTerms = searchResponse.getAggregations().get(ENTITY_AGG_NAME);
      return parsedTerms.getBuckets()
          .stream()
          .map(bucket -> buildContent(bucket.getKeyAsString()))
          .filter(Optional::isPresent)
          .map(Optional::get).limit(MAX_CONTENT)
          .collect(Collectors.toList());
    } catch (Exception e) {
      log.error("Search query to get most recently viewed entities failed", e);
      throw new ESQueryException("Search query failed:", e);
    }
  }

  private SearchRequest buildSearchRequest(@Nonnull Urn userUrn) {
    SearchRequest request = new SearchRequest();
    SearchSourceBuilder source = new SearchSourceBuilder();
    BoolQueryBuilder query = QueryBuilders.boolQuery();
    // Filter for the entity view events of the user requesting recommendation
    query.must(
        QueryBuilders.termQuery(DataHubUsageEventConstants.ACTOR_URN + ESUtils.KEYWORD_SUFFIX, userUrn.toString()));
    query.must(
        QueryBuilders.termQuery(DataHubUsageEventConstants.TYPE, DataHubUsageEventType.ENTITY_VIEW_EVENT.getType()));
    source.query(query);

    // Find the entity with the largest last viewed timestamp
    String lastViewed = "last_viewed";
    AggregationBuilder aggregation = AggregationBuilders.terms(ENTITY_AGG_NAME)
        .field(DataHubUsageEventConstants.ENTITY_URN + ESUtils.KEYWORD_SUFFIX)
        .size(MAX_CONTENT)
        .order(BucketOrder.aggregation(lastViewed, false))
        .subAggregation(AggregationBuilders.max(lastViewed).field(DataHubUsageEventConstants.TIMESTAMP));
    source.aggregation(aggregation);
    source.size(0);

    request.source(source);
    request.indices(_indexConvention.getIndexName(DATAHUB_USAGE_INDEX));
    return request;
  }

  private Optional<RecommendationContent> buildContent(@Nonnull String entityUrn) {
    Urn entity = UrnUtils.getUrn(entityUrn);
    if (EntityUtils.checkIfRemoved(_entityService, entity)) {
      return Optional.empty();
    }

    return Optional.of(new RecommendationContent().setEntity(entity)
        .setValue(entityUrn)
        .setParams(new RecommendationParams().setEntityProfileParams(new EntityProfileParams().setUrn(entity))));
  }
}
