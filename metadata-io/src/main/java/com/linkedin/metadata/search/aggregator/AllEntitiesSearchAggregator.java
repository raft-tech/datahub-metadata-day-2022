package com.linkedin.metadata.search.aggregator;

import com.codahale.metrics.Timer;
import com.linkedin.data.template.LongMap;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.FilterValueArray;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.search.cache.EntitySearchServiceCache;
import com.linkedin.metadata.search.cache.EntityDocCountCache;
import com.linkedin.metadata.search.ranker.SearchRanker;
import com.linkedin.metadata.search.utils.SearchUtils;
import com.linkedin.metadata.utils.ConcurrencyUtils;
import com.linkedin.metadata.utils.SearchUtil;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.util.Pair;
import io.opentelemetry.extension.annotations.WithSpan;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.CacheManager;

import static com.linkedin.metadata.search.utils.FilterUtils.rankFilterGroups;


@Slf4j
public class AllEntitiesSearchAggregator {
  private final EntitySearchService _entitySearchService;
  private final SearchRanker _searchRanker;
  private final EntityDocCountCache _entityDocCountCache;

  private final EntitySearchServiceCache _entitySearchServiceCache;

  public AllEntitiesSearchAggregator(EntityRegistry entityRegistry, EntitySearchService entitySearchService,
      SearchRanker searchRanker, CacheManager cacheManager, int batchSize, boolean enableCache) {
    _entitySearchService = entitySearchService;
    _searchRanker = searchRanker;
    _entityDocCountCache = new EntityDocCountCache(entityRegistry, entitySearchService);
    _entitySearchServiceCache = new EntitySearchServiceCache(cacheManager, entitySearchService, batchSize, enableCache);
  }

  @Nonnull
  @WithSpan
  public SearchResult search(@Nonnull List<String> entities, @Nonnull String input, @Nullable Filter postFilters,
      @Nullable SortCriterion sortCriterion, int from, int size, @Nullable SearchFlags searchFlags) {
    // 1. Get entities to query for (Do not query entities without a single document)
    List<String> nonEmptyEntities;
    List<String> lowercaseEntities = entities.stream().map(String::toLowerCase).collect(Collectors.toList());
    try (Timer.Context ignored = MetricUtils.timer(this.getClass(), "getNonEmptyEntities").time()) {
      nonEmptyEntities = _entityDocCountCache.getNonEmptyEntities();
    }
    if (!entities.isEmpty()) {
      nonEmptyEntities = nonEmptyEntities.stream().filter(lowercaseEntities::contains).collect(Collectors.toList());
    }

    // Make sure the request does not exceed max result size of the underlying entity search service
    int queryFrom = from;
    int querySize = size;
    if (from >= _entitySearchService.maxResultSize()) {
      queryFrom = 0;
      querySize = 0;
    } else if (from + size >= _entitySearchService.maxResultSize()) {
      querySize = _entitySearchService.maxResultSize() - from;
    }

    // 2. Get search results for each entity
    Map<String, SearchResult> searchResults =
        getSearchResultsForEachEntity(nonEmptyEntities, input, postFilters, sortCriterion, queryFrom, querySize,
            searchFlags);

    if (searchResults.isEmpty()) {
      return getEmptySearchResult(from, size);
    }

    Timer.Context postProcessTimer = MetricUtils.timer(this.getClass(), "postProcessTimer").time();

    // 3. Combine search results from all entities
    int numEntities = 0;
    List<SearchEntity> matchedResults = new ArrayList<>();
    Map<String, AggregationMetadata> aggregations = new HashMap<>();

    Map<String, Long> numResultsPerEntity = searchResults.entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getNumEntities().longValue()));
    aggregations.put("entity", new AggregationMetadata().setName("entity")
        .setDisplayName("Type")
        .setAggregations(new LongMap(numResultsPerEntity))
        .setFilterValues(new FilterValueArray(SearchUtil.convertToFilters(numResultsPerEntity))));

    for (String entity : searchResults.keySet()) {
      SearchResult result = searchResults.get(entity);
      numEntities += result.getNumEntities();
      matchedResults.addAll(result.getEntities());
      // Merge filters
      result.getMetadata().getAggregations().forEach(metadata -> {
        if (aggregations.containsKey(metadata.getName())) {
          aggregations.put(metadata.getName(), SearchUtils.merge(aggregations.get(metadata.getName()), metadata));
        } else {
          aggregations.put(metadata.getName(), metadata);
        }
      });
    }

    // 4. Rank results across entities
    List<SearchEntity> rankedResult = _searchRanker.rank(matchedResults);
    SearchResultMetadata finalMetadata =
        new SearchResultMetadata().setAggregations(new AggregationMetadataArray(rankFilterGroups(aggregations)));

    postProcessTimer.stop();
    return new SearchResult().setEntities(new SearchEntityArray(rankedResult))
        .setNumEntities(numEntities)
        .setFrom(from)
        .setPageSize(size)
        .setMetadata(finalMetadata);
  }

  private SearchResult getEmptySearchResult(int from, int size) {
    return new SearchResult().setEntities(new SearchEntityArray())
        .setNumEntities(0)
        .setFrom(from)
        .setPageSize(size)
        .setMetadata(new SearchResultMetadata().setAggregations(new AggregationMetadataArray()));
  }

  @WithSpan
  private Map<String, SearchResult> getSearchResultsForEachEntity(@Nonnull List<String> entities, @Nonnull String input,
      @Nullable Filter postFilters, @Nullable SortCriterion sortCriterion, int queryFrom, int querySize,
      @Nullable SearchFlags searchFlags) {
    Map<String, SearchResult> searchResults;
    // Query the entity search service for all entities asynchronously
    try (Timer.Context ignored = MetricUtils.timer(this.getClass(), "searchEntities").time()) {
      searchResults = ConcurrencyUtils.transformAndCollectAsync(entities, entity -> new Pair<>(entity,
          _entitySearchServiceCache.getSearcher(entity, input, postFilters, sortCriterion, searchFlags)
              .getSearchResults(queryFrom, querySize)))
          .stream()
          .filter(pair -> pair.getValue().getNumEntities() > 0)
          .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }
    return searchResults;
  }
}
