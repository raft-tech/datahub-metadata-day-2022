package com.linkedin.gms.factory.search;

import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.gms.factory.common.RestHighLevelClientFactory;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import javax.annotation.Nonnull;
import lombok.Value;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;


/**
 * Factory for components required for any services using elasticsearch
 */
@Configuration
@Import({RestHighLevelClientFactory.class, IndexConventionFactory.class, ElasticSearchBulkProcessorFactory.class,
    ElasticSearchIndexBuilderFactory.class})
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class BaseElasticSearchComponentsFactory {
  @Value
  public static class BaseElasticSearchComponents {
    RestHighLevelClient searchClient;
    IndexConvention indexConvention;
    BulkProcessor bulkProcessor;
    ESIndexBuilder indexBuilder;
  }

  @Autowired
  @Qualifier("elasticSearchRestHighLevelClient")
  private RestHighLevelClient searchClient;

  @Autowired
  @Qualifier(IndexConventionFactory.INDEX_CONVENTION_BEAN)
  private IndexConvention indexConvention;

  @Autowired
  @Qualifier("elasticSearchBulkProcessor")
  private BulkProcessor bulkProcessor;

  @Autowired
  @Qualifier("elasticSearchIndexBuilder")
  private ESIndexBuilder indexBuilder;

  @Bean(name = "baseElasticSearchComponents")
  @Nonnull
  protected BaseElasticSearchComponents getInstance() {
    return new BaseElasticSearchComponents(searchClient, indexConvention, bulkProcessor, indexBuilder);
  }
}
