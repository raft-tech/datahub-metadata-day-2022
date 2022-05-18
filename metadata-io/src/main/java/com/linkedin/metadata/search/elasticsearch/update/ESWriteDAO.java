package com.linkedin.metadata.search.elasticsearch.update;

import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.io.IOException;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;


@Slf4j
@RequiredArgsConstructor
public class ESWriteDAO {

  private final EntityRegistry entityRegistry;
  private final RestHighLevelClient searchClient;
  private final IndexConvention indexConvention;
  private final BulkProcessor bulkProcessor;

  /**
   * Updates or inserts the given search document.
   *
   * @param entityName name of the entity
   * @param document the document to update / insert
   * @param docId the ID of the document
   */
  public void upsertDocument(@Nonnull String entityName, @Nonnull String document, @Nonnull String docId) {
    final String indexName = indexConvention.getIndexName(entityRegistry.getEntitySpec(entityName));
    final IndexRequest indexRequest = new IndexRequest(indexName).id(docId).source(document, XContentType.JSON);
    final UpdateRequest updateRequest =
        new UpdateRequest(indexName, docId).doc(document, XContentType.JSON).detectNoop(false).upsert(indexRequest);
    bulkProcessor.add(updateRequest);
  }

  /**
   * Deletes the document with the given document ID from the index.
   *
   * @param entityName name of the entity
   * @param docId the ID of the document to delete
   */
  public void deleteDocument(@Nonnull String entityName, @Nonnull String docId) {
    final String indexName = indexConvention.getIndexName(entityRegistry.getEntitySpec(entityName));
    bulkProcessor.add(new DeleteRequest(indexName).id(docId));
  }

  /**
   * Clear all documents in all the indices
   */
  public void clear() {
    String[] indices = getIndices(indexConvention.getAllEntityIndicesPattern());
    DeleteByQueryRequest deleteRequest = new DeleteByQueryRequest(indices).setQuery(QueryBuilders.matchAllQuery());
    try {
      searchClient.deleteByQuery(deleteRequest, RequestOptions.DEFAULT);
    } catch (Exception e) {
      log.error("Failed to delete content of search indices: {}", e.toString());
    }
  }

  private String[] getIndices(String pattern) {
    try {
      GetIndexResponse response = searchClient.indices().get(new GetIndexRequest(pattern), RequestOptions.DEFAULT);
      return response.getIndices();
    } catch (IOException e) {
      log.error("Failed to get indices using pattern {}", pattern);
      return new String[]{};
    }
  }
}
