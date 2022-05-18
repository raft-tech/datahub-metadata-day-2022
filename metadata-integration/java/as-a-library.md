# Java Emitter

In some cases, you might want to construct Metadata events directly and use programmatic ways to emit that metadata to DataHub. Use-cases are typically push-based and include emitting metadata events from CI/CD pipelines, custom orchestrators etc.

The [`io.acryl:datahub-client`](https://mvnrepository.com/artifact/io.acryl/datahub-client) Java package offers REST emitter API-s, which can be easily used to emit metadata from your JVM-based systems. For example, the Spark lineage integration uses the Java emitter to emit metadata events from Spark jobs.


## Installation

Follow the specific instructions for your build system to declare a dependency on the appropriate version of the package. 

**_Note_**: Check the [Maven repository](https://mvnrepository.com/artifact/io.acryl/datahub-client) for the latest version of the package before following the instructions below.

### Gradle
Add the following to your build.gradle.
```gradle
implementation 'io.acryl:datahub-client:0.0.1'
```
### Maven
Add the following to your `pom.xml`.
```xml
<!-- https://mvnrepository.com/artifact/io.acryl/datahub-client -->
<dependency>
    <groupId>io.acryl</groupId>
    <artifactId>datahub-client</artifactId>
    <!-- replace with the latest version number -->
    <version>0.0.1</version>
</dependency>
```

## REST Emitter

The REST emitter is a thin wrapper on top of the [`Apache HttpClient`](https://hc.apache.org/httpcomponents-client-4.5.x/index.html) library. It supports non-blocking emission of metadata and handles the details of JSON serialization of metadata aspects over the wire.

Constructing a REST Emitter follows a lambda-based fluent builder pattern. The config parameters mirror the Python emitter [configuration](../../metadata-ingestion/sink_docs/datahub.md#config-details) for the most part. In addition, you can also customize the HttpClient that is constructed under the hood by passing in customizations to the HttpClient builder.
```java
import datahub.client.rest.RestEmitter;
//...
RestEmitter emitter = RestEmitter.create(b -> b
                                              .server("http://localhost:8080")
//Auth token for Managed DataHub              .token(AUTH_TOKEN_IF_NEEDED)
//Override default timeout of 10 seconds      .timeoutSec(OVERRIDE_DEFAULT_TIMEOUT_IN_SECONDS)
//Add additional headers                      .extraHeaders(Collections.singletonMap("Session-token", "MY_SESSION"))
// Customize HttpClient's connection ttl      .customizeHttpAsyncClient(c -> c.setConnectionTimeToLive(30, TimeUnit.SECONDS))
                                    );
```

### Usage

```java
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.events.metadata.ChangeType;
import datahub.event.MetadataChangeProposalWrapper;
import datahub.client.rest.RestEmitter;
import datahub.client.Callback;
// ... followed by

// Creates the emitter with the default coordinates and settings
RestEmitter emitter = RestEmitter.createWithDefaults(); 

MetadataChangeProposalWrapper mcpw = MetadataChangeProposalWrapper.builder()
        .entityType("dataset")
        .entityUrn("urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.my-dataset.user-table,PROD)")
        .upsert()
        .aspect(new DatasetProperties().setDescription("This is the canonical User profile dataset"))
        .build();

// Blocking call using future
Future<MetadataWriteResponse> requestFuture = emitter.emit(mcpw, null).get();

// Non-blocking using callback
emitter.emit(mcpw, new Callback() {
      @Override
      public void onCompletion(MetadataWriteResponse response) {
        if (response.isSuccess()) {
          System.out.println(String.format("Successfully emitted metadata event for %s", mcpw.getEntityUrn()));
        } else {
          // Get the underlying http response
          HttpResponse httpResponse = (HttpResponse) response.getUnderlyingResponse();
          System.out.println(String.format("Failed to emit metadata event for %s, aspect: %s with status code: %d",
              mcpw.getEntityUrn(), mcpw.getAspectName(), httpResponse.getStatusLine().getStatusCode()));
          // Print the server side exception if it was captured
          if (response.getServerException() != null) {
            System.out.println(String.format("Server side exception was %s", response.getServerException()));
          }
        }
      }

      @Override
      public void onFailure(Throwable exception) {
        System.out.println(
            String.format("Failed to emit metadata event for %s, aspect: %s due to %s", mcpw.getEntityUrn(),
                mcpw.getAspectName(), exception.getMessage()));
      }
    });
```

### Emitter Code

If you're interested in looking at the REST emitter code, it is available [here](./datahub-client/src/main/java/datahub/client/rest/RestEmitter.java).

## Kafka Emitter

The Java package doesn't currently support a Kafka emitter, but this will be available shortly.


## Other Languages

Emitter API-s are also supported for:
- [Python](../../metadata-ingestion/as-a-library.md)


