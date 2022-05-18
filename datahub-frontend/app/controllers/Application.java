package controllers;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.util.ByteString;
import auth.Authenticator;
import com.datahub.authentication.AuthenticationConstants;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.util.Pair;
import com.typesafe.config.Config;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import play.api.Play;
import play.http.HttpEntity;
import play.libs.ws.InMemoryBodyWritable;
import play.libs.ws.StandaloneWSClient;
import play.libs.Json;
import play.libs.ws.ahc.StandaloneAhcWSClient;
import play.mvc.Controller;
import play.mvc.Http;
import play.mvc.ResponseHeader;
import play.mvc.Result;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.io.InputStream;
import play.mvc.Security;
import play.shaded.ahc.org.asynchttpclient.AsyncHttpClient;
import play.shaded.ahc.org.asynchttpclient.AsyncHttpClientConfig;
import play.shaded.ahc.org.asynchttpclient.DefaultAsyncHttpClient;
import play.shaded.ahc.org.asynchttpclient.DefaultAsyncHttpClientConfig;
import utils.ConfigUtil;
import java.time.Duration;

import static auth.AuthUtils.*;


public class Application extends Controller {

  private final Config _config;
  private final StandaloneWSClient _ws;

  @Inject
  public Application(@Nonnull Config config) {
    _config = config;
    _ws = createWsClient();
  }

  /**
   * Serves the build output index.html for any given path
   *
   * @param path takes a path string, which essentially is ignored
   *             routing is managed client side
   * @return {Result} build output index.html resource
   */
  @Nonnull
  private Result serveAsset(@Nullable String path) {
    InputStream indexHtml = Play.current().classloader().getResourceAsStream("public/index.html");
    response().setHeader("Cache-Control", "no-cache");
    return ok(indexHtml).as("text/html");
  }

  @Nonnull
  public Result healthcheck() {
    return ok("GOOD");
  }

  /**
   * index Action proxies to serveAsset
   *
   * @param path takes a path string which is either index.html or the path segment after /
   * @return {Result} response from serveAsset method
   */
  @Nonnull
  public Result index(@Nullable String path) {
    return serveAsset("");
  }

  /**
   * Proxies requests to the Metadata Service
   *
   * TODO: Investigate using mutual SSL authentication to call Metadata Service.
   */
  @Security.Authenticated(Authenticator.class)
  public CompletableFuture<Result> proxy(String path) throws ExecutionException, InterruptedException {
    final String authorizationHeaderValue = getAuthorizationHeaderValueToProxy();
    final String resolvedUri = mapPath(request().uri());

    final String metadataServiceHost = ConfigUtil.getString(
        _config,
        ConfigUtil.METADATA_SERVICE_HOST_CONFIG_PATH,
        ConfigUtil.DEFAULT_METADATA_SERVICE_HOST);
    final int metadataServicePort = ConfigUtil.getInt(
        _config,
        ConfigUtil.METADATA_SERVICE_PORT_CONFIG_PATH,
        ConfigUtil.DEFAULT_METADATA_SERVICE_PORT);
    final boolean metadataServiceUseSsl = ConfigUtil.getBoolean(
        _config,
        ConfigUtil.METADATA_SERVICE_USE_SSL_CONFIG_PATH,
        ConfigUtil.DEFAULT_METADATA_SERVICE_USE_SSL
    );

    // TODO: Fully support custom internal SSL.
    final String protocol = metadataServiceUseSsl ? "https" : "http";

    return _ws.url(String.format("%s://%s:%s%s", protocol, metadataServiceHost, metadataServicePort, resolvedUri))
        .setMethod(request().method())
        .setHeaders(request()
            .getHeaders()
            .toMap()
            .entrySet()
            .stream()
            // Remove X-DataHub-Actor to prevent malicious delegation.
            .filter(entry -> !AuthenticationConstants.LEGACY_X_DATAHUB_ACTOR_HEADER.equals(entry.getKey()))
            .filter(entry -> !Http.HeaderNames.CONTENT_LENGTH.equals(entry.getKey()))
            .filter(entry -> !Http.HeaderNames.CONTENT_TYPE.equals(entry.getKey()))
            .filter(entry -> !Http.HeaderNames.AUTHORIZATION.equals(entry.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
        )
        .addHeader(Http.HeaderNames.AUTHORIZATION, authorizationHeaderValue)
        .addHeader(AuthenticationConstants.LEGACY_X_DATAHUB_ACTOR_HEADER, getDataHubActorHeader())
        .setBody(new InMemoryBodyWritable(ByteString.fromByteBuffer(request().body().asBytes().asByteBuffer()), "application/json"))
        .setRequestTimeout(Duration.ofSeconds(120))
        .execute()
        .thenApply(apiResponse -> {
          final ResponseHeader header = new ResponseHeader(apiResponse.getStatus(), apiResponse.getHeaders()
              .entrySet()
              .stream()
              .filter(entry -> !Http.HeaderNames.CONTENT_LENGTH.equals(entry.getKey()))
              .filter(entry -> !Http.HeaderNames.CONTENT_TYPE.equals(entry.getKey()))
              .map(entry -> Pair.of(entry.getKey(), String.join(";", entry.getValue())))
              .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond)));
          final HttpEntity body = new HttpEntity.Strict(apiResponse.getBodyAsBytes(), Optional.ofNullable(apiResponse.getContentType()));
          return new Result(header, body);
        }).toCompletableFuture();
  }

  /**
   * Creates a wrapping ObjectNode containing config information
   *
   * @return Http Result instance with app configuration attributes
   */
  @Nonnull
  public Result appConfig() {
    final ObjectNode config = Json.newObject();
    config.put("application", "datahub-frontend");
    config.put("appVersion", _config.getString("app.version"));
    config.put("isInternal", _config.getBoolean("linkedin.internal"));
    config.put("shouldShowDatasetLineage", _config.getBoolean("linkedin.show.dataset.lineage"));
    config.put("suggestionConfidenceThreshold",
        Integer.valueOf(_config.getString("linkedin.suggestion.confidence.threshold")));
    config.set("wikiLinks", wikiLinks());
    config.set("tracking", trackingInfo());
    // In a staging environment, we can trigger this flag to be true so that the UI can handle based on
    // such config and alert users that their changes will not affect production data
    config.put("isStagingBanner", _config.getBoolean("ui.show.staging.banner"));
    config.put("isLiveDataWarning", _config.getBoolean("ui.show.live.data.banner"));
    config.put("showChangeManagement", _config.getBoolean("ui.show.CM.banner"));
    // Flag to enable people entity elements
    config.put("showPeople", _config.getBoolean("ui.show.people"));
    config.put("changeManagementLink", _config.getString("ui.show.CM.link"));
    // Flag set in order to warn users that search is experiencing issues
    config.put("isStaleSearch", _config.getBoolean("ui.show.stale.search"));
    config.put("showAdvancedSearch", _config.getBoolean("ui.show.advanced.search"));
    // Flag to use the new api for browsing datasets
    config.put("useNewBrowseDataset", _config.getBoolean("ui.new.browse.dataset"));
    // show lineage graph in relationships tabs
    config.put("showLineageGraph", _config.getBoolean("ui.show.lineage.graph"));
    // show institutional memory for available entities
    config.put("showInstitutionalMemory", _config.getBoolean("ui.show.institutional.memory"));

    // Insert properties for user profile operations
    config.set("userEntityProps", userEntityProps());

    final ObjectNode response = Json.newObject();
    response.put("status", "ok");
    response.set("config", config);
    return ok(response);
  }

  /**
   * Creates a JSON object of profile / avatar properties
   * @return Json avatar / profile image properties
   */
  @Nonnull
  private ObjectNode userEntityProps() {
    final ObjectNode props = Json.newObject();
    props.put("aviUrlPrimary", _config.getString("linkedin.links.avi.urlPrimary"));
    props.put("aviUrlFallback", _config.getString("linkedin.links.avi.urlFallback"));
    return props;
  }

  /**
   * @return Json object with internal wiki links
   */
  @Nonnull
  private ObjectNode wikiLinks() {
    final ObjectNode wikiLinks = Json.newObject();
    wikiLinks.put("appHelp", _config.getString("links.wiki.appHelp"));
    wikiLinks.put("gdprPii", _config.getString("links.wiki.gdprPii"));
    wikiLinks.put("tmsSchema", _config.getString("links.wiki.tmsSchema"));
    wikiLinks.put("gdprTaxonomy", _config.getString("links.wiki.gdprTaxonomy"));
    wikiLinks.put("staleSearchIndex", _config.getString("links.wiki.staleSearchIndex"));
    wikiLinks.put("dht", _config.getString("links.wiki.dht"));
    wikiLinks.put("purgePolicies", _config.getString("links.wiki.purgePolicies"));
    wikiLinks.put("jitAcl", _config.getString("links.wiki.jitAcl"));
    wikiLinks.put("metadataCustomRegex", _config.getString("links.wiki.metadataCustomRegex"));
    wikiLinks.put("exportPolicy", _config.getString("links.wiki.exportPolicy"));
    wikiLinks.put("metadataHealth", _config.getString("links.wiki.metadataHealth"));
    wikiLinks.put("purgeKey", _config.getString("links.wiki.purgeKey"));
    wikiLinks.put("datasetDecommission", _config.getString("links.wiki.datasetDecommission"));
    return wikiLinks;
  }

  /**
   * @return Json object containing the tracking configuration details
   */
  @Nonnull
  private ObjectNode trackingInfo() {
    final ObjectNode piwik = Json.newObject();
    piwik.put("piwikSiteId", Integer.valueOf(_config.getString("tracking.piwik.siteid")));
    piwik.put("piwikUrl", _config.getString("tracking.piwik.url"));

    final ObjectNode trackers = Json.newObject();
    trackers.set("piwik", piwik);

    final ObjectNode trackingConfig = Json.newObject();
    trackingConfig.set("trackers", trackers);
    trackingConfig.put("isEnabled", true);
    return trackingConfig;
  }

  private StandaloneWSClient createWsClient() {
    final String name = "proxyClient";
    ActorSystem system = ActorSystem.create(name);
    system.registerOnTermination(() -> System.exit(0));
    Materializer materializer = ActorMaterializer.create(system);
    AsyncHttpClientConfig asyncHttpClientConfig =
        new DefaultAsyncHttpClientConfig.Builder()
            .setMaxRequestRetry(0)
            .setShutdownQuietPeriod(0)
            .setShutdownTimeout(0)
            .build();
    AsyncHttpClient asyncHttpClient = new DefaultAsyncHttpClient(asyncHttpClientConfig);
    return new StandaloneAhcWSClient(asyncHttpClient, materializer);
  }

  /**
   * Returns the value of the Authorization Header to be provided when proxying requests to the downstream Metadata Service.
   *
   * Currently, the Authorization header value may be derived from
   *
   * a) The value of the "token" attribute of the Session Cookie provided by the client. This value is set
   * when creating the session token initially from a token granted by the Metadata Service.
   *
   * Or if the "token" attribute cannot be found in a session cookie, then we fallback to
   *
   * b) The value of the Authorization
   * header provided in the original request. This will be used in cases where clients are making programmatic requests
   * to Metadata Service APIs directly, without providing a session cookie (ui only).
   *
   * If neither are found, an empty string is returned.
   */
  private String getAuthorizationHeaderValueToProxy() {
    // If the session cookie has an authorization token, use that. If there's an authorization header provided, simply
    // use that.
    String value = "";
    if (ctx().session().containsKey(SESSION_COOKIE_GMS_TOKEN_NAME)) {
      value = "Bearer " + ctx().session().get(SESSION_COOKIE_GMS_TOKEN_NAME);
    } else if (request().getHeaders().contains(Http.HeaderNames.AUTHORIZATION)) {
      value = request().getHeaders().get(Http.HeaderNames.AUTHORIZATION).get();
    }
    return value;
  }

  /**
   * Returns the value of the legacy X-DataHub-Actor header to forward to the Metadata Service. This is sent along
   * with any requests that have a valid frontend session cookie to identify the calling actor, for backwards compatibility.
   *
   * If Metadata Service authentication is enabled, this value is not required because Actor context will most often come
   * from the authentication credentials provided in the Authorization header.
   */
  private String getDataHubActorHeader() {
    String actor = ctx().session().get(ACTOR);
    return actor == null ? "" : actor;
  }

  private String mapPath(@Nonnull final String path) {
    // Case 1: Map legacy GraphQL path to GMS GraphQL API (for compatibility)
    if (path.equals("/api/v2/graphql")) {
      return "/api/graphql";
    }

    // Case 2: Map requests to /gms to / (Rest.li API)
    final String gmsApiPath = "/api/gms";
    if (path.startsWith(gmsApiPath)) {
      return String.format("%s", path.substring(gmsApiPath.length()));
    }

    // Otherwise, return original path
    return path;
  }
}
