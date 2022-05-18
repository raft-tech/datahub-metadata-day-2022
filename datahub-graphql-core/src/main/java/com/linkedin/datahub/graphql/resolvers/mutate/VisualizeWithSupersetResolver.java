package com.linkedin.datahub.graphql.resolvers.mutate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.TupleKey;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.VisualizeWithSupersetInput;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.ebeaninternal.server.type.ScalarTypeJsonMapPostgres;
import io.vavr.Tuple2;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

@Slf4j
@RequiredArgsConstructor
public class VisualizeWithSupersetResolver implements DataFetcher<CompletableFuture<String>> {
    private static final Logger _logger = LoggerFactory.getLogger(MutableTypeResolver.class.getName());
    private static final String SUPERSET_PASSWORD = "SUPERSET_PASSWORD";
    private static final String SUPERSET_USERNAME = "SUPERSET_USERNAME";
    private static final String SUPERSET_PROVIDER = "SUPERSET_PROVIDER";
    private static final String SUPERSET_ENDPOINT = "SUPERSET_ENDPOINT";
    private static final String SUPERSET_EXTERNAL_ENDPOINT = "SUPERSET_EXTERNAL_ENDPOINT";

    private static final String DATASOURCE_HOST_AND_PORT = "DATASOURCE_HOST_AND_PORT";

    private static final String DATASOURCE_PASSWORD = "DATASOURCE_PASSWORD";
    private static final String DATASOURCE_USERNAME = "DATASOURCE_USERNAME";

    private static final String SUPERSET_AUTH_ROUTE = "/api/v1/security/login";
    private static final String SUPERSET_CSRF_ROUTE = "/api/v1/security/csrf_token";
    private static final String SUPERSET_CREATE_DATABASE_ROUTE = "/api/v1/database/";

    private static final String SUPERSET_CREATE_SAVED_QUERY_ROUTE = "/api/v1/saved_query/";

    private static final String SUPERSET_EXECUTE_SAVED_QUERY_ROUTE = "/superset/sqllab/?savedQueryId=";

    @Override
    public CompletableFuture<String> get(DataFetchingEnvironment environment) throws Exception {
        final VisualizeWithSupersetInput input = bindArgument(environment.getArgument("input"), VisualizeWithSupersetInput.class);
        Urn targetUrn = Urn.createFromString(input.getQualifiedName());

        _logger.info("[DATA FABRIC] Visualizing with Superset `" + input.getName() + "`.");
//        return CompletableFuture.supplyAsync(() -> {return null;});
        return CompletableFuture.supplyAsync(() -> {
            String supersetUrl = System.getenv(SUPERSET_ENDPOINT);
            if (supersetUrl == null) {
                log.error("Environment variable `" + SUPERSET_ENDPOINT + "` was not found.");
                return "";
            }

            String supersetExternalUrl = System.getenv(SUPERSET_EXTERNAL_ENDPOINT);
            if (supersetExternalUrl == null) {
                log.error("Environment variable `" + SUPERSET_EXTERNAL_ENDPOINT + "` was not found.");
                return "";
            }

            String supersetUser = System.getenv(SUPERSET_USERNAME);
            if (supersetUser == null) {
                log.error("Environment variable `" + SUPERSET_USERNAME + "` was not found.");
                return "";
            }

            String supersetPassword = System.getenv(SUPERSET_PASSWORD);
            if (supersetPassword == null) {
                log.error("Environment variable `" + SUPERSET_PASSWORD + "` was not found.");
                return "";
            }

            String supersetProvider = System.getenv(SUPERSET_PROVIDER);
            if (supersetProvider == null) {
                log.error("Environment variable `" + SUPERSET_PROVIDER + "` was not found.");
                return "";
            }

            String datasourceHostAndPort = System.getenv(DATASOURCE_HOST_AND_PORT);
            if (datasourceHostAndPort == null) {
                log.error("Environment variable `" + DATASOURCE_HOST_AND_PORT + "` was not found.");
                return "";
            }

            String datasourceUsername = System.getenv(DATASOURCE_USERNAME);
            if (datasourceUsername == null) {
                log.error("Environment variable `" + DATASOURCE_USERNAME + "` was not found.");
                return "";
            }

            String datasourcePassword = System.getenv(DATASOURCE_PASSWORD);
            if (datasourcePassword == null) {
                log.error("Environment variable `" + DATASOURCE_PASSWORD + "` was not found.");
                return "";
            }
            try {
                // First, extract the relevant information from the URN
                // TODO: This should become more generic than it is
                String datasetUrnStr = targetUrn.getEntityKey().toString().replace("(", "").replace(")", "");
                String[] datasetUrnPieces = datasetUrnStr.split(":");
                String[] datasetUrnInfo = datasetUrnPieces[datasetUrnPieces.length - 1].split(",");
                String dataPlatform = datasetUrnInfo[0];
                String datasetNameRaw = datasetUrnInfo[1];
                String[] datasetNameParts = datasetNameRaw.split("\\.");
                log.info(datasetNameRaw);
                String datasetSourceName = datasetNameParts[0];
                String datasetSchema = datasetNameParts[1];
                String datasetName = datasetNameParts[2];

                // First, get the auth token from Superset
                String accessToken = getSupersetAccessToken(supersetUrl, supersetUser, supersetPassword, supersetProvider);
                // Get the CSRF token and session header from Superset and extract them
                Tuple2<String, String> csrfAndSession = getCSRFToken(supersetUrl, accessToken);
                log.info(csrfAndSession._1);
                log.info(csrfAndSession._2);

                // Create the database in Superset (it may already exist)
                long id = createDatabase(supersetUrl, accessToken, csrfAndSession._1, csrfAndSession._2, dataPlatform, datasetSourceName, datasourceUsername, datasourcePassword, datasourceHostAndPort);
                log.info(String.valueOf(id));

                // Create the saved query in Superset
                String savedQueryUrl = createSavedQuery(supersetUrl, supersetExternalUrl, accessToken, csrfAndSession._1, csrfAndSession._2, targetUrn, id, datasetName, datasetSchema);
                log.info(savedQueryUrl);

                return savedQueryUrl;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public String getSupersetAccessToken(String supersetUrl, String supersetUser, String supersetPassword, String supersetProvider) throws Exception {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpPost request = new HttpPost(supersetUrl + SUPERSET_AUTH_ROUTE);
        SupersetCredentials params = new SupersetCredentials(supersetUser, supersetPassword, supersetProvider, true);
        request.setHeader(new BasicHeader("Content-Type", "application/json"));
        request.setEntity(new StringEntity(params.toString()));
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200) {
                log.info("Access Token Request successful.");
                JSONParser parser = new JSONParser();
                JSONObject obj = (JSONObject) parser.parse(EntityUtils.toString(response.getEntity()));
                String accessToken = (String) obj.get("access_token");
                log.info(accessToken);
                log.info(Arrays.toString(response.getAllHeaders()));
                return accessToken;
            } else {
                log.error("Access Token Request failed.");
                log.error(Arrays.toString(response.getAllHeaders()));
                log.error(EntityUtils.toString(response.getEntity()));
                throw new Exception("Access Token Request failed.");
            }
        } catch (UnsupportedEncodingException e) {
            log.error("Failed to execute request to `" + supersetUrl + SUPERSET_AUTH_ROUTE + "`");
            log.error(e.toString());
            throw new Exception("Request failed.");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Tuple2<String, String> getCSRFToken(String supersetUrl, String accessToken) throws Exception {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet request = new HttpGet(supersetUrl + SUPERSET_CSRF_ROUTE);
        request.setHeader(new BasicHeader("Authorization", "Bearer " + accessToken));
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200) {
                log.info("Request successful.");
                JSONParser parser = new JSONParser();
                JSONObject obj = (JSONObject) parser.parse(EntityUtils.toString(response.getEntity()));
                String csrfToken = (String) obj.get("result");
                log.info(csrfToken);

                Header header = response.getHeaders("Set-Cookie")[0];
                log.info(header.getValue());
                String[] parts = header.getValue().split(";");
                String sessionHeader = parts[0].replace("session=", "");
                return new Tuple2<>(csrfToken, sessionHeader);
            } else {
                log.error("Status code " + statusCode);
                throw new Exception("Request failed.");
            }
        } catch (Exception e) {
            log.error("Failed to execute request to `" + supersetUrl + SUPERSET_CSRF_ROUTE + "`");
            log.error(e.toString());
            throw new RuntimeException(e);
        }
    }

    public long createDatabase(String supersetUrl, String accessToken, String csrfToken, String session, String dataPlatform, String datasetSource, String datasourceUsername, String datasourcePassword, String datasourceHostAndPort) throws Exception {
        // TODO: In production, add support for non-generic databases
        log.info("Access token:" + accessToken);
        log.info("csrf: " + csrfToken);
        log.info("session: " + session);
        JSONObject obj = new JSONObject();
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpPost request = new HttpPost(supersetUrl + SUPERSET_CREATE_DATABASE_ROUTE);
        request.setHeader(new BasicHeader("Authorization", "Bearer " + accessToken));
        request.setHeader(new BasicHeader("Cookie", "session=" + session));
        request.setHeader(new BasicHeader("X-CSRFToken", csrfToken));
        request.setHeader(new BasicHeader("Content-Type", "application/json"));

        //TODO: Make this non-hardcoded

        obj.put("database_name", "df-postgres");
        obj.put("engine", dataPlatform);
        obj.put("configuration_method", "sqlalchemy_form");
        if (datasourcePassword.equals("")) {
            obj.put("sqlalchemy_uri", dataPlatform + "://" + datasourceUsername + "@" + datasourceHostAndPort + "/" + datasetSource);
        } else {
            obj.put("sqlalchemy_uri", dataPlatform + "://" + datasourceUsername + ":" + datasourcePassword + "@" + datasourceHostAndPort + "/df-postgres");
        }
        ObjectMapper mapper = new ObjectMapper();
        log.info("ObjectMapper: " + mapper.writeValueAsString(obj));
        log.info("Json simple: " + obj.toJSONString());
        try {
//            request.setEntity(new StringEntity(obj.toJSONString()));
            request.setEntity(new StringEntity(mapper.writeValueAsString(obj)));
            CloseableHttpResponse response = httpClient.execute(request);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 201) {
                log.info("Request to create database successful.");
                JSONParser parser = new JSONParser();
                JSONObject responseObj = (JSONObject) parser.parse(EntityUtils.toString(response.getEntity()));
                long id = (long) responseObj.get("id");
                return id;
            } else if (statusCode == 422) {
                log.info("Returned a 422. Database may already exist.");
                long maybeDatabaseId = findDatabaseId(supersetUrl, accessToken, csrfToken, session, datasetSource);
                if (maybeDatabaseId == -1L) {
                    throw new Exception("Could not create database; failed to find if it currently exists.");
                }
                return maybeDatabaseId;
            } else {
                log.error("Status code " + statusCode);
                throw new Exception("Request failed.");
            }
        } catch (Exception e) {
            log.error("Failed to execute request to `" + supersetUrl + SUPERSET_CREATE_DATABASE_ROUTE + "`");
            log.error(e.toString());
            throw new RuntimeException(e);
        }
    }

    public long findDatabaseId(String supersetUrl, String accessToken, String csrfToken, String session, String datasetSource) {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet request = new HttpGet(supersetUrl + SUPERSET_CREATE_DATABASE_ROUTE);
        request.setHeader(new BasicHeader("Authorization", "Bearer " + accessToken));
        request.setHeader(new BasicHeader("Cookie", "session=" + session));
        request.setHeader(new BasicHeader("X-CSRFToken", csrfToken));
        try {
            CloseableHttpResponse response = httpClient.execute(request);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200) {
                log.info("Request to list databases successful.");
                JSONParser parser = new JSONParser();
                JSONObject responseObj = (JSONObject) parser.parse(EntityUtils.toString(response.getEntity()));
                JSONArray results = (JSONArray) responseObj.get("result");
                for (Object result : results) {
                    JSONObject res = (JSONObject) result;
                    if (res.get("database_name").equals(datasetSource)) {
                        return (long) res.get("id");
                    }
                }
                return -1L;
            } else {
                log.error("Status code " + statusCode);
                log.error("Could not list databases.");
                return -1L;
            }
        } catch (Exception e) {
            log.error("Failed to execute request to `" + supersetUrl + SUPERSET_CREATE_DATABASE_ROUTE + "`");
            log.error(e.toString());
            return -1L;
        }

    }

    public String createSavedQuery(String supersetUrl, String supersetExternalUrl, String accessToken, String csrfToken, String session, Urn urn, long databaseId, String datasetName, String datasetSchema) {
        JSONObject obj = new JSONObject();
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpPost request = new HttpPost(supersetUrl + SUPERSET_CREATE_SAVED_QUERY_ROUTE);
        request.setHeader(new BasicHeader("Authorization", "Bearer " + accessToken));
        request.setHeader(new BasicHeader("Cookie", "session=" + session));
        request.setHeader(new BasicHeader("X-CSRFToken", csrfToken));
        request.setHeader(new BasicHeader("Content-Type", "application/json"));

        obj.put("db_id", databaseId);
        obj.put("schema", datasetSchema);
        obj.put("sql", "SELECT * FROM " + datasetName + " LIMIT 100");
        obj.put("label", datasetName + "-query");
        obj.put("description", "Auto-generated data product from Datahub.");

        try {
            request.setEntity(new StringEntity(obj.toJSONString()));
            CloseableHttpResponse response = httpClient.execute(request);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 201) {
                log.info("Request to create saved query successful.");
                JSONParser parser = new JSONParser();
                JSONObject responseObj = (JSONObject) parser.parse(EntityUtils.toString(response.getEntity()));
                long queryId = (long) responseObj.get("id");
                return supersetExternalUrl + SUPERSET_EXECUTE_SAVED_QUERY_ROUTE + queryId;
            } else {
                log.error("Status code " + statusCode);
                throw new Exception("Request failed.");
            }
        } catch (Exception e) {
            log.error("Failed to execute request to `" + supersetUrl + SUPERSET_CREATE_SAVED_QUERY_ROUTE + "`");
            log.error(e.toString());
            throw new RuntimeException(e);
        }
    }
}


@RequiredArgsConstructor
class SupersetCredentials {
    public final String _username;
    public final String _password;
    public final String _provider;
    public final boolean _refresh;

    @Override
    public String toString() {
        return "{" +
                "\"username\": \"" + _username + "\"," +
                "\"password\": \"" + _password + "\"," +
                "\"provider\": \"" + _provider + "\"," +
                "\"refresh\": \"" + _refresh + "\"" +
                "}";
    }
}