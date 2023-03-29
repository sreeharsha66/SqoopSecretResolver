package com.eca;

import org.apache.commons.logging.LogFactory;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import com.google.gson.JsonObject;
import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.gson.JsonParser;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;

import java.util.Arrays;
import java.util.regex.Matcher;
import org.apache.sqoop.Sqoop;
import org.apache.commons.logging.Log;
import java.util.regex.Pattern;

public class SqoopSecretResolver
{
    public static Pattern p;
    public static final Log LOG;

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("Try 'sqoop help' for usage.");
            System.exit(1);
        }
        final String defaultProjectId = getProjectId();
        SqoopSecretResolver.LOG.info("Using defaultProjectId: " + defaultProjectId);
        SqoopSecretResolver.LOG.info("Resolving secrets...");
        SqoopSecretResolver.LOG.debug(Arrays.toString(args));
        args = resolveSecrets(args);
        SqoopSecretResolver.LOG.debug(Arrays.toString(args));
        SqoopSecretResolver.LOG.info("Completed secrets resolution: ");
        SqoopSecretResolver.LOG.info("Invoking Sqoop");
        final int ret = Sqoop.runTool(args);
        SqoopSecretResolver.LOG.info("Completed Sqoop job with status: " + ret);
        System.exit(ret);
    }

    private static String[] resolveSecrets(final String[] args) {
        for (int i = 0; i < args.length; ++i) {
            if (args[i].contains("sm://")) {
                SqoopSecretResolver.LOG.debug(args[i]);
                final Matcher m = SqoopSecretResolver.p.matcher(args[i]);
                if (m.find()) {
                    SqoopSecretResolver.LOG.debug("Match found");
                    final String secretEntry = m.group(0);
                    final String[] secretEntryParts = secretEntry.replaceAll("sm://", "").split("/");
                    String json_key = null;
                    String secret;
                    SqoopSecretResolver.LOG.debug(secretEntryParts.length);
                    if (secretEntryParts.length == 2) {
                        secret = secretEntryParts[0].trim();
                        json_key = secretEntryParts[1].trim();
                    }
                    else {
                        if (secretEntryParts.length != 1) {
                            throw new IllegalArgumentException("Invalid secret entry: " + args[i]);
                        }
                        secret = secretEntryParts[0].trim();
                    }
                    try {
                        SqoopSecretResolver.LOG.debug("Secret " + secret + " json_key " + json_key);
                        final String value = getSecret(null, secret, json_key);
                        SqoopSecretResolver.LOG.debug("Secret value is " + value);
                        args[i] = args[i].replaceAll(secretEntry, value);
                        SqoopSecretResolver.LOG.debug("Replace value is " + args[i]);
                    }
                    catch (Exception e) {
                        System.err.println("Failed to get secret from secretmanager due to " + e.getMessage());
                        System.exit(1);
                    }
                }
            }
        }
        return args;
    }

    private static String getSecret(String projectId, final String secret, final String json_key) throws Exception {
        try {
            projectId = ((projectId != null) ? projectId : getProjectId());
            final SecretManagerServiceClient client = SecretManagerServiceClient.create();
            final SecretVersionName secretVersionName = SecretVersionName.of(projectId, secret, "latest");
            final AccessSecretVersionResponse response = client.accessSecretVersion(secretVersionName);
            String responseStr = response.getPayload().getData().toStringUtf8();
            if (json_key != null) {
                try {
                    final JsonObject jsonObject = new JsonParser().parse(responseStr).getAsJsonObject();
                    responseStr = jsonObject.get(json_key).getAsString();
                }
                catch (Exception e2) {
                    throw new Exception("Failed to parse json response: " + responseStr + ", for the key: " + json_key);
                }
            }
            return responseStr;
        }
        catch (Exception e) {
            throw new Exception("Failed to retrieve secret " + secret + ", due to exception: " + e.getMessage());
        }
    }

    private static String getProjectId() {
        String projectId = null;
        HttpURLConnection conn = null;
        try {
            final URL url = new URL("http://metadata.google.internal/computeMetadata/v1/project/project-id");
            conn = (HttpURLConnection)url.openConnection();
            conn.setRequestProperty("Metadata-Flavor", "Google");
            final InputStream is = conn.getInputStream();
            final byte[] targetArray = new byte[is.available()];
            is.read(targetArray);
            projectId = new String(targetArray);
            conn.disconnect();
        }
        catch (Throwable t) {}
        return projectId;
    }

    static {
        SqoopSecretResolver.p = Pattern.compile("sm:\\/\\/.*$");
        LOG = LogFactory.getLog(SqoopSecretResolver.class.getName());
    }
}