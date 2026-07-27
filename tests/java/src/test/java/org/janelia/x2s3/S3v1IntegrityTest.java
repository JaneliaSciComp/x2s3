package org.janelia.x2s3;

import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;
import com.google.gson.JsonParser;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * Reproduces the "Unable to verify integrity of data download" error reported by
 * N5/Zarr readers (org.janelia.saalfeldlab.n5.s3), which use the AWS SDK v1 S3 client.
 * SDK v1 validates GetObject downloads by comparing a locally computed MD5 against the
 * ETag header -- unless the ETag is absent, looks like a multipart ETag (has a "-N"
 * suffix), or the object is SSE-C/SSE-KMS encrypted. That's a different code path than
 * the SDK v2 client used in {@link S3CompatTest}, which does not do this validation.
 *
 * Point this at the same x2s3 target/key that failed in Fiji/BigDataViewer to confirm
 * whether setting `proxy_etag: false` on that target (the default since this fix)
 * resolves the integrity error, and separately whether the downloaded bytes are
 * actually intact.
 *
 * Configure via env vars:
 *   PROXY_ENDPOINT (default http://localhost:8000)
 *   TEST_BUCKET    (default janelia-data-examples)
 *   TEST_KEY       (default jrc_mus_lung_covid.n5/attributes.json)
 */
public class S3v1IntegrityTest {

    private static AmazonS3 client;
    private static String bucket;
    private static String key;

    @BeforeClass
    public static void setup() {
        String endpoint = System.getenv().getOrDefault("PROXY_ENDPOINT", "http://localhost:8000");
        bucket = System.getenv().getOrDefault("TEST_BUCKET", "janelia-data-examples");
        key = System.getenv().getOrDefault("TEST_KEY", "jrc_mus_lung_covid.n5/attributes.json");

        client = AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, "us-east-1"))
                .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
                .withPathStyleAccessEnabled(true)
                .build();
    }

    @Test
    public void testGetObjectPassesIntegrityCheck() throws Exception {
        // This is the exact path that threw SdkClientException in Fiji: reading the
        // object body fully triggers DigestValidationInputStream's end-of-stream MD5
        // comparison against the ETag.
        S3Object obj = client.getObject(bucket, key);
        byte[] body;
        try {
            body = IOUtils.toByteArray(obj.getObjectContent());
        } finally {
            obj.close();
        }
        assertTrue("Body should not be empty", body.length > 0);
    }

    @Test
    public void testDownloadedJsonIsWellFormed() throws Exception {
        // Independent of the ETag check: verify the bytes we actually got are intact
        // and not truncated/corrupted, since that's a separate failure mode from the
        // ETag mismatch (and was seen in the Fiji trace as a MalformedJsonException).
        S3Object obj = client.getObject(bucket, key);
        byte[] body;
        try {
            body = IOUtils.toByteArray(obj.getObjectContent());
        } finally {
            obj.close();
        }
        String text = new String(body, StandardCharsets.UTF_8).trim();
        assertTrue("Response should look like JSON, got: " + text,
                text.startsWith("{") && text.endsWith("}"));
        // Throws JsonSyntaxException if malformed, same as the Zarr reader.
        JsonParser.parseString(text);
    }

    @Test
    public void testContentLengthMatchesActualBytes() throws Exception {
        ObjectMetadata meta = client.getObjectMetadata(bucket, key);
        S3Object obj = client.getObject(bucket, key);
        byte[] body;
        try {
            body = IOUtils.toByteArray(obj.getObjectContent());
        } finally {
            obj.close();
        }
        assertEquals("Downloaded byte count should match Content-Length",
                meta.getContentLength(), body.length);
    }

    @Test
    public void testReportedETagVsActualMd5() throws Exception {
        // Purely diagnostic: disable v1 SDK's own validation so a mismatch here prints
        // instead of throwing, to confirm whether the backend's ETag is a real content
        // MD5 at all.
        System.setProperty("com.amazonaws.services.s3.disableGetObjectMD5Validation", "true");
        try {
            ObjectMetadata meta = client.getObjectMetadata(bucket, key);
            String reportedETag = meta.getETag();

            S3Object obj = client.getObject(bucket, key);
            byte[] body;
            try {
                body = IOUtils.toByteArray(obj.getObjectContent());
            } finally {
                obj.close();
            }

            MessageDigest md5 = MessageDigest.getInstance("MD5");
            StringBuilder sb = new StringBuilder();
            for (byte b : md5.digest(body)) {
                sb.append(String.format("%02x", b & 0xff));
            }
            String actualMd5 = sb.toString();

            System.out.println("Reported ETag: " + reportedETag);
            System.out.println("Actual MD5:    " + actualMd5);
            System.out.println("Bytes read:    " + body.length);
        } finally {
            System.clearProperty("com.amazonaws.services.s3.disableGetObjectMD5Validation");
        }
    }
}
