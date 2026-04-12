package org.janelia.x2s3;

import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.*;

import java.net.URI;
import java.util.List;

/**
 * S3 compatibility tests that run the same operations against both
 * real AWS S3 and the x2s3 proxy, comparing the results.
 *
 * Set the PROXY_ENDPOINT environment variable to override the default proxy URL.
 */
public class S3CompatTest {

    private static final String BUCKET = "janelia-data-examples";
    private static final String DEFAULT_PROXY = "https://nextflow.int.janelia.org:8003";

    private static S3Client awsClient;
    private static S3Client proxyClient;

    @BeforeClass
    public static void setup() {
        awsClient = S3Client.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .build();

        String proxyEndpoint = System.getenv("PROXY_ENDPOINT");
        if (proxyEndpoint == null) {
            proxyEndpoint = DEFAULT_PROXY;
        }

        proxyClient = S3Client.builder()
                .region(Region.US_EAST_1)
                .endpointOverride(URI.create(proxyEndpoint))
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true)
                        .build())
                .build();
    }

    @Test
    public void testListObjectsV2Basic() {
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(BUCKET)
                .maxKeys(5)
                .build();

        ListObjectsV2Response awsResp = awsClient.listObjectsV2(request);
        ListObjectsV2Response proxyResp = proxyClient.listObjectsV2(request);

        assertEquals("Status should match", 200, proxyResp.sdkHttpResponse().statusCode());
        assertEquals("Bucket name", awsResp.name(), proxyResp.name());
        assertEquals("MaxKeys", awsResp.maxKeys(), proxyResp.maxKeys());
        assertEquals("KeyCount", awsResp.keyCount(), proxyResp.keyCount());
        assertEquals("IsTruncated", awsResp.isTruncated(), proxyResp.isTruncated());

        // Compare object keys
        List<S3Object> awsObjects = awsResp.contents();
        List<S3Object> proxyObjects = proxyResp.contents();
        assertEquals("Object count", awsObjects.size(), proxyObjects.size());

        for (int i = 0; i < awsObjects.size(); i++) {
            assertEquals("Key[" + i + "]", awsObjects.get(i).key(), proxyObjects.get(i).key());
            assertEquals("Size[" + i + "]", awsObjects.get(i).size(), proxyObjects.get(i).size());
            assertEquals("ETag[" + i + "]", awsObjects.get(i).eTag(), proxyObjects.get(i).eTag());
            assertEquals("StorageClass[" + i + "]",
                    awsObjects.get(i).storageClassAsString(),
                    proxyObjects.get(i).storageClassAsString());
        }
    }

    @Test
    public void testListObjectsV2WithDelimiter() {
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(BUCKET)
                .delimiter("/")
                .build();

        ListObjectsV2Response awsResp = awsClient.listObjectsV2(request);
        ListObjectsV2Response proxyResp = proxyClient.listObjectsV2(request);

        assertEquals("Delimiter", awsResp.delimiter(), proxyResp.delimiter());
        assertEquals("CommonPrefixes count",
                awsResp.commonPrefixes().size(),
                proxyResp.commonPrefixes().size());

        for (int i = 0; i < awsResp.commonPrefixes().size(); i++) {
            assertEquals("CommonPrefix[" + i + "]",
                    awsResp.commonPrefixes().get(i).prefix(),
                    proxyResp.commonPrefixes().get(i).prefix());
        }
    }

    @Test
    public void testListObjectsV2WithPrefix() {
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(BUCKET)
                .prefix("jrc_mus_lung_covid.n5/")
                .delimiter("/")
                .maxKeys(10)
                .build();

        ListObjectsV2Response awsResp = awsClient.listObjectsV2(request);
        ListObjectsV2Response proxyResp = proxyClient.listObjectsV2(request);

        assertEquals("Prefix", awsResp.prefix(), proxyResp.prefix());
        assertEquals("KeyCount", awsResp.keyCount(), proxyResp.keyCount());

        // Compare contents
        List<S3Object> awsObjects = awsResp.contents();
        List<S3Object> proxyObjects = proxyResp.contents();
        assertEquals("Object count", awsObjects.size(), proxyObjects.size());

        for (int i = 0; i < awsObjects.size(); i++) {
            assertEquals("Key[" + i + "]", awsObjects.get(i).key(), proxyObjects.get(i).key());
        }

        // Compare common prefixes
        assertEquals("CommonPrefixes count",
                awsResp.commonPrefixes().size(),
                proxyResp.commonPrefixes().size());
    }

    @Test
    public void testListObjectsV2Pagination() {
        // Page 1
        ListObjectsV2Request req1 = ListObjectsV2Request.builder()
                .bucket(BUCKET)
                .delimiter("/")
                .maxKeys(1)
                .build();

        ListObjectsV2Response awsResp1 = awsClient.listObjectsV2(req1);
        ListObjectsV2Response proxyResp1 = proxyClient.listObjectsV2(req1);

        assertEquals("Page 1 IsTruncated", awsResp1.isTruncated(), proxyResp1.isTruncated());
        assertTrue("Should be truncated", proxyResp1.isTruncated());
        assertNotNull("Proxy should have continuation token", proxyResp1.nextContinuationToken());

        // Page 2 using each system's own token
        ListObjectsV2Response awsResp2 = awsClient.listObjectsV2(req1.toBuilder()
                .continuationToken(awsResp1.nextContinuationToken()).build());
        ListObjectsV2Response proxyResp2 = proxyClient.listObjectsV2(req1.toBuilder()
                .continuationToken(proxyResp1.nextContinuationToken()).build());

        // Both page 2 results should have the same content
        assertEquals("Page 2 key count", awsResp2.keyCount(), proxyResp2.keyCount());

        // Verify we got different content than page 1
        if (!proxyResp1.commonPrefixes().isEmpty() && !proxyResp2.commonPrefixes().isEmpty()) {
            assertNotEquals("Pages should differ",
                    proxyResp1.commonPrefixes().get(0).prefix(),
                    proxyResp2.commonPrefixes().get(0).prefix());
        }
    }

    @Test
    public void testListObjectsV2EmptyPrefix() {
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(BUCKET)
                .prefix("this-does-not-exist-xyz/")
                .build();

        ListObjectsV2Response awsResp = awsClient.listObjectsV2(request);
        ListObjectsV2Response proxyResp = proxyClient.listObjectsV2(request);

        assertEquals("KeyCount", Integer.valueOf(0), proxyResp.keyCount());
        assertEquals("KeyCount match", awsResp.keyCount(), proxyResp.keyCount());
        assertEquals("IsTruncated", awsResp.isTruncated(), proxyResp.isTruncated());
        assertTrue("Contents should be empty", proxyResp.contents().isEmpty());
    }

    @Test
    public void testListObjectsV2MaxKeysZero() {
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(BUCKET)
                .maxKeys(0)
                .build();

        ListObjectsV2Response proxyResp = proxyClient.listObjectsV2(request);

        assertEquals("Status", 200, proxyResp.sdkHttpResponse().statusCode());
        assertEquals("MaxKeys", Integer.valueOf(0), proxyResp.maxKeys());
        assertEquals("KeyCount", Integer.valueOf(0), proxyResp.keyCount());
        assertFalse("IsTruncated should be false", proxyResp.isTruncated());
    }

    @Test
    public void testGetObject() {
        String key = "jrc_mus_lung_covid.n5/attributes.json";

        var awsResp = awsClient.getObject(GetObjectRequest.builder()
                .bucket(BUCKET).key(key).build());
        var proxyResp = proxyClient.getObject(GetObjectRequest.builder()
                .bucket(BUCKET).key(key).build());

        // Compare response metadata
        assertEquals("Content-Type",
                awsResp.response().contentType(),
                proxyResp.response().contentType());
        assertEquals("Content-Length",
                awsResp.response().contentLength(),
                proxyResp.response().contentLength());
        assertEquals("ETag",
                awsResp.response().eTag(),
                proxyResp.response().eTag());

        // Compare body content
        try {
            byte[] awsBody = awsResp.readAllBytes();
            byte[] proxyBody = proxyResp.readAllBytes();
            assertArrayEquals("Body content", awsBody, proxyBody);
        } catch (Exception e) {
            fail("Failed to read response body: " + e.getMessage());
        }
    }

    @Test
    public void testGetObjectRange() {
        String key = "jrc_mus_lung_covid.n5/attributes.json";

        var awsResp = awsClient.getObject(GetObjectRequest.builder()
                .bucket(BUCKET).key(key).range("bytes=0-9").build());
        var proxyResp = proxyClient.getObject(GetObjectRequest.builder()
                .bucket(BUCKET).key(key).range("bytes=0-9").build());

        assertEquals("Status", 206, proxyResp.response().sdkHttpResponse().statusCode());
        assertEquals("Content-Length",
                awsResp.response().contentLength(),
                proxyResp.response().contentLength());
        assertEquals("Content-Range",
                awsResp.response().contentRange(),
                proxyResp.response().contentRange());

        try {
            byte[] awsBody = awsResp.readAllBytes();
            byte[] proxyBody = proxyResp.readAllBytes();
            assertEquals("Body length", 10, proxyBody.length);
            assertArrayEquals("Body content", awsBody, proxyBody);
        } catch (Exception e) {
            fail("Failed to read response body: " + e.getMessage());
        }
    }

    @Test
    public void testGetObjectNotFound() {
        String key = "this-key-xyz-does-not-exist-12345.txt";

        try {
            proxyClient.getObject(GetObjectRequest.builder()
                    .bucket(BUCKET).key(key).build());
            fail("Should have thrown NoSuchKeyException");
        } catch (NoSuchKeyException e) {
            assertEquals("Status", 404, e.statusCode());
        }

        try {
            awsClient.getObject(GetObjectRequest.builder()
                    .bucket(BUCKET).key(key).build());
            fail("Should have thrown NoSuchKeyException");
        } catch (NoSuchKeyException e) {
            assertEquals("Status", 404, e.statusCode());
        }
    }

    @Test
    public void testHeadObject() {
        String key = "jrc_mus_lung_covid.n5/attributes.json";

        HeadObjectResponse awsResp = awsClient.headObject(HeadObjectRequest.builder()
                .bucket(BUCKET).key(key).build());
        HeadObjectResponse proxyResp = proxyClient.headObject(HeadObjectRequest.builder()
                .bucket(BUCKET).key(key).build());

        assertEquals("Content-Type",
                awsResp.contentType(), proxyResp.contentType());
        assertEquals("Content-Length",
                awsResp.contentLength(), proxyResp.contentLength());
        assertEquals("ETag",
                awsResp.eTag(), proxyResp.eTag());
        assertEquals("Last-Modified",
                awsResp.lastModified(), proxyResp.lastModified());
    }

    @Test
    public void testHeadObjectNotFound() {
        String key = "this-key-xyz-does-not-exist-12345.txt";

        try {
            proxyClient.headObject(HeadObjectRequest.builder()
                    .bucket(BUCKET).key(key).build());
            fail("Should have thrown exception");
        } catch (S3Exception e) {
            assertEquals("Status", 404, e.statusCode());
        }
    }

    @Test
    public void testNoSuchBucket() {
        try {
            proxyClient.listObjectsV2(ListObjectsV2Request.builder()
                    .bucket("this-bucket-xyz-does-not-exist-99")
                    .build());
            fail("Should have thrown NoSuchBucketException");
        } catch (NoSuchBucketException e) {
            assertEquals("Status", 404, e.statusCode());
        }
    }
}
