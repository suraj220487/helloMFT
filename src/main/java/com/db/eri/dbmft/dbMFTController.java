package com.db.eri.dbmft;

import com.google.cloud.storage.*;
import io.cloudevents.CloudEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

@RestController
@RequestMapping("/eventarc")
public class dbMFTController {

    private static final Logger logger = Logger.getLogger(dbMFTController.class.getName());

    private final Storage storage;

    @Value("${gcs.dest-bucket}")
    private String destBucket;

    public dbMFTController() {
        this.storage = StorageOptions.getDefaultInstance().getService();
    }

    @PostMapping("/gcs")
    public ResponseEntity<String> handleCloudStorageEvent(@RequestBody byte[] body) {
        try {
            String json = new String(body, StandardCharsets.UTF_8);
            logger.info("Received CloudEvent JSON: " + json);

            // Basic parsing of bucket and name from event data payload
            String bucket = extract(json, "\"bucket\":\"", "\"");
            String name = extract(json, "\"name\":\"", "\"");

            logger.info("Processing file '" + name + "' in bucket '" + bucket + "'");

            BlobId sourceBlobId = BlobId.of(bucket, name);
            BlobId destBlobId = BlobId.of(destBucket, name);

            // Copy file
            storage.copy(Storage.CopyRequest.of(sourceBlobId, destBlobId));
            // Delete source
            storage.delete(sourceBlobId);

            logger.info("Moved file '" + name + "' to bucket '" + destBucket + "'");

            return ResponseEntity.ok("File moved successfully");
        } catch (Exception e) {
            logger.severe("Error processing event: " + e.getMessage());
            return ResponseEntity.status(500).body("Error processing event");
        }
    }

    private String extract(String source, String start, String end) {
        int startIndex = source.indexOf(start);
        if (startIndex < 0) return null;
        startIndex += start.length();
        int endIndex = source.indexOf(end, startIndex);
        if (endIndex < 0) return null;
        return source.substring(startIndex, endIndex);
    }
}
