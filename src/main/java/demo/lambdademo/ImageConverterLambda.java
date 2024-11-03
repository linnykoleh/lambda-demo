package demo.lambdademo;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ImageConverterLambda {

    private final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();

    public void handleRequest(S3Event s3Event, Context context) {
        System.out.println("Received event: " + s3Event);
        s3Event.getRecords().forEach(record -> {
            var bucketName = record.getS3().getBucket().getName();
            var key = record.getS3().getObject().getKey();
            try {
                var image = ImageIO.read(s3Client.getObject(bucketName, key).getObjectContent());

                uploadConvertedImage(bucketName, image, "bmp", "bmp/");
                uploadConvertedImage(bucketName, image, "gif", "gif/");
                uploadConvertedImage(bucketName, image, "png", "png/");

            } catch (IOException e) {
                context.getLogger().log("Error processing image: " + e.getMessage());
            }
        });
    }

    private void uploadConvertedImage(String bucketName, BufferedImage image, String format, String folder) throws IOException {
        var outputStream = new ByteArrayOutputStream();
        ImageIO.write(image, format, outputStream);
        var imageBytes = outputStream.toByteArray();

        var inputStream = new ByteArrayInputStream(imageBytes);
        var metadata = new ObjectMetadata();
        metadata.setContentLength(imageBytes.length);
        metadata.setContentType("image/" + format);

        var outputKey = folder + "converted_image." + format;
        var putRequest = new PutObjectRequest(bucketName, outputKey, inputStream, metadata);
        s3Client.putObject(putRequest);
    }
}
