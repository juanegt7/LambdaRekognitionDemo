package com.amazonaws.lambda.lambdarekognition;

import java.util.List;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.Attribute;
import com.amazonaws.services.rekognition.model.DetectFacesRequest;
import com.amazonaws.services.rekognition.model.DetectFacesResult;
import com.amazonaws.services.rekognition.model.FaceDetail;
import com.amazonaws.services.rekognition.model.Image;
import com.amazonaws.services.rekognition.model.S3Object;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;

import java.util.Calendar;
import java.util.HashMap;

public class LambdaFunctionHandler implements RequestHandler<S3Event, String> {

	private AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();

	public LambdaFunctionHandler() {
	}

	LambdaFunctionHandler(AmazonS3 s3) {
		this.s3 = s3;
	}

	@Override
	public String handleRequest(S3Event event, Context context) {
		context.getLogger().log("Received event: " + event);
		LambdaLogger logger = context.getLogger();
		// Obtiene el objeto del evento
		String bucket = event.getRecords().get(0).getS3().getBucket().getName();
		String key = event.getRecords().get(0).getS3().getObject().getKey();
		logger.log("Imagen: " + key);
		logger.log("Bucket: " + bucket);
		DetectFacesRequest request = new DetectFacesRequest()
				.withImage(new Image().withS3Object(new S3Object().withName(key).withBucket(bucket)))
				.withAttributes(Attribute.ALL);

		// Llama API DetectFaces
		AmazonRekognition amazonRekognition = AmazonRekognitionClientBuilder.standard().withRegion("us-east-1").build();
		DetectFacesResult result = amazonRekognition.detectFaces(request);

		// Obtiene el resultado por cada rostro y almacena en dynamo
		List<FaceDetail> faceDetails = result.getFaceDetails();

		HashMap<String, AttributeValue> item_values = new HashMap<String, AttributeValue>();
		final AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.defaultClient();

		for (FaceDetail face : faceDetails) {
			item_values.put("hash", new AttributeValue(String.valueOf(face.hashCode())));
			item_values.put("gender", new AttributeValue(String.valueOf(face.getGender().getValue())));
			item_values.put("highage", new AttributeValue(String.valueOf(face.getAgeRange().getHigh().toString())));
			item_values.put("lowage", new AttributeValue(String.valueOf(face.getAgeRange().getLow().toString())));

			try {
				ddb.putItem("AmazonRekognitionDemo", item_values);
			} catch (ResourceNotFoundException e) {
				System.err.format("Error: La tabla \"%s\" no existe.\n", "AmazonRekognitionDemo");
				System.exit(1);
			} catch (AmazonServiceException e) {
				System.err.println(e.getMessage());
				System.exit(1);
			}
			item_values = new HashMap<String, AttributeValue>();
		}
		return "OK";
	}
}