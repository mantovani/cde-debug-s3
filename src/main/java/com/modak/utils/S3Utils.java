package com.modak.utils;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.IOUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.*;
import java.util.regex.Pattern;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;
import software.amazon.awssdk.services.sts.model.StsException;

public class S3Utils {

    public static AmazonS3 getS3ClientIAM( Regions regions) {
        AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(regions).build();
        return s3;
    }

    public static StsClient getStrClient2() {
        software.amazon.awssdk.regions.Region region = software.amazon.awssdk.regions.Region.US_EAST_1;
        StsClient stsClient = StsClient.builder()
                .region(region)
                .build();

        return stsClient;
    }

    public static void getCallerId(StsClient stsClient) {
        try {
            GetCallerIdentityResponse response = stsClient.getCallerIdentity();

            System.out.println("The user id is" +response.userId());
            System.out.println("The ARN value is" +response.arn());

        } catch (StsException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    public static AmazonS3 getS3Client(String accessKey, String secretKey, Regions defaultRegion) {
        BasicAWSCredentials credentials = new BasicAWSCredentials(
                accessKey, secretKey
        );
        AmazonS3 s3Client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(defaultRegion)
                .build();
        return s3Client;
    }

    public static List<String> listObjects(AmazonS3 s3Client, String bucketName, String prefix) {
        if (s3Client== null || bucketName == null) {
            throw new NullPointerException("parameters are null");
        }
        bucketName = bucketName.replace("s3://","");
        bucketName = bucketName.replace("s3a://","");
        if ( prefix == null) {
            prefix = "";
        }
        ArrayList<String> list = new ArrayList<>();
        S3Objects.withPrefix(s3Client, bucketName,prefix).withBatchSize(100).forEach((S3ObjectSummary objectSummary) -> {
            list.add(objectSummary.getKey());
        });

        return list;
    }

    public static void deleteObjects(AmazonS3 s3Client, String sourceBucketName, String prefix) {
        List<String> list = listObjects(s3Client,sourceBucketName,prefix);
        for (String key: list) {
            deleteObject(s3Client,sourceBucketName,key);
        }
    }

    public static void consolidateSparkOutputFiles(AmazonS3 s3Client, String s3URL) throws Exception {
        if (s3URL != null) {
            s3URL = s3URL.replace("s3a://","s3://");
            AmazonS3URI as3uri = new AmazonS3URI(s3URL);
            if (as3uri != null) {
                String bucket = as3uri.getBucket();
                String key = as3uri.getKey();
                if (bucket != null & key != null) {
                    String prefix = key + "/";
                    String extension = FilenameUtils.getExtension(key);
                    if (extension == null) throw new Exception("URL needs to have an extension. e.g. CSV or Parquet");
                    List<String> list = listObjects(s3Client, bucket, prefix, "part-.*\\." + extension);
                    if (list.size() == 1) {
                        //move the file
                        // e.g bucket/folder/myfile.csv/part-nslnvlsns13r83-.csv will be moved to bucket/folder/myfile.csv
                        moveObject(s3Client, bucket, list.get(0), bucket, key);
                        //Now delete the files
                        deleteObjects(s3Client, bucket, prefix);
                    } else if (list.size() > 1) {
                        throw new Exception("Only one file is expected to consolidate.");
                    } else {
                        throw new Exception("No file found to consolidate.");
                    }
                }
                else {
                    throw new Exception("URL doesn't seem to be valid");
                }
            }
        }
        else {
            throw new NullPointerException("Params are null");
        }
    }


    public static void moveObjectURI(AmazonS3 s3Client, String s3SourceURL, String s3DestURL) throws Exception {
        if (s3SourceURL != null && s3DestURL != null && s3Client != null) {
            s3SourceURL = s3SourceURL.replace("s3a://","s3://");
            s3DestURL = s3DestURL.replace("s3a://","s3://");

            AmazonS3URI as3Sourceuri = new AmazonS3URI(s3SourceURL);
            AmazonS3URI as3Desturi = new AmazonS3URI(s3DestURL);
            if (as3Sourceuri != null && as3Desturi != null) {
                String sourceBucket = as3Sourceuri.getBucket();
                String sourceKey = as3Sourceuri.getKey();

                String destBucket = as3Desturi.getBucket();
                String destKey = as3Desturi.getKey();
                if (sourceBucket != null & sourceKey != null && destBucket != null && destKey != null) {

                    moveObject(s3Client, sourceBucket, sourceKey, destBucket, destKey);
                    //Now delete the files
                    //deleteObjects(s3Client, sourceBucket, sourceKey);
                }
                else {
                    throw new Exception("URL doesn't seem to be valid");
                }
            }
        }
        else {
            throw new NullPointerException("Params are null");
        }
    }



    public static List<String> listObjects(AmazonS3 s3Client, String bucketName, String prefix, String regexMatchPattern) {
        List<String> list = listObjects(s3Client,bucketName,prefix);
        ArrayList<String> rList = new ArrayList<>();
        Pattern p = Pattern.compile(regexMatchPattern);
        for (String key : list) {
            if (p.matcher(key).find()) {
                rList.add(key);
            }
        }
        if(rList.isEmpty())
        {
            Pattern p1 = Pattern.compile(regexMatchPattern.toLowerCase());
            for (String key : list) {
                if (p1.matcher(key).find()) {
                    rList.add(key);
                }
            }
        }
        return rList;
    }

    public static List<String> listObjectsSamePath(AmazonS3 s3Client, String bucketName, String prefix, String regexMatchPattern)
    {
        List<String> list = listObjects(s3Client,bucketName,prefix);
        ArrayList<String> rList = new ArrayList<>();
        Pattern p = Pattern.compile(regexMatchPattern);
        for (String key : list) {
            if (p.matcher(key).find())
            {
                rList.add(key);
            }
        }
        ArrayList<String> finalList = new ArrayList<>();
        int countOfPrefix = StringUtils.countMatches(prefix,"/");
        int countOfKey;
        for(String str : rList)
        {
            countOfKey = StringUtils.countMatches(str,"/");
            if(countOfPrefix == countOfKey)
                finalList.add(str);
        }
        /*
        Map<String,String> checkKey = new HashMap();
        for(String key : rList)
        {
            String duplicate = key;
            duplicate = duplicate.replaceAll(FilenameUtils.getBaseName(duplicate), "");
            duplicate = duplicate.replaceAll("."+FilenameUtils.getExtension(duplicate),"");
            checkKey.put(key,duplicate);
        }

        ArrayList<String> finalList = new ArrayList<>();
        for(Map.Entry<String,String> entry : checkKey.entrySet())
        {
            String key = entry.getKey();
            String value = entry.getValue();
            if(value.equalsIgnoreCase(prefix))
            {
                finalList.add(key);
            }
        }*/
        return finalList;
    }
    public static List<String> listPathsSamePath(AmazonS3 s3Client, String bucketName, String prefix, String regexMatchPattern) {
        List<String> list = listObjectsSamePath(s3Client,bucketName,prefix,regexMatchPattern);
        ArrayList<String> rList = new ArrayList<>();
        for (String key : list)
        {
            String path = bucketName + "/" + key ;
            path = path.replace("s3://","s3a://");
            rList.add(path);
        }
        return rList;
    }

    public static List<String> listPaths(AmazonS3 s3Client, String bucketName, String prefix, String regexMatchPattern) {
        List<String> list = listObjects(s3Client,bucketName,prefix);
        ArrayList<String> rList = new ArrayList<>();
        Pattern p = Pattern.compile(regexMatchPattern);
        for (String key : list) {
            if (p.matcher(key).find()) {
                String path = bucketName + "/" + key ;
                path = path.replace("s3://","s3a://");
                rList.add(path);
            }
        }
        return rList;
    }

    public static void moveObject(AmazonS3 s3Client, String sourceBucketName, String sourceObjectKey,
                                  String destBucketName, String destObjectKey) {
        if (s3Client== null || sourceBucketName == null
                || destBucketName == null || sourceObjectKey == null || destObjectKey == null ) {
            throw new NullPointerException("parameters are null");
        }
        sourceBucketName = sourceBucketName.replace("s3://","");
        sourceBucketName = sourceBucketName.replace("s3a://","");
        destBucketName = destBucketName.replace("s3://","");
        destBucketName = destBucketName.replace("s3a://","");

        if (s3Client.doesObjectExist(sourceBucketName, sourceObjectKey)) {
            ObjectMetadata objectMetadata = s3Client.getObjectMetadata(sourceBucketName,sourceObjectKey);
            //objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
            long inputSize = objectMetadata.getContentLength();
            if (inputSize >= 1024*1024*1024) {
                // size greater than 1GB, let's use multipartload
                multipartUpload(s3Client, sourceBucketName,
                        destBucketName, sourceObjectKey, destObjectKey);
            }
            else {
                CopyObjectRequest copyObjRequest = new CopyObjectRequest(sourceBucketName, sourceObjectKey
                        , destBucketName, destObjectKey);
                copyObjRequest
                        .withCannedAccessControlList(CannedAccessControlList.BucketOwnerFullControl);
                ObjectMetadata meta = new ObjectMetadata();
                meta.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
                copyObjRequest.setNewObjectMetadata(meta);
                s3Client.copyObject(copyObjRequest);
            }
            ObjectMetadata objectMetadata2 = s3Client.getObjectMetadata(destBucketName,destObjectKey);
            if (objectMetadata2 != null) {
                long outputSize = objectMetadata2.getContentLength();
                if (inputSize != outputSize) {
                    throw new AmazonServiceException("Copy failed. The size in destination doesn't match source object size.");
                }
            }
            else {
                throw new AmazonServiceException("Copy failed");
            }
        }
        else {
            throw new AmazonServiceException(sourceObjectKey + " object not found in " + sourceBucketName + " bucket.");
        }

    }

    private static void multipartUpload(AmazonS3 s3Client, String sourceBucketName,
                                        String destBucketName, String sourceObjectKey, String destObjectKey)  {
        try {

            // Initiate the multipart upload.
            InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(destBucketName, destObjectKey);
            InitiateMultipartUploadResult initResult = s3Client.initiateMultipartUpload(initRequest);

            // Get the object size to track the end of the copy operation.
            GetObjectMetadataRequest metadataRequest = new GetObjectMetadataRequest(sourceBucketName, sourceObjectKey);
            ObjectMetadata metadataResult = s3Client.getObjectMetadata(metadataRequest);
            long objectSize = metadataResult.getContentLength();

            // Copy the object using 5 MB parts.
            long partSize = 5 * 1024 * 1024;
            long bytePosition = 0;
            int partNum = 1;
            List<CopyPartResult> copyResponses = new ArrayList<CopyPartResult>();
            while (bytePosition < objectSize) {
                // The last part might be smaller than partSize, so check to make sure
                // that lastByte isn't beyond the end of the object.
                long lastByte = Math.min(bytePosition + partSize - 1, objectSize - 1);

                // Copy this part.
                CopyPartRequest copyRequest = new CopyPartRequest()
                        .withSourceBucketName(sourceBucketName)
                        .withSourceKey(sourceObjectKey)
                        .withDestinationBucketName(destBucketName)
                        .withDestinationKey(destObjectKey)
                        .withUploadId(initResult.getUploadId())
                        .withFirstByte(bytePosition)
                        .withLastByte(lastByte)
                        .withPartNumber(partNum++);
                copyResponses.add(s3Client.copyPart(copyRequest));
                bytePosition += partSize;
            }

            // Complete the upload request to concatenate all uploaded parts and make the copied object available.
            CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(
                    destBucketName,
                    destObjectKey,
                    initResult.getUploadId(),
                    getETags(copyResponses));
            s3Client.completeMultipartUpload(completeRequest);
            System.out.println("Multipart copy complete.");
        } catch (AmazonServiceException e) {
            throw e;
        } catch (SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            throw e;
        }
    }

    // This is a helper function to construct a list of ETags.
    private static List<PartETag> getETags(List<CopyPartResult> responses) {
        List<PartETag> etags = new ArrayList<PartETag>();
        for (CopyPartResult response : responses) {
            etags.add(new PartETag(response.getPartNumber(), response.getETag()));
        }
        return etags;
    }

    public static void deleteObject(AmazonS3 s3Client, String sourceBucketName, String sourceObjectKey) {
        if (sourceBucketName != null & sourceObjectKey != null) {
            sourceBucketName = sourceBucketName.replace("s3://", "");
            sourceBucketName = sourceBucketName.replace("s3a://", "");
            if (s3Client.doesObjectExist(sourceBucketName, sourceObjectKey)) {
                s3Client.deleteObject(new DeleteObjectRequest(sourceBucketName, sourceObjectKey));
            } else {
                System.out.println("WARN: Object doesn't exist to delete : " + sourceBucketName + "/" + sourceObjectKey);
            }
        }

    }

    public static void deleteObjectURI(AmazonS3 s3Client, String s3URI) {
        if (s3Client != null && s3URI != null) {

            s3URI = s3URI.replace("s3a://", "s3://");
            AmazonS3URI sources3uri = new AmazonS3URI(s3URI);

            if (sources3uri != null) {
                String sourceBucketName = sources3uri.getBucket();
                String sourceObjectKey = sources3uri.getKey();
                if (s3Client.doesObjectExist(sourceBucketName, sourceObjectKey)) {
                    s3Client.deleteObject(new DeleteObjectRequest(sourceBucketName, sourceObjectKey));
                } else {
                    System.out.println("WARN: Object doesn't exist to delete : " + sourceBucketName + "/" + sourceObjectKey);
                }
            }
        }
    }

    public static void moveStringToS3(AmazonS3 s3Client,String value, String destBucketName, String destObjectKey) throws IOException
    {
        if (s3Client == null || value == null || destBucketName == null || destObjectKey == null)
        {
            throw new NullPointerException("parameters are null");
        }
        ByteArrayInputStream inputStream = new ByteArrayInputStream(value.getBytes(Charset.forName("UTF-8")));
        OutputStream outputStream = new S3OutputStream(s3Client, destBucketName, destObjectKey);
        IOUtils.copy(inputStream, outputStream);

        inputStream.close();
        outputStream.close();
    }

    public static void deleteObject(AmazonS3 s3Client, String s3URL) {
        if (s3URL != null) {
            AmazonS3URI as3uri = new AmazonS3URI(s3URL);
            if (as3uri != null) {
                String sourceBucketName = as3uri.getBucket();
                String sourceObjectKey = as3uri.getKey();
                if (sourceBucketName != null & sourceObjectKey != null) {
                    if (s3Client.doesObjectExist(sourceBucketName, sourceObjectKey)) {
                        s3Client.deleteObject(new DeleteObjectRequest(sourceBucketName, sourceObjectKey));
                    } else {
                        System.out.println("WARN: Object doesn't exist to delete : " + s3URL);
                    }
                }
            }
        }

    }

}

