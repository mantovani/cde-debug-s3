package com.modak.utils;

import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class S3Utils {

    public static AmazonS3 getS3ClientIAM( Regions regions) {
        AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(regions)
                .withCredentials(new InstanceProfileCredentialsProvider(false)) .build();
        return s3;
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

