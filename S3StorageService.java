package com.bsb.portal.db;

import java.io.File;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.bsb.portal.common.Constants;
import com.bsb.portal.common.Constants.S3_BUCKETS;
import com.bsb.portal.config.S3Config;
import com.bsb.portal.coupons.constants.CouponConstants;

/**
 * Notes: 1. There's a limit of 100 buckets per AWS account, if I remember correctly. 2. Follow the
 * structure : - bucket_name - user_images user_1 user_2 user_n+1 other_stuff more_stuff
 */
public class S3StorageService {

    private AmazonS3 amazonS3Service;

    private Bucket newsImageBucket;
    private Bucket photoImageBucket;

    private S3Config config = null;
    private final static String SINGAPORE_DEFAULT_BUCKET_ENDPOINT = "http://s3-ap-southeast-1.amazonaws.com";

    public S3StorageService(S3Config s3Config) {
        config = s3Config;
        init(SINGAPORE_DEFAULT_BUCKET_ENDPOINT);
    }

    public S3StorageService(S3Config s3Config, String endPoint) {
        config = s3Config;
        init(endPoint);
    }
    
    private void init(String endPoint) {
        AWSCredentials awsCredentials = new AWSCredentials() {

            @Override
            public String getAWSAccessKeyId() {
                return config.getAwsAccessKeyId();
            }

            @Override
            public String getAWSSecretKey() {
                return config.getAwsSecretKey();
            }
        };

        amazonS3Service = new AmazonS3Client(awsCredentials);
        // http://docs.amazonwebservices.com/general/latest/gr/rande.html#s3_region
        amazonS3Service.setEndpoint(endPoint);
        List<Bucket> s3buckets = amazonS3Service.listBuckets();
        for (int i = 0; i < s3buckets.size(); i++) {
            Bucket bucket = s3buckets.get(i);
            System.out.println(bucket.getName());
            if (bucket.getName().equalsIgnoreCase(config.getPhotoBucketName())) {
                photoImageBucket = bucket;
            }
        }

    }

    public String store(String type, String basepath, String filename, File resourceFile) {
        // ObjectListing objlist = amazonS3Service.listObjects(photoImageBucket.getName());
        // List<S3ObjectSummary> objs = objlist.getObjectSummaries();
        // for (int i = 0; i < objs.size(); i++) {
        // S3ObjectSummary s3ObjectSummary = objs.get(i);
        // System.out.println("s3ObjectSummary : "+s3ObjectSummary.getKey()+": "+s3ObjectSummary.getETag());
        // }
        PutObjectResult result = amazonS3Service.putObject(photoImageBucket.getName(), basepath + "/" + filename, resourceFile);
        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        System.out.println(result.getETag() + " : " + result.getExpirationTime());
        return result.getETag();
    }

    public String store(String bucketName, String filename, File resourceFile) {
        PutObjectResult result = amazonS3Service.putObject(new PutObjectRequest(bucketName, filename, resourceFile).withCannedAcl(CannedAccessControlList.PublicRead));
        return result.getETag();
    }
    
    public String store(Constants.S3_BUCKETS type, String basepath, String filename, String contentType,
                        InputStream inputStream) {
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentType(contentType);
        PutObjectResult result = amazonS3Service.putObject(new PutObjectRequest(getBucketName(type), basepath + "/" + filename, inputStream, meta).withCannedAcl(CannedAccessControlList.PublicRead));

        return result.getETag();
    }
    
    public String store(String bucketName, String basepath, String filename, String contentType,
                        InputStream inputStream) {
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentType(contentType);
        PutObjectResult result = amazonS3Service.putObject(new PutObjectRequest(bucketName, basepath + "/" + filename, inputStream, meta).withCannedAcl(CannedAccessControlList.PublicRead));
        
        return result.getETag();
    }
    
    public String store(String bucketName, String basepath, String filename, InputStream inputStream, long contentLength) {
        ObjectMetadata objectMetadata = null;
        
        if (contentLength >= 0) {
            objectMetadata = new ObjectMetadata();
            objectMetadata.setContentLength(contentLength);
        }
        
        PutObjectResult result = amazonS3Service.putObject(new PutObjectRequest(bucketName, basepath + "/" + filename, inputStream, objectMetadata).withCannedAcl(CannedAccessControlList.PublicRead));
        return result.getETag();
    }
    
    public InputStream fetchData(S3_BUCKETS bucketType, String filename){
        
        S3Object object = amazonS3Service.getObject(new GetObjectRequest(getBucketName(bucketType), filename)); 
        return object.getObjectContent();        
    } 
    
    public ObjectMetadata fetchFile(S3_BUCKETS bucketType, String filename, String destPath){
        
        return amazonS3Service.getObject(new GetObjectRequest(getBucketName(bucketType), filename), new File (destPath));        
    }
    
    public ObjectMetadata fetchFile(String bucketName, String filename, String destPath){
        
        return amazonS3Service.getObject(new GetObjectRequest(bucketName, filename), new File (destPath));        
    }
    
    public InputStream fetchData(String bucketName, String filename){
        
        S3Object object = amazonS3Service.getObject(new GetObjectRequest(bucketName, filename)); 
        return object.getObjectContent();        
    } 
    
    public void deleteObject(String bucketName, String file){
        amazonS3Service.deleteObject(bucketName, file);
    }

    public void createBucket(String bucketName){
        
        amazonS3Service.createBucket(bucketName);
        amazonS3Service.setBucketAcl(bucketName, CannedAccessControlList.PublicRead);    
    }
    
    public String getBucketName(Constants.S3_BUCKETS bucket) {
        switch (bucket) {
            case IMAGE:
                return config.getPhotoBucketName();
            case VIDEO:
                return config.getVideoBucketName();
            case TEXTIMAGE:
                return config.getTextImageBucketName();
            case AVATARS:
                return config.getAvatarImageBucketName();
            case RDLOGS:
                return config.getRdLogsBucketName();
            default:
                return "";
        }
    }

    public static void main(String[] args) {
        S3Config s3Config = new S3Config();
        s3Config.setAwsAccessKeyId("AKIAI3NZJM4RIEXSK2XQ");
        s3Config.setAwsSecretKey("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");
        //s3Config.setPhotoBucketName("photos.bsbportal");
        S3StorageService s3StorageService = new S3StorageService(s3Config);
       // s3StorageService.store(null,"indiatoday","81842_org.jpg", new
        //File("/Users/bhuvangupta/portaldev/microsite/web/albums/jthj/81842_org.jpg"));
        
////        s3StorageService.createBucket("myairtelapp");
//        File file = new File("/Users/seema/git/portal-bheem-myairtelapp/config/dev/genericTile.json");
//        s3StorageService.store("myairtelapp", "json/genericTile.json", file);
////        s3StorageService.deleteObject("myairtelapp", "json/genericTile.json");
//        System.out.println("Done");

        
//        s3StorageService.fetchFile("myairtelapp", "2015-02/2015-02-11/stbacken0001apse01.in.bsbportal.com.maa_transaction_analytics.log.2015-02-11-22",
//                "/Users/seema/git/portal-bheem-myairtelapp/config/dev/logfile");
//        System.out.println("Done");
        
//        System.out.println("Start");
//        File file = new File("/Users/seema/git/portal-bheem-myairtelapp/config/dev/GenreChannels.json");
//        s3StorageService.store("myairtel-dev", "json/channelList.json", file);
//        System.out.println("Done");
        	
//        DO NOT REMOVE THIS CODE: [Aakash Garg]
//		  For UPLOADING Json to s3.        
//        System.out.println("Start");
//        File file = new File("/Users/akash/Desktop/bsbPortal/myAirtelApp/portal-bheem-MyAirtelApp/portal-bheem/config/dev/couponImgMapping.json");
//        s3StorageService.store("myairtel-dev", CouponConstants.COUPON_IMG_MAP_FILEPATH_AT_S3, file);
//        System.out.println("Done");
//        
//        System.out.println("Start");
//        File file = new File("/Users/akash/Desktop/bsbPortal/myAirtelApp/portal-bheem-MyAirtelApp/portal-bheem/config/dev/promoOfferMapping.json");
//        s3StorageService.store("myairtel-dev", MaaOffersConstants.PROMO_OFFERS_RULE_FILEPATH_AT_S3, file);
//        System.out.println("Done");


    }
}
