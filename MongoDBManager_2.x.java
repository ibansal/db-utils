package com.bsb.portal.db;

import java.math.BigInteger;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import com.bsb.portal.common.PortalContext;
import com.bsb.portal.config.MongoDBConfig;
import com.bsb.portal.dto.AmazonS3Publisher;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceCommand.OutputType;
import com.mongodb.MapReduceOutput;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.bsb.portal.dto.AmazonS3Publisher;
import com.bsb.portal.music.ExceptionTypeEnum;
import com.bsb.portal.util.LogstashLoggerUtils;
import com.mongodb.MapReduceCommand.OutputType;
import com.mongodb.util.JSON;

/**
 * Created with IntelliJ IDEA. User: bhuvangupta Date: 20/09/12 Time: 10:49 PM To change this
 * template use File | Settings | File Templates.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class MongoDBManager {

    protected static final Logger logger   = LoggerFactory.getLogger(MongoDBManager.class);
    
    private static final Logger mDatabaseLogger = LoggerFactory.getLogger("mdatabasetransactions");

    public static final String  MONGO_ID = "_id";
    
    private static final String AMAZON_S3_COLLECTION = "amazons3";
    private static final int DEFAULT_ALLOWED_THREADS_TO_BLOCK = 50;
    private static final int DEFAULT_CONNECTIONS_PER_HOST = 100;
    private Mongo               mongo;
    private DB                  metadataDB;
    private Boolean             isLoggingEnabled;
    
    private MongoDBConfig       config;

    public MongoDBManager(MongoDBConfig config) {
        init(config);
    }

    private void init(MongoDBConfig config) {
        
        try {
            this.config = config;
            if(config == null){
                logger.error("not creating mongodb manager");
                return;
             }
             if(StringUtils.isBlank(config.getMongodbHost())){
                logger.error("not creating mongodb manager for [{}]",config.getMongoDBName());
                return;
             }
             List<ServerAddress> seeds = new ArrayList<>();
             if(config == null){
            	 logger.error("not creating mongodb manager");
            	 return;
             }
             if(StringUtils.isBlank(config.getMongodbHost())){
            	 logger.error("not creating mongodb manager for "+config.getMongoDBName());
            	 return;
             }
             ServerAddress primary = new ServerAddress(config.getMongodbHost(), config.getMongodbPort());
             seeds.add(primary);
             if(config.getMongodbHostSlave() != null) {
                 ServerAddress slave = new ServerAddress(config.getMongodbHostSlave(), config.getMongodbPortSlave());
                 seeds.add(slave);
             }
             if(config.getMongodbHostArbiter() != null) {
                 ServerAddress arbiter = new ServerAddress(config.getMongodbHostArbiter(), config.getMongodbPortArbiter());
                 seeds.add(arbiter);
             }

             Builder builder = MongoClientOptions.builder();
             
             int mongodbThreadsAllowedToBlock = config.getMongodbThreadsAllowedToBlock();
             if(mongodbThreadsAllowedToBlock <=0 ){
            	 mongodbThreadsAllowedToBlock = DEFAULT_ALLOWED_THREADS_TO_BLOCK;
             }
			 int mongodbConnectionsPerHost = config.getMongodbConnectionsPerHost();
			 if(mongodbConnectionsPerHost <=0 ){
				 mongodbConnectionsPerHost = DEFAULT_CONNECTIONS_PER_HOST;
             }
			 builder.threadsAllowedToBlockForConnectionMultiplier(mongodbThreadsAllowedToBlock).autoConnectRetry(true).connectionsPerHost(mongodbConnectionsPerHost);
             if(config.getMongodbHostSlave() != null && !config.isReadPrimary()) {
                 builder.readPreference(ReadPreference.secondaryPreferred());
             }
             MongoClientOptions options = builder.build();
             
             mongo = new MongoClient(seeds, options);
             metadataDB = mongo.getDB(config.getMongoDBName());
             isLoggingEnabled = false;
             if(config.getMongodbLoggingEnabled() != null && config.getMongodbLoggingEnabled())
                 isLoggingEnabled = true;
         }
         catch (UnknownHostException e) {
             throw new RuntimeException(e.getMessage(), e);
         }
    }

    DB getDB() {
        return metadataDB;
    }

    private DBCollection createCollection(String collectionName) {
        DBCollection collection = getDB().getCollection(collectionName);
        if(collection == null) {
            collection = getDB().createCollection(collectionName, null);
        }
        return collection;
    }

    @Deprecated
    public String addObject(String collectionName, String objectId, String jsonData) {
        long startTime = System.currentTimeMillis();
        DBCollection collection = getDB().getCollection(collectionName);
        DBObject dbObject = (DBObject) JSON.parse(jsonData);
        if(objectId == null) {
            collection.insert(dbObject);
            // System.out.println(collection.findOne());
            if(isLoggingEnabled){
                mDatabaseLogger.info("addObject: Collection - "+ collectionName +", idStr - "+objectId+", jsonData - "+jsonData+" TimeTaken : "+ (System.currentTimeMillis() - startTime));
            }
            return getObjectIdString((ObjectId) dbObject.get(MONGO_ID));
        }

        dbObject.put(MONGO_ID, getObjectId(objectId));
        DBObject result = getObject(collection, objectId);
        if(result == null) {
            collection.insert(dbObject);
            if(isLoggingEnabled){
                mDatabaseLogger.info("addObject: Collection - "+ collectionName +", idStr - "+objectId+", jsonData - "+jsonData+" TimeTaken : "+ (System.currentTimeMillis() - startTime));
            }
            return getObjectIdString((ObjectId) dbObject.get(MONGO_ID));
        }
        else {
            BasicDBObject query = new BasicDBObject();
            query.put(MONGO_ID, getObjectId(objectId));
            collection.update(query, dbObject);
            if(isLoggingEnabled){
                mDatabaseLogger.info("addObject: Collection - "+ collectionName +", idStr - "+objectId+", jsonData - "+jsonData+" TimeTaken : "+ (System.currentTimeMillis() - startTime));
            }
            return objectId;
        }
    }

    public String addObject(String collectionName, String objectId, String jsonData, boolean returnEncodedId) {
        return addObject(collectionName, objectId, jsonData, returnEncodedId, true);
    }

    public String addObject(String collectionName, String idStr, String jsonData, boolean returnEncodedId, boolean useObjectIdAsId) {
        if(returnEncodedId) {
            return addObject(collectionName, idStr, jsonData);
        }
        long startTime = System.currentTimeMillis();
        DBCollection collection = getDB().getCollection(collectionName);
        DBObject dbObject = (DBObject) JSON.parse(jsonData);
        if(StringUtils.isBlank(idStr)){
            collection.insert(dbObject);
//            ObjectId mongoId = getObjectId((String)dbObject.get(MONGO_ID));
            ObjectId mongoId = (ObjectId) dbObject.get(MONGO_ID);
            if(isLoggingEnabled){
                mDatabaseLogger.info("addObject: Collection - "+ collectionName +", idStr - "+ idStr +", jsonData - "+jsonData+" TimeTaken : "+ (System.currentTimeMillis() - startTime));
            }
            return mongoId.toString();
        }

        Object id = useObjectIdAsId? getObjectId(idStr) : idStr;
        dbObject.put(MONGO_ID, id);

        DBObject result = getObject(collection, idStr, useObjectIdAsId);
        if(result == null) {
            collection.insert(dbObject);
            Object idObject = dbObject.get(MONGO_ID);
            String mongoId = useObjectIdAsId? ((ObjectId) idObject).toString() : idObject.toString();
            if(isLoggingEnabled){
                mDatabaseLogger.info("addObject: Collection - "+ collectionName +", idStr - "+idStr+", jsonData - "+jsonData+" TimeTaken : "+ (System.currentTimeMillis() - startTime));
            }
            return mongoId;
        }
        else {
            BasicDBObject query = new BasicDBObject();
            Object idObj = useObjectIdAsId? getObjectId(idStr) : idStr;
            query.put(MONGO_ID, idObj);
            collection.update(query, dbObject);
            if(isLoggingEnabled){
                mDatabaseLogger.info("addObject: Collection - "+ collectionName +", idStr - "+idStr+", jsonData - "+jsonData+" TimeTaken : "+ (System.currentTimeMillis() - startTime));
            }
            return idStr;
        }
    }

    /**
     * This method uses 'insert()' and not 'save()' so jsonData must have '_id' field
     * 
     * @param collectionName
     * @return
     */
    // TODO: check the write concern for if there is any object which couldn't get inserted
    // successfully
    public void addObjects(String collectionName, List<DBObject> dbObjects) {
        DBCollection collection = getDB().getCollection(collectionName);
        if(dbObjects != null && dbObjects.size() > 0) {
            WriteConcern writeConcern = new WriteConcern();
            writeConcern = writeConcern.continueOnErrorForInsert(true);
            collection.insert(dbObjects, writeConcern);
        }
    }

    public void addObject(String collectionName, String jsonData) {
        DBCollection collection = getDB().getCollection(collectionName);
        collection.insert((DBObject) JSON.parse(jsonData));
    }

    public DBObject getObject(DBCollection collection, String objectId) {
        return getObject(collection, objectId, true);
    }

    public DBObject getObject(DBCollection collection, String idStr, boolean useObjectIdAsId) {
        if(StringUtils.isBlank(idStr))
            return null;
        long startTime = System.currentTimeMillis();
        BasicDBObject query = new BasicDBObject();
        Object id = useObjectIdAsId? getObjectId(idStr) : idStr;
        query.put(MONGO_ID, id);
        DBObject result = collection.findOne(query);
        if(isLoggingEnabled){
            mDatabaseLogger.info("getObject: Collection - "+ collection.toString() +", query - "+ query.toString() +", TimeTaken : "+ (System.currentTimeMillis() - startTime));
        }
        return result;
    }

    public DBObject getObject(String collectionName, String objectId) {
        return getObject(true, collectionName, objectId);
    }

    public DBObject getObject(boolean useObjectIdAsId, String collectionName, String idStr) {
        DBCollection collection = getDB().getCollection(collectionName);
        BasicDBObject query = new BasicDBObject();
        Object id = useObjectIdAsId? getObjectId(idStr) : idStr;
        query.put(MONGO_ID, id);
        DBObject result = collection.findOne(query);
        return result;
    }

    public boolean deleteObject(String collectionName, DBObject object) {
        DBCollection collection = getDB().getCollection(collectionName);
        if(object != null) {
            collection.remove(object);
            return true;
        }
        else {
            return false;
        }
    }

    public boolean deleteObject(String collectionName, String objectId) {
        DBCollection collection = getDB().getCollection(collectionName);
        DBObject result = getObject(collection, objectId);
        if(result != null) {
            collection.remove(result);
            return true;
        }
        else {
            return false;
        }
    }

    public void deleteObjects(String collectionName, Map queryParams) {
        DBCollection collection = getDB().getCollection(collectionName);
        DBObject query = new BasicDBObject(queryParams);
        collection.remove(query);
    }

    public DBObject getObject(String collectionName, Map queryParams) {
        long startTime = System.currentTimeMillis();
        DBCollection collection = getDB().getCollection(collectionName);

        BasicDBObject query = new BasicDBObject(queryParams);

        DBObject result = collection.findOne(query);
        if(isLoggingEnabled){
            mDatabaseLogger.info("getObject: Collection - "+ collectionName +", query - "+ query.toString() +", TimeTaken : "+ (System.currentTimeMillis() - startTime));
        }
        return result;
    }

    public void updateObject(String collectionName, Map queryParams, DBObject newObj) {
        DBCollection collection = getDB().getCollection(collectionName);
        BasicDBObject query = new BasicDBObject(queryParams);
        // Will create new if no matching record is found
        collection.update(query, newObj, true, false);
    }
    
    public void updateObject(String collectionName, Map queryParams, String jsonData) {
    	DBObject dbObject = (DBObject) JSON.parse(jsonData);
        updateObject(collectionName, queryParams, dbObject);
    }

    public WriteResult updateObject(String collectionName, Map queryParams, Map newObj, boolean upsert, boolean multi) {
        DBCollection collection = getDB().getCollection(collectionName);
        BasicDBObject query = new BasicDBObject(queryParams);
        BasicDBObject newObject = new BasicDBObject(newObj);
        return collection.update(query, newObject, upsert, multi);
    }
    
    public List<DBObject> getObjects(String collectionName, Map queryParams) {
        long startTime = System.currentTimeMillis();
        DBCollection collection = getDB().getCollection(collectionName);

        BasicDBObject query = new BasicDBObject(queryParams);
        DBCursor results = collection.find(query);
        List<DBObject> resultsList = new ArrayList<DBObject>();
        while(results.hasNext()) {
            DBObject next = results.next();
            resultsList.add(next);
        }
        if(isLoggingEnabled){
            mDatabaseLogger.info("getObjects: Collection - "+ collectionName +", query - "+ query.toString() +", TimeTaken : "+ (System.currentTimeMillis() - startTime));
        }
        return resultsList;
    }
   
    /**
     * For case insensitive search on the values of queryParams in mongo
     * @param collectionName
     * @param queryParams
     * @param keyToBeEscapedFromCaseInsensitiveSearch
     * @return
     */
    public List<DBObject> getObjectsWithCaseInsensitiveSearch(String collectionName, Map queryParams, String keyToBeEscapedFromCaseInsensitiveSearch) {
        long startTime = System.currentTimeMillis();
        DBCollection collection = getDB().getCollection(collectionName);
        boolean isEscapedKeyPresent = !(StringUtils.isBlank(keyToBeEscapedFromCaseInsensitiveSearch));
        
        BasicDBObjectBuilder patternedQuery =BasicDBObjectBuilder.start();
        
        //for case insensitive search
        for(Iterator<String> itr =queryParams.keySet().iterator(); itr.hasNext(); ){
        	String key = itr.next();
        	if(isEscapedKeyPresent && keyToBeEscapedFromCaseInsensitiveSearch.equalsIgnoreCase(key)){
        		continue;
        	}
        	Pattern pattern = Pattern.compile(queryParams.get(key).toString(), Pattern.CASE_INSENSITIVE);
        	patternedQuery.add(key, pattern);
        }
        if(isEscapedKeyPresent && queryParams.containsKey(keyToBeEscapedFromCaseInsensitiveSearch)){
        	patternedQuery.add(keyToBeEscapedFromCaseInsensitiveSearch,queryParams.get(keyToBeEscapedFromCaseInsensitiveSearch));
        }
        
        BasicDBObject query = (BasicDBObject)patternedQuery.get();
        DBCursor results = collection.find(query);
        
        List<DBObject> resultsList = new ArrayList<DBObject>();
        while(results.hasNext()) {
            DBObject next = results.next();
            resultsList.add(next);
        }
        
        if(isLoggingEnabled){
            mDatabaseLogger.info("getObjects: Collection - "+ collectionName +", query - "+ query.toString() +", TimeTaken : "+ (System.currentTimeMillis() - startTime));
        }
        return resultsList;
    }

    public List<DBObject> getAllObjects(String collectionName) {
        DBCollection collection = getDB().getCollection(collectionName);
        DBCursor cursor = collection.find();
        List<DBObject> resultsList = new ArrayList<DBObject>();
        while(cursor.hasNext()) {
            DBObject next = cursor.next();
            resultsList.add(next);
        }
        return resultsList;
    }

    public DBObject getObject(String collectionName, Map queryParams, Map keys) {
        long startTime = System.currentTimeMillis();
        DBCollection collection = getDB().getCollection(collectionName);
        BasicDBObject query = new BasicDBObject(queryParams);
        BasicDBObject dbkeys = new BasicDBObject(keys);
        DBObject result = collection.findOne(query, dbkeys);
        if(isLoggingEnabled){
            mDatabaseLogger.info("getObject: Collection - "+ collectionName +", query - "+ query.toString() +", keys - "+ dbkeys.toString() +", TimeTaken : "+ (System.currentTimeMillis() - startTime));
        }
        return result;
    }

    public List<DBObject> getObjects(String collectionName, Map queryParams, Map keys) {
        DBCollection collection = getDB().getCollection(collectionName);

        BasicDBObject query = new BasicDBObject(queryParams);
        BasicDBObject dbkeys = new BasicDBObject(keys);

        DBCursor results = collection.find(query, dbkeys);

        List<DBObject> resultsList = new ArrayList<DBObject>();
        while(results.hasNext()) {
            DBObject next = results.next();
            resultsList.add(next);
        }
        return resultsList;
    }

    public static ObjectId getObjectId(String oid) {
        byte[] byts = new BigInteger(oid, 16).toByteArray();
        ObjectId mongoId = new ObjectId(byts);
        return mongoId;
    }

    public static String getObjectIdString(ObjectId mongoId) {
        Base64 base64 = new Base64(true);
        return base64.encodeToString(mongoId.toByteArray());
    }

    public static String generateMongoId() {
        Base64 base64 = new Base64(true);
        ObjectId obj = new ObjectId();
        return base64.encodeToString(obj.toByteArray()).trim();
    }

    // give negative value of n to get all results
    public List<DBObject> getObjects(String collectionName, int startPos, int n, Map queryParams, Map sortingKey) {
        List<DBObject> objList = new ArrayList<DBObject>();
        if(startPos < 0) {
            return objList;
        }
        DBCollection collection = getDB().getCollection(collectionName);
        if(collection == null) {
            return objList;
        }
        BasicDBObject sortBy = new BasicDBObject();
        if(!CollectionUtils.isEmpty(sortingKey)) {
            sortBy.putAll(sortingKey);
            collection.ensureIndex(sortBy);// if index is already present this call has no effect.
        }
        BasicDBObject query = new BasicDBObject(queryParams);
        DBCursor dbCursor = collection.find(query);
        DBCursor sort = dbCursor.sort(sortBy);
        DBCursor skip = sort.skip(startPos);
        DBCursor limit = skip;
        if(n >= 0) {
            limit = skip.limit(n);
        }
        while(limit.hasNext()) {
            DBObject dbObject = limit.next();
            objList.add(dbObject);
        }
        return objList;
    }

    public List<DBObject> getObjects(String collectionName, int startPos, int n, Map queryParams) {
//        return getObjects(collectionName, startPos, n, queryParams, new HashMap<String, Object>());
    	 return getObjects(collectionName, startPos, n, queryParams, null);
    }

    public long getCount(String collectionName, Map queryParams) {
        DBCollection collection = getDB().getCollection(collectionName);
        if(collection == null) {
            return 0;
        }
        DBObject query = new BasicDBObject(queryParams);
        long count = collection.getCount(query);
        return count;
    }

    public long getTotalCount(String collectionName, Map queryParams, String dateField, long fromTime) {
        return getTotalCount(collectionName, queryParams, null, dateField, fromTime);
    }

    public long getTotalCount(String collectionName, Map queryParams, Map<String, List<Object>> keyRangeValueMap, String dateField, long fromTime) {
        DBCollection collection = getDB().getCollection(collectionName);
        if(collection == null) {
            return 0;
        }

        DBObject query = mergeAllANDQueries(new BasicDBObject(queryParams), getExpiryQuery(dateField, fromTime));

        if(keyRangeValueMap != null) {
            for(Entry<String, List<Object>> en : keyRangeValueMap.entrySet()) {
                String fieldName = en.getKey();
                BasicDBList rangeList = new BasicDBList();
                rangeList.addAll(en.getValue());
                DBObject rangeObj = new BasicDBObject("$in", rangeList);
                query.put(fieldName, rangeObj);
            }
        }

        long count = collection.getCount(query);

        return count;
    }

    public static String getIdFromMongoOId(Object jsonObj) {
        if(jsonObj instanceof JSONObject) {
            JSONObject obj = (JSONObject) jsonObj;
            return (String) obj.get("$oid");
        }
        return jsonObj.toString();
    }

    public List<DBObject> getObjectsInDateRange(String collectionName, Map queryParams, String dateField, long fromTime, long toTime, Map sortKey) {
        List<DBObject> objList = new ArrayList<DBObject>();

        DBCollection collection = getDB().getCollection(collectionName);
        if(collection == null) {
            return objList;
        }
        BasicDBObject query = new BasicDBObject(queryParams);
        query.put(dateField, BasicDBObjectBuilder.start("$gte", fromTime).add("$lte", toTime).get());
        DBCursor dbCursor = collection.find(query);
        if(dbCursor != null) {
            if(sortKey != null && sortKey.size() > 0) {
                DBCursor sort = dbCursor.sort(new BasicDBObject(sortKey));
                while(sort.hasNext()) {
                    DBObject dbObject = sort.next();
                    objList.add(dbObject);
                }
            }
            else {
                while(dbCursor.hasNext()) {
                    DBObject dbObject = dbCursor.next();
                    objList.add(dbObject);
                }
            }
        }
        return objList;
    }
    
    public List<DBObject> getObjectsInDateRangeWithOffsetAndCount(String collectionName, Map queryParams, String dateField, long fromTime, long toTime, Map sortKey,int n,int startPos) {
        List<DBObject> objList = new ArrayList<DBObject>();

        DBCollection collection = getDB().getCollection(collectionName);
        if(collection == null) {
            return objList;
        }
        BasicDBObject query = new BasicDBObject(queryParams);
        query.put(dateField, BasicDBObjectBuilder.start("$gte", fromTime).add("$lte", toTime).get());
        DBCursor dbCursor = collection.find(query);
        if(dbCursor != null) {
            if(sortKey != null && sortKey.size() > 0) {
                DBCursor sort = dbCursor.sort(new BasicDBObject(sortKey));
                DBCursor skip = sort.skip(startPos);
    	        DBCursor limit = skip;
    	        if(n >= 0) {
    	            limit = skip.limit(n);
    	        }
                while(limit.hasNext()) {
                    DBObject dbObject = limit.next();
                    objList.add(dbObject);
                }
            }
            else {
            	DBCursor skip = dbCursor.skip(startPos);
    	        DBCursor limit = skip;
    	        if(n >= 0) {
    	            limit = skip.limit(n);
    	        }
                while(limit.hasNext()) {
                    DBObject dbObject = limit.next();
                    objList.add(dbObject);
                }
            }
        }
        return objList;
    }

    // will return only fields given in field list
    public List<DBObject> getPartialObjectsInDateRange(String collectionName, Map queryParams, List<String> fieldList, String dateField, long fromTime, long toTime, Map sortKey) {
        List<DBObject> objList = new ArrayList<DBObject>();

        DBCollection collection = getDB().getCollection(collectionName);
        if(collection == null) {
            return objList;
        }
        BasicDBObject query = new BasicDBObject(queryParams);
        query.put(dateField, BasicDBObjectBuilder.start("$gte", fromTime).add("$lte", toTime).get());
        Map fieldMap = new HashMap();
        for(String field : fieldList) {
            fieldMap.put(field, 1);
        }
        DBCursor dbCursor = collection.find(query, new BasicDBObject(fieldMap));
        if(dbCursor != null) {
            if(sortKey != null && sortKey.size() > 0) {
                DBCursor sort = dbCursor.sort(new BasicDBObject(sortKey));
                while(sort.hasNext()) {
                    DBObject dbObject = sort.next();
                    objList.add(dbObject);
                }
            }
            else {
                while(dbCursor.hasNext()) {
                    DBObject dbObject = dbCursor.next();
                    objList.add(dbObject);
                }
            }
        }
        return objList;
    }

    // will return only fields given in field list
    public List<DBObject> getPartialObjects(String collectionName, Map<String, Object> queryParams, List<String> fieldList, Map<String, Object> sortKey) {
        Map fieldMap = new HashMap();
        for(String field : fieldList) {
            fieldMap.put(field, 1);
        }
        return getPartialObjects(collectionName, queryParams, fieldMap, sortKey);
    }

    public List<DBObject> getPartialObjects(String collectionName, Map<String, Object> queryParams, Map fieldParams, Map sortKey) {
        return getPartialObjects(collectionName, 0, -1, queryParams, fieldParams, sortKey);
    }

    // give negative value of n to get all results
    public List<DBObject> getPartialObjects(String collectionName, int startPos, int n, Map queryParams, Map fieldParams, Map sortingKey) {
        List<DBObject> objList = new ArrayList<DBObject>();
        if(startPos < 0) {
            return objList;
        }
        DBCollection collection = getDB().getCollection(collectionName);
        if(collection == null) {
            return objList;
        }
        BasicDBObject sortBy = null;
        if(sortingKey != null) {
            sortBy = new BasicDBObject(sortingKey);
            collection.ensureIndex(sortBy);// if index is already present this call has no effect.
        }
        BasicDBObject query = new BasicDBObject(queryParams);
        DBCursor dbCursor = collection.find(query, new BasicDBObject(fieldParams));
        DBCursor skip = dbCursor.skip(startPos);
        DBCursor limit = skip;
        if(n >= 0) {
            limit = skip.limit(n);
        }
        DBCursor sort = limit;
        if(sortBy != null) {
            sort = limit.sort(sortBy);
        }
        while(sort.hasNext()) {
            DBObject dbObject = sort.next();
            objList.add(dbObject);
        }
        return objList;
    }

    public boolean setField(String collectionName, Map<String, Object> queryParams, Map<String, Object> fieldValueMap) {
       return setField(collectionName, queryParams, fieldValueMap, true, false);
    }
    
    public boolean setField(String collectionName, Map<String, Object> queryParams, Map<String, Object> fieldValueMap, boolean upsert) {
        return setField(collectionName, queryParams, fieldValueMap, upsert, false);
     }
    
    public boolean setField(String collectionName, Map<String, Object> queryParams, Map<String, Object> fieldValueMap, boolean upsert, boolean multi) {
        long startTime = System.currentTimeMillis();
        DBCollection collection = getDB().getCollection(collectionName);
        BasicDBObject query = new BasicDBObject(queryParams);
        logger.debug("query = "+query.toString());
        BasicDBObjectBuilder builder = BasicDBObjectBuilder.start("$set", new BasicDBObject(fieldValueMap));
        DBObject setField = builder.get();
        logger.debug("Query to be run : " + setField.toString());
        WriteResult writeResult = null;
        try {
            writeResult = collection.update(query, setField, upsert, multi);
            if(isLoggingEnabled){
                mDatabaseLogger.info("setField: Collection - "+ collectionName +", query - "+ query.toString() +" setField - "+ setField.toString() +" , TimeTaken : "+ (System.currentTimeMillis() - startTime));
            }
            return true;
        } catch (MongoException e) {
            logger.error("Error setField: Collection - "+ collectionName +", query - "+ query.toString() +" setField - "+ setField.toString()+". Erorr : "+e.getMessage(),e );
            LogstashLoggerUtils.createFatalExceptionLogWithMessage(e, 
                    ExceptionTypeEnum.INFRA.MONGO.name(),
                    "",
                    "MongoDBManager.setField", 
                    "Error setField " + collectionName);
            
            return false;
        }
//        return writeResult.getLastError().ok();
    }
    
    public boolean pushAllField(String collectionName, Map<String, Object> queryParams, Map<String, Object> fieldValueMap) {
      return pushAllField(collectionName, queryParams, fieldValueMap, false);
    }

    /**
     * See : http://docs.mongodb.org/manual/reference/operator/pushAll/
     * The $pushAll operator is similar to the $push but adds the ability to append several values to an array at once.
     * @param collectionName
     * @param queryParams
     * @param fieldValueMap
     * @return
     */
    public boolean pushAllField(String collectionName, Map<String, Object> queryParams, Map<String, Object> fieldValueMap,boolean upsert) {
        DBCollection collection = getDB().getCollection(collectionName);
        BasicDBObject query = new BasicDBObject(queryParams);
        logger.debug("query = "+query.toString());
        BasicDBObjectBuilder builder = BasicDBObjectBuilder.start("$pushAll", new BasicDBObject(fieldValueMap));
        DBObject pushAllField = builder.get();
        logger.debug("Query to be run : " + pushAllField.toString());
        WriteResult writeResult = collection.update(query, pushAllField, upsert, false);
        return writeResult.getLastError().ok();
    }
    
    public List<DBObject> getObjects(String collectionName, int startPos, int numResults, Map<String, Object> queryParamsMap, Map<String, List<Object>> keyRangeValueMap, Map<String, Object> sortingMap) {
        DBObject query = new BasicDBObject(queryParamsMap);
        if(keyRangeValueMap != null) {
            for(Entry<String, List<Object>> en : keyRangeValueMap.entrySet()) {
                String fieldName = en.getKey();
                BasicDBList rangeList = new BasicDBList();
                rangeList.addAll(en.getValue());
                DBObject rangeObj = new BasicDBObject("$in", rangeList);
                query.put(fieldName, rangeObj);
            }
        }

        return getData(collectionName, query, null, sortingMap, startPos, numResults);
    }
    
    public static void main(String[] args) {
        DBObject one = new BasicDBObject("categoryId", "1");
        DBObject two = getExpiryQuery("expiryTime", System.currentTimeMillis());
        // DBObject two = new BasicDBObject("expiryTime", new BasicDBObject("$not", new
        // BasicDBObject("$lt", 0)));

        DBObject f = new BasicDBObject();
        f.putAll(one);
        f.putAll(two);

        // BasicDBObject query = new BasicDBObject();
        // query.put("id", "1");
        //
        // BasicDBObject clause1 = new BasicDBObject("post_title", "1");
        // BasicDBObject clause2 = new BasicDBObject("post_description", "2");
        // BasicDBList clauses = new BasicDBList();
        // clauses.add(clause1);
        // clauses.add(clause2);
        //
        // query.put("$or", clauses);
        //
        // Map m = query.toMap();
        // if(m instanceof LinkedHashMap) {
        // System.out.println("LinkedHashMap");
        // }
        // else if(m instanceof HashMap) {
        // System.out.println("HashMap");
        // }
        // System.out.println(m);
        //
        // for(Object obj : m.values()) {
        // System.out.println(obj.getClass().getName());
        // }
        // // {$or:[{expiry:{$exists:false}},{expiry:{$exists:true, $gte:40}}]}
    }

    public List<DBObject> getObjectsInDateRangeIfExist(String collectionName, Map<String, Object> queryParams, Map<String, Object> fieldsParam, Map<String, Object> sortingMap, String dateField,
            long fromTime, long toTime, int numResults, int startPos) {

        return getData(collectionName, mergeAllANDQueries(new BasicDBObject(queryParams), getExpiryQuery(dateField, fromTime)), fieldsParam, sortingMap, startPos, numResults);
    }

    private static DBObject getExpiryQuery(String dateField, long fromTime) {
        DBObject finalQuery = new BasicDBObject();

        BasicDBList orQueries = new BasicDBList();
        orQueries.add(new BasicDBObject(dateField, new BasicDBObject("$not", new BasicDBObject("$lt", fromTime))));
        orQueries.add(new BasicDBObject(dateField, 0));

        finalQuery.put("$or", orQueries);

        return finalQuery;
    }

    public DBObject mergeAllANDQueries(DBObject... andQueries) {
        if(andQueries == null || andQueries.length == 0) {
            return null;
        }
        DBObject finalQuery = new BasicDBObject();
        for(DBObject andQuery : andQueries) {
            finalQuery.putAll(andQuery);
        }
        return finalQuery;
    }

    public DBObject getObject(String collectionName, String objectId, boolean encoded) {
        DBCollection collection = getDB().getCollection(collectionName);

        BasicDBObject query = new BasicDBObject();
        query.put(MONGO_ID, getObjectId(objectId, encoded));
        DBObject result = collection.findOne(query);
        return result;
    }

    public static ObjectId getObjectId(String oid, boolean encoded) {
        if(!encoded) {
            return getObjectId(oid);
        }
        Base64 base64 = new Base64(true);
        byte[] decode = base64.decode(oid);
        ObjectId mongoId = new ObjectId(decode);
        return mongoId;
    }

    public static boolean isValidObjectId(String s) {
        if(s == null) {
            return false;
        }
        int len = s.length();
        if(len != 24) {
            return false;
        }
        for(int i = 0; i < len; ++i) {
            char c = s.charAt(i);
            if((c >= '0') && (c <= '9')) {
                continue;
            }
            if((c >= 'a') && (c <= 'f')) {
                continue;
            }
            if((c < 'A') || (c > 'F')) {
                return false;
            }
        }
        return true;
    }

    /**
     * 
     * @param collectionName
     * @param query
     * @param sortingMap
     * @param fieldsParam
     *            Map containing fields to be returned. Null to return all the fields
     * @param startPos
     * @param numResults
     * @return
     */
    private List<DBObject> getData(String collectionName, DBObject query, Map<String, Object> fieldsParam, Map<String, Object> sortingMap, int startPos, int numResults) {
        DBCollection dbCollection = getDB().getCollection(collectionName);
        if(sortingMap != null) {
            BasicDBObject sortBy = new BasicDBObject(sortingMap);
            dbCollection.ensureIndex(sortBy);
        }
        List<DBObject> resultList = new ArrayList<DBObject>();
        DBCursor cursor = null;
        if(fieldsParam == null)
            cursor = dbCollection.find(query);
        else
            cursor = dbCollection.find(query, new BasicDBObject(fieldsParam));
        if(cursor == null)
            return resultList; // empty list
        if(startPos >= 0)
            cursor = cursor.skip(startPos);
        if(numResults >= 0)
            cursor = cursor.limit(numResults);
        if(sortingMap != null && sortingMap.size() > 0)
            cursor = cursor.sort(new BasicDBObject(sortingMap));
        while(cursor.hasNext())
            resultList.add(cursor.next());
        return resultList;
    }

    /*
     * This method requires mongo-java-client-2.9.x.jar and currently we are using 2.7.x version.
     * Commenting out (and not deleting) as we might require this method in future
     */
    // public List<DBObject> aggregate(String collectionName, DBObject firstOperation, DBObject ...
    // additionalOperations) {
    // if (firstOperation == null)
    // throw new
    // IllegalArgumentException("At least one aggregate operation (DBObject) is required for aggregation. Collection Name: "
    // + collectionName);
    // List<DBObject> list = new ArrayList<DBObject>();
    // DBCollection collection = getDB().getCollection(collectionName);
    // AggregationOutput output = collection.aggregate(firstOperation, additionalOperations);
    // for (DBObject result : output.results())
    // list.add(result);
    // return list;
    // }

    public List<DBObject> mapReduce(String collectionName, DBObject query, String map, String reduce, String finalize, Map<String, Object> extraOptions) {
        List<DBObject> list = new ArrayList<DBObject>();
        DBCollection collection = getDB().getCollection(collectionName);

        // collection.ensureIndex("user_id");
        // collection.ensureIndex(new BasicDBObject("delivered", 1));

        // Map<String, Object> sortingParam = new HashMap<String, Object>();
        // sortingParam.put("delivered", -1);
        // collection.ensureIndex(new BasicDBObject(sortingParam));

        MapReduceCommand cmd = new MapReduceCommand(collection, map, reduce, null, OutputType.INLINE, query);
        if(finalize != null && !finalize.trim().isEmpty()) {
            cmd.setFinalize(finalize);
        }
        if(extraOptions != null) {
            for(Entry<String, Object> entry : extraOptions.entrySet())
                cmd.addExtraOption(entry.getKey(), entry.getValue());
        }
        MapReduceOutput out = collection.mapReduce(cmd);
        for(DBObject dbObject : out.results())
            list.add(dbObject);
        return list;
    }


    public long getTotalCount(String collectionName, Map queryParams) {
        DBCollection collection = getDB().getCollection(collectionName);
        if(collection == null)
            return 0;
        DBObject query = new BasicDBObject(queryParams);
        long count = collection.getCount(query);
        return count;
    }

    public long getTotalCount(String collectionName, Map queryParams, Map<String, List<Object>> keyRangeValueMap) {
        DBCollection collection = getDB().getCollection(collectionName);
        if(collection == null)
            return 0;
        DBObject query = new BasicDBObject(queryParams);
        if(keyRangeValueMap != null) {
            for(Entry<String, List<Object>> en : keyRangeValueMap.entrySet()) {
                String fieldName = en.getKey();
                BasicDBList rangeList = new BasicDBList();
                rangeList.addAll(en.getValue());
                DBObject rangeObj = new BasicDBObject("$in", rangeList);
                query.put(fieldName, rangeObj);
            }
        }
        long count = collection.getCount(query);
        return count;
    }
    
    public boolean incrementField(String collectionName, Map<String, Object> queryParams, Map<String, Object> fieldValueMap, boolean upsert) {
        DBCollection collection = getDB().getCollection(collectionName);
        BasicDBObject query = new BasicDBObject(queryParams);
        logger.debug("query = "+query.toString());
        BasicDBObjectBuilder builder = BasicDBObjectBuilder.start("$inc", new BasicDBObject(fieldValueMap));
        DBObject incField = builder.get();
        logger.debug("Query to be run : " + incField.toString());
        try {
            collection.update(query, incField, upsert, false);
            return true;
        } catch (MongoException e) {
            logger.error("Error incrementField : "+collectionName+" : "+e.getMessage(),e);
            LogstashLoggerUtils.createFatalExceptionLogWithMessage(e, 
                    ExceptionTypeEnum.INFRA.MONGO.name(),
                    "",
                    "MongoDBManager.incrementField", 
                    "Error increment field " + collectionName);

            return false;
        }

    }
    
    public List<DBObject> getObjectsWithANDQueryInDateRangeIfExist(String collectionName,
    		Map<Object, List<Object>> queryParams, String dateField, long fromTime, long toTime, Map sortKey, int n,
    		int startPos) {

    	DBCollection collection = getDB().getCollection(collectionName);
    	BasicDBObject andQuery = new BasicDBObject();
    	List<BasicDBObject> dbObjectList = new ArrayList<BasicDBObject>();
    	for (Entry<Object, List<Object>> entry : queryParams.entrySet()) {
    		for (Object paramvalue : entry.getValue()) {
    			dbObjectList.add(new BasicDBObject((String) entry.getKey(), paramvalue));
    		}

    	}
    	andQuery.put("$and", dbObjectList);
    	if(StringUtils.isNotBlank(dateField) && fromTime<toTime)
    		andQuery.put(dateField, BasicDBObjectBuilder.start("$gte", fromTime).add("$lte", toTime).get());
    	DBCursor dbCursor = collection.find(andQuery);

    	List<DBObject> resultsList = new ArrayList<DBObject>();
    	if(dbCursor != null) {
    		if(sortKey != null && sortKey.size() > 0) {
    			DBCursor sort = dbCursor.sort(new BasicDBObject(sortKey));
    			DBCursor skip = sort.skip(startPos);
    			DBCursor limit = skip;
    			if(n >= 0) {
    				limit = skip.limit(n);
    			}
    			while(limit.hasNext()) {
    				DBObject dbObject = limit.next();
    				resultsList.add(dbObject);
    			}
    		}
    		else {
    			while(dbCursor.hasNext()) {
    				DBObject dbObject = dbCursor.next();
    				resultsList.add(dbObject);
    			}
    		}
    	}
    	return resultsList;

    }

    /**
     * For deleting nth object matching with specified field values, from an array
     * @param collectionName
     * @param queryParams
     * @param fieldValueMap
     * @return
     *
     * @return
     */
    public boolean pullField(String collectionName, Map<String, Object> queryParams, Map<String, Object> fieldValueMap,boolean upsert) {
        DBCollection collection = getDB().getCollection(collectionName);
        BasicDBObject query = new BasicDBObject(queryParams);
        logger.debug("query = "+query.toString());
        BasicDBObjectBuilder builder = BasicDBObjectBuilder.start("$pull", new BasicDBObject(fieldValueMap));
        DBObject pullField = builder.get();
        logger.debug("Query to be run : " + pullField.toString());
        WriteResult writeResult = collection.update(query, pullField, upsert, false);
        return writeResult.getLastError().ok();
    }
    
    public void addObjectForS3(String title,
                               String id,
                               String publisher,
                               String categoryId,
                               String price) {
        DBCollection collection = mongo.getDB("cmsdb").getCollection(AMAZON_S3_COLLECTION);
        
        AmazonS3Publisher data = new AmazonS3Publisher(price, categoryId, id, publisher, title);
        data.setLastUpdated(System.currentTimeMillis());
        collection.insert((DBObject) JSON.parse(data.toJson().toString()));
    }
    
    public List<DBObject> getObjectsToUpload() {

        DBCollection collection = mongo.getDB("cmsdb").getCollection(AMAZON_S3_COLLECTION);
        BasicDBObject andQuery = new BasicDBObject();

        long toTime = System.currentTimeMillis();
        long fromTime = toTime - 60*60*1000;
        String dateFieldName = "lastUpdated";
        
        if(StringUtils.isNotBlank(dateFieldName) && fromTime<toTime)
            andQuery.put(dateFieldName, BasicDBObjectBuilder.start("$gte", fromTime).add("$lte", toTime).get());
        DBCursor dbCursor = collection.find(andQuery);

        List<DBObject> resultsList = new ArrayList<DBObject>();
        if(dbCursor != null) {
            while(dbCursor.hasNext()) {
                DBObject dbObject = dbCursor.next();
                resultsList.add(dbObject);
            }
        }
        return resultsList;
    }
    
    public boolean deleteObjectFromS3(DBObject object) {
        DBCollection collection = mongo.getDB("cmsdb").getCollection(AMAZON_S3_COLLECTION);
        if(object != null) {
            collection.remove(object);
            return true;
        }
        else {
            return false;
        }
    }
    
    public List<DBObject> getObjectsInOidRange(String collectionName, Map queryParams, Map fieldParams, ObjectId start, ObjectId end) {
        List<DBObject> objList = new ArrayList<DBObject>();

        DBCollection collection = getDB().getCollection(collectionName);
        if (collection == null) {
            return objList;
        }

        BasicDBObjectBuilder basicDBObjectBuilder = BasicDBObjectBuilder.start("$gte", start);
        if (end != null) {
            basicDBObjectBuilder.add("$lt", end);
        }

        BasicDBObject query = new BasicDBObject(queryParams);
        query.put(MONGO_ID, basicDBObjectBuilder.get());

        DBCursor dbCursor = collection.find(query, new BasicDBObject(fieldParams)).sort(new BasicDBObject(MONGO_ID, 1));
        if (dbCursor != null) {
            while (dbCursor.hasNext()) {
                DBObject dbObject = dbCursor.next();
                objList.add(dbObject);
            }
        }
        return objList;
    }

    public List<ObjectId> getObjectsIdsRanges(String collectionName, Map queryParams, int range) {
        List<ObjectId> objectIds = new ArrayList<>();
        ObjectId start = getFirstOrLastObjectId(collectionName, queryParams);
        objectIds.add(start);

        while (true) {
            start = getObjectIdInRange(collectionName, queryParams, range, start);
            if (start == null) {
                break;
            }
            /*if (!start.equals(last)) {
                objectIds.add(start);
            }*/
            objectIds.add(start);
        }
        //objectIds.add(last);
        return objectIds;
    }

    public ObjectId getFirstOrLastObjectId(String collectionName, Map queryParams) {
        DBCollection collection = getDB().getCollection(collectionName);
        if (collection == null) {
            return null;
        }

        BasicDBObject query = new BasicDBObject(queryParams);

        BasicDBObject fields = new BasicDBObject();
        fields.append(MONGO_ID, 1);

        DBCursor dbCursor = collection.find(query, fields).limit(1).sort(new BasicDBObject(MONGO_ID, 1));
        if (dbCursor != null && dbCursor.hasNext()) {
            return (ObjectId) dbCursor.next().get(MONGO_ID);
        }
        return null;
    }

    public ObjectId getObjectIdInRange(String collectionName, Map queryParams, int range, ObjectId start) {
        DBCollection collection = getDB().getCollection(collectionName);
        if (collection == null) {
            return null;
        }

        BasicDBObject query = new BasicDBObject(queryParams);
        if (start != null) {
            query.put(MONGO_ID, BasicDBObjectBuilder.start("$gt", start).get());
        }

        BasicDBObject fields = new BasicDBObject();
        fields.append(MONGO_ID, 1);

        DBCursor dbCursor = collection.find(query, fields).skip(range).limit(1).sort(new BasicDBObject(MONGO_ID, 1));
        if (dbCursor != null && dbCursor.hasNext()) {
            return (ObjectId) dbCursor.next().get(MONGO_ID);
        }
        return null;
    }
}
