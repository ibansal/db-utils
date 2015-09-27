package com.snapdeal.bhasha.services;

import com.aerospike.client.*;
import com.aerospike.client.policy.*;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class AeroSpikeDBManager {

    private static Logger logger = LoggerFactory.getLogger(AeroSpikeDBManager.class);

    private static final String ALL = "!>all";

    private AerospikeClient client;

    public AeroSpikeDBManager(String host, int port) {
        ClientPolicy policy = new ClientPolicy();
        Host hosts = new Host(host, port);
        client = new AerospikeClient(policy, hosts);
    }

    public AeroSpikeDBManager(List<String> hostUrls) {
        ClientPolicy policy = new ClientPolicy();
        List<Host> hostList = new ArrayList<>();
        for (String h : hostUrls) {
            String host = h;
            int port = 3000;
            if (h.indexOf(':') > 0) {
                host = h.substring(0, h.indexOf(':'));
                host = host.trim();
                String portString = h.substring(h.indexOf(':') + 1).trim();
                try {
                    port = Integer.parseInt(portString);
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
            host = host.trim();
            Host dbHost = new Host(host, port);
            hostList.add(dbHost);
        }
        Host[] hosts = hostList.toArray(new Host[0]);
        client = new AerospikeClient(policy, hosts);
    }

    public AerospikeClient getClient() {
        return client;
    }

    public void insertOrUpdate(String dbName, String table, String key, JSONObject value) {
        insertOrUpdate(dbName, table, key, value, -1);
    }

    public void insertOrUpdate(String dbName, String table, String key, JSONObject value, int ttl) {
        WritePolicy policy = new WritePolicy();
        policy.recordExistsAction = RecordExistsAction.UPDATE;
        policy.expiration = ttl;
        insert(policy, dbName, table, key, value);
    }

    public void insertOrReplace(String dbName, String table, String key, JSONObject value) {
        insertOrReplace(dbName, table, key, value, -1);
    }

    public void insertOrReplace(String dbName, String table, String key, JSONObject value, int ttl) {
        WritePolicy policy = new WritePolicy();
        policy.recordExistsAction = RecordExistsAction.REPLACE;
        policy.expiration = ttl;
        insert(policy, dbName, table, key, value);
    }

    public void insert(String dbName, String table, String key, JSONObject value) {
        insert(dbName, table, key, value, -1);
    }

    public void insert(String dbName, String table, String key, JSONObject value, int ttl) {
        WritePolicy policy = new WritePolicy();
        policy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
        policy.expiration = ttl;
        insert(policy, dbName, table, key, value);
    }

    public void insert(WritePolicy policy, String dbName, String table, String key, JSONObject value) {
        ArrayList<Bin> bins = new ArrayList<Bin>();
        if (value != null) {
            for (Object en : value.entrySet()) {
                if (en instanceof Entry) {
                    Entry entry = (Entry) en;
                    Object jsonkey = entry.getKey();
                    Object jsonValue = entry.getValue();
                    if (jsonkey instanceof String) {
                        String keyStr = (String) jsonkey;
                        Bin bin = new Bin(keyStr, jsonValue);
                        bins.add(bin);
                    }
                }
            }
        }
        client.put(policy, new Key(dbName, table, key), bins.toArray(new Bin[0]));
    }

    public void set(String dbName, String table, String key, String value) {
        set(dbName, table, key, value, -1);
    }

    public void set(String dbName, String table, String key, Object value, int ttl) {
        WritePolicy policy = new WritePolicy();
        policy.expiration = ttl;
        policy.recordExistsAction = RecordExistsAction.UPDATE;
        client.put(policy, new Key(dbName, table, key), new Bin("value", value));
    }

    public boolean exists(String dbName, String table, String key) {
        try {
            Policy policy = new Policy();
            boolean exists = client.exists(policy, new Key(dbName, table, key));
            return exists;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return false;
    }

    public List<String> exists(String dbName, String table, List<String> keys) {
        try {
            if (keys == null) {
                return null;
            }
            List<String> list = new ArrayList<>();
            BatchPolicy policy = new BatchPolicy();
            Key[] keyArr = new Key[keys.size()];
            for (int i = 0; i < keys.size(); i++) {
                keyArr[i] = new Key(dbName, table, keys.get(i));
            }
            boolean[] existArr = client.exists(policy, keyArr);
            for (int i = 0; i < keys.size(); i++) {
                if (existArr[i]) {
                    list.add(keys.get(i));
                }
            }
            return list;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    public String get(String dbName, String table, String key) {
        try {
            Policy policy = new Policy();
            Record record = client.get(policy, new Key(dbName, table, key));
            if (record != null) {
                String value = (String) record.getValue("value");
                return value;
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    public List<String> getAll(String dbName, String table, List<String> keys) {
        if (keys == null || keys.size() == 0) {
            return null;
        }
        try {
            BatchPolicy policy = new BatchPolicy();
            Key[] keyArr = new Key[keys.size()];
            for (int i = 0; i < keys.size(); i++) {
                keyArr[i] = new Key(dbName, table, keys.get(i));
            }
            Record[] records = client.get(policy, keyArr);
            List<String> dbrs = new ArrayList<>();
            if (records != null) {
                for (Record record : records) {
                    if (record == null) {
                        continue;
                    }
                    dbrs.add((String) record.getValue("value"));
                }
            }
            return dbrs;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }


    public boolean delete(String dbName, String table, String key) {
        try {
            WritePolicy policy = new WritePolicy();
            boolean delete = client.delete(policy, new Key(dbName, table, key));
            return delete;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return false;
    }

    public void deleteAll(String dbName, String table) {
        final List<Key> keys = new ArrayList<>();
        client.scanAll(new ScanPolicy(), dbName, table, new ScanCallback() {

            @Override
            public void scanCallback(Key arg0, Record r) throws AerospikeException {
                keys.add(arg0);
            }
        });
        for(Key k:keys){
            client.delete(new WritePolicy(), k);
        }
    }

    public void incrementValue(String dbName, String table, String key, Map<String, Number> binValueMap) {
        try {
            if (binValueMap == null || binValueMap.size() == 0) {
                return;
            }
            Bin[] bins = new Bin[binValueMap.size()];
            int pos = 0;
            for (Entry<String, Number> en : binValueMap.entrySet()) {
                bins[pos] = new Bin(en.getKey(), en.getValue().longValue());
                pos++;
            }
            WritePolicy policy = new WritePolicy();
            client.add(policy, new Key(dbName, table, key), bins);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void incrementValue(String dbName, String table, String key, Map<String, Number> binValueMap, int ttl) {
        try {
            if (binValueMap == null || binValueMap.size() == 0) {
                return;
            }
            Bin[] bins = new Bin[binValueMap.size()];
            int pos = 0;
            for (Entry<String, Number> en : binValueMap.entrySet()) {
                bins[pos] = new Bin(en.getKey(), en.getValue().longValue());
                pos++;
            }
            WritePolicy policy = new WritePolicy();
            policy.expiration = ttl;
            client.add(policy, new Key(dbName, table, key), bins);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void scanAll(String databaseName, String tableName, ScanCallback scanCallback) {
        try {
            ScanPolicy policy = new ScanPolicy();
            policy.concurrentNodes = true;
            policy.priority = Priority.DEFAULT;
            policy.includeBinData = true;
//            client.(policy, databaseName, tableName, scanCallback);
        }
        finally {
            client.close();
        }
    }
}
