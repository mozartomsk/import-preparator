package com.tradeshift.productengine.filepreparator.translations;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;


/**
 * HTreeMap (HashMap) can be used as cache, where items are removed after timeout or when maximal size is reached.
 */
public class Cache implements Closeable {

    private final Map<CacheKey, String> instance;
    private final DB db;

    public Cache(String filePath, String cacheInnerName) throws IOException {
        File dbFile = new File(filePath);
        if (!dbFile.exists()) {
            dbFile.createNewFile();
        }

        db = DBMaker.newFileDB(dbFile)
                .sizeLimit(2)
                .transactionDisable()
                .make();

        if (db.exists(cacheInnerName)) {
            instance =  db
                    .getHashMap(cacheInnerName);
        } else {
            instance = db
                    .createHashMap(cacheInnerName)
                    .make();
        }

        System.out.println("Cache loadad, items count: " + instance.size());
    }

    @Override
    public void close() {
        System.out.println("Cache closed, items count: " + instance.size());
        if (db != null) {
            db.commit();
            db.close();
        }
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class CacheKey implements Serializable {
        private String fromLanguage, toLanguage, source;
    }

    public void put(String fromLanguage, String toLanguage, String source, String translation) {
        instance.put(new CacheKey(fromLanguage, toLanguage, source), translation);
        db.commit();
    }

    public String get(String fromLanguage, String toLanguage, String source) {
        return instance.get(new CacheKey(fromLanguage, toLanguage, source));
    }

    public boolean containsKey(String fromLanguage, String toLanguage, String source) {
        return instance.containsKey(new CacheKey(fromLanguage, toLanguage, source));
    }
}