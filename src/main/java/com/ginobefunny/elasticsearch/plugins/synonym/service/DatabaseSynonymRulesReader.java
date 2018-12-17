package com.ginobefunny.elasticsearch.plugins.synonym.service;

import com.ginobefunny.elasticsearch.plugins.synonym.service.utils.JDBCUtils;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import java.util.List;

public class DatabaseSynonymRulesReader implements SynonymRulesReader {
    private static Logger logger = ESLoggerFactory.getLogger("dynamic-synonym");

    private String dbUrl;

    private long currentMaxVersion;
    private long lastUpdateVersion;

    public DatabaseSynonymRulesReader(String dbUrl) {
        this.dbUrl = dbUrl;

        isNeedReloadSynonymRules();
    }

    @Override
    public boolean isNeedReloadSynonymRules() {
        try {
            currentMaxVersion = JDBCUtils.queryMaxSynonymRuleVersion(this.dbUrl);
            logger.info("remote synonym version is: {}, local version is: {}, need to reload.", currentMaxVersion, lastUpdateVersion);
        } catch (Exception e) {
            logger.error("query db synonym rule max version error: {}", e.getMessage());
        }

        return lastUpdateVersion < currentMaxVersion;
    }

    @Override
    public List<String> reloadSynonymRules() {
        try {
            //TODO: here we assume the reloading is always successful, which is not.
            lastUpdateVersion = currentMaxVersion;
            return JDBCUtils.querySynonymRules(dbUrl, currentMaxVersion);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
