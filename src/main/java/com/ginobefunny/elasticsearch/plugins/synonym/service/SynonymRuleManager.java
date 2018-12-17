/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ginobefunny.elasticsearch.plugins.synonym.service;

import com.ginobefunny.elasticsearch.plugins.synonym.service.utils.Monitor;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Created by ginozhang on 2017/1/12.
 */
public class SynonymRuleManager {

    private static Logger LOGGER = ESLoggerFactory.getLogger("dynamic-synonym-manager");

    private static final int CHECK_SYNONYM_INTERVAL = 60;

    private static final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "monitor-thread");
        }
    });

    private static SynonymRuleManager singleton;

    private Configuration configuration;

    private SimpleSynonymMap synonymMap;

    SynonymRulesReader synonymRulesReader;

    public static synchronized SynonymRuleManager initial(Configuration cfg) {
        if (singleton == null) {
            synchronized (SynonymRuleManager.class) {
                if (singleton == null) {
                    singleton = new SynonymRuleManager();
                    singleton.configuration = cfg;
                    singleton.synonymRulesReader = new RemoteSynonymRulesReader(cfg.getRemoteUrl());

                    //TODO: 根据index配置判断使用db还是远程服务，这里写死了远程服务。
                    //singleton.synonymRulesReader = new DatabaseSynonymRulesReader(cfg.getDBUrl());
                    singleton.reloadSynonymRule(singleton.synonymRulesReader.reloadSynonymRules());
                    executorService.scheduleWithFixedDelay(new Monitor(singleton.synonymRulesReader), 1,
                        cfg.getInterval(), TimeUnit.SECONDS);
                }
            }
        }

        return singleton;
    }

    public static SynonymRuleManager getSingleton() {
        if (singleton == null) {
            throw new IllegalStateException("Please initialize first.");
        }
        return singleton;
    }

    public List<String> getSynonymWords(String inputToken) {
        if (this.synonymMap == null) {
            return null;
        }

        return this.synonymMap.getSynonymWords(inputToken);
    }

    public boolean reloadSynonymRule(List<String> rules) {
        LOGGER.info("Start to reload synonym rule...");
        if (rules == null || rules.size() == 0) {
            LOGGER.error("synonym rule is empty, return...");
            return false;
        }
        boolean reloadResult = true;
        try {
            SimpleSynonymMap tempSynonymMap = new SimpleSynonymMap(this.configuration);
            for (String rule : rules) {
                tempSynonymMap.addRule(rule);
            }

            this.synonymMap = tempSynonymMap;
            LOGGER.info("Succeeded to reload {} synonym rule!", rules.size());
        } catch (Throwable t) {
            LOGGER.error("Failed to reload synonym rule!", t);
            reloadResult = false;
        }

        return reloadResult;
    }
}
