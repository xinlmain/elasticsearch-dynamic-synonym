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
package com.ginobefunny.elasticsearch.plugins.synonym.service.utils;

import com.ginobefunny.elasticsearch.plugins.synonym.service.Configuration;
import com.ginobefunny.elasticsearch.plugins.synonym.service.SynonymRuleManager;
import com.ginobefunny.elasticsearch.plugins.synonym.service.SynonymRulesReader;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import java.util.List;

/**
 * Created by ginozhang on 2017/1/12.
 */
public class Monitor implements Runnable {

    private static Logger logger = ESLoggerFactory.getLogger("dynamic-synonym");

    private SynonymRulesReader rulesReader;

    public Monitor(SynonymRulesReader reader) {
        this.rulesReader = reader;
    }

    @Override
    public void run() {
        try {
            if (rulesReader.isNeedReloadSynonymRules()) {
                List<String> newRules = rulesReader.reloadSynonymRules();
                SynonymRuleManager.getSingleton().reloadSynonymRule(newRules);
            }
        } catch (Exception e) {
            logger.error("Failed to reload synonym rule!", e);
        }
    }

}
