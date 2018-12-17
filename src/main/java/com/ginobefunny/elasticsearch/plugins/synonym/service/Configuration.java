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

import org.apache.lucene.analysis.Analyzer;

/**
 * Created by ginozhang on 2017/1/12.
 */
public class Configuration {

    private final boolean ignoreCase;

    private final boolean expand;

    private final String dbUrl;

    private final String remoteUrl;

    private final Analyzer analyzer;

    private final Integer interval;

    public Configuration(boolean ignoreCase, boolean expand, Analyzer analyzer, String dbUrl, String remoteUrl, Integer interval) {
        this.ignoreCase = ignoreCase;
        this.expand = expand;
        this.analyzer = analyzer;
        this.dbUrl = dbUrl;
        this.remoteUrl = remoteUrl;
        this.interval = interval;
    }

    public Analyzer getAnalyzer() {
        return analyzer;
    }

    public boolean isIgnoreCase() {
        return ignoreCase;
    }

    public boolean isExpand() {
        return expand;
    }

    public String getDBUrl() {
        return dbUrl;
    }

    public String getRemoteUrl() {
        return remoteUrl;
    }

    public Integer getInterval() {
        return interval;
    }
}
