package com.ginobefunny.elasticsearch.plugins.synonym.service;

/**
 * 获取同义词配置文本的抽象
 */
public interface SynonymRulesReader {

    boolean isNeedReloadSynonymRules();

    String reloadSynonymRules();
}
