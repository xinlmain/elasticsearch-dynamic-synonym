package com.ginobefunny.elasticsearch.plugins.synonym.service;

import java.util.List;

/**
 * 获取同义词配置文本的抽象
 */
public interface SynonymRulesReader {

    boolean isNeedReloadSynonymRules();

    List<String> reloadSynonymRules();
}
