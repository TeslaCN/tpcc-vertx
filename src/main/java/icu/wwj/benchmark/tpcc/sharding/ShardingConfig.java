package icu.wwj.benchmark.tpcc.sharding;/*
 * Copyright (c) @ justbk. 2021-2031. All rights reserved.
 */

import lombok.extern.slf4j.Slf4j;

import java.util.Locale;
import java.util.Properties;

/**
 * Title: the ShardingConfig class.
 * <p>
 * Description:
 *
 * @author Administrator
 * @version [issueManager 0.0.1, 2021/10/18]
 * @since 2021/10/18
 */
@Slf4j
public class ShardingConfig {
    
    public static final ShardingConfig instance = new ShardingConfig();
    
    public int terminals;
    
    public int warehouseTotal;
    
    public ShardingType shardingType;
    
    public int shardingNumber;
    
    public boolean routeItemByHint;
    
    public void init(Properties prop) {
        warehouseTotal = Integer.parseInt(prop.getProperty("warehouses"));
        terminals = Integer.parseInt(prop.getProperty("terminals"));
        shardingType = ShardingType.valueOf(prop.getProperty("shardingType", "MOD").toUpperCase(Locale.ROOT));
        log.info("shardingType=" + shardingType);
        shardingNumber = Integer.parseInt(prop.getProperty("shardingNumber", "1"));
        log.info("shardingNumber=" + shardingNumber);
        routeItemByHint = Boolean.parseBoolean(prop.getProperty("routeItemByHint", Boolean.FALSE.toString()));
        log.info("routeItemByHint=" + routeItemByHint);
    }
}
