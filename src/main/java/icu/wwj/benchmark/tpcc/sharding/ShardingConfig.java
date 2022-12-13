package icu.wwj.benchmark.tpcc.sharding;/*
 * Copyright (c) @ justbk. 2021-2031. All rights reserved.
 */

import icu.wwj.benchmark.tpcc.config.BenchmarkConfiguration;
import lombok.extern.slf4j.Slf4j;

import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
    
    private static final Pattern NUMBER_RANGES_PATTERN = Pattern.compile("\\d+(-\\d+)?(,\\d+(-\\d+)?)*");
    
    public BenchmarkConfiguration configuration;
    
    public ShardingType shardingType;
    
    public int shardingNumber;
    
    public int[] availableWarehouses;
    
    public boolean useMultiValuesInsert;
    
    public boolean itemIsBroadcastTable;
    
    public void init(BenchmarkConfiguration configuration, Properties prop) {
        this.configuration = configuration;
        shardingType = ShardingType.valueOf(System.getProperty("shardingType", prop.getProperty("shardingType", "MOD")).toUpperCase(Locale.ROOT));
        log.info("shardingType=" + shardingType);
        shardingNumber = Integer.parseInt(System.getProperty("shardingNumber", prop.getProperty("shardingNumber", "1")));
        log.info("shardingNumber=" + shardingNumber);
        String warehousesAvailableRanges = System.getProperty("warehousesAvailableRanges", prop.getProperty("warehousesAvailableRanges", "")).replace(" ", "");
        log.info("warehousesAvailableRanges=" + warehousesAvailableRanges);
        availableWarehouses = parseWarehousesAvailableRanges(warehousesAvailableRanges);
        useMultiValuesInsert = Boolean.parseBoolean(System.getProperty("useMultiValuesInsert", prop.getProperty("useMultiValuesInsert", Boolean.FALSE.toString())));
        log.info("useMultiValuesInsert=" + useMultiValuesInsert);
        itemIsBroadcastTable = Boolean.parseBoolean(System.getProperty("itemIsBroadcastTable", prop.getProperty("itemIsBroadcastTable", Boolean.FALSE.toString())));
        log.info("itemIsBroadcastTable=" + itemIsBroadcastTable);
    }
    
    private int[] parseWarehousesAvailableRanges(String warehousesAvailableRanges) {
        Set<Integer> totalWarehouses = IntStream.rangeClosed(1, configuration.getWarehouses()).boxed().collect(Collectors.toCollection(TreeSet::new));
        if (warehousesAvailableRanges.isBlank()) {
            return setToArray(totalWarehouses);
        }
        if (!NUMBER_RANGES_PATTERN.matcher(warehousesAvailableRanges).matches()) {
            throw new IllegalArgumentException("Invalid warehousesAvailableRanges pattern. Example: warehousesAvailableRanges=1,3-7,10-20,32");
        }
        TreeSet<Integer> availableWarehouses = new TreeSet<>();
        for (String eachRange : warehousesAvailableRanges.split(",")) {
            eachRange = eachRange.trim();
            if (eachRange.contains("-")) {
                String[] split = eachRange.split("-");
                int lowerInclusive = Integer.parseInt(split[0]);
                int upperInclusive = Integer.parseInt(split[1]);
                Set<Integer> range = IntStream.rangeClosed(lowerInclusive, upperInclusive).boxed().collect(Collectors.toSet());
                if (!totalWarehouses.containsAll(range)) {
                    throw new IllegalArgumentException("Invalid warehousesAvailableRanges value " + eachRange + ". Range should distribute in [1," + configuration.getWarehouses() + "]");
                }
                availableWarehouses.addAll(range);
            } else {
                int warehouse = Integer.parseInt(eachRange);
                if (!totalWarehouses.contains(warehouse)) {
                    throw new IllegalArgumentException("Invalid warehousesAvailableRanges value " + warehouse + ". Range should distribute in [1," + configuration.getWarehouses() + "]");
                }
                availableWarehouses.add(warehouse);
            }
        }
        return setToArray(availableWarehouses);
    }
    
    private static int[] setToArray(Set<Integer> totalWarehouses) {
        int[] result = new int[totalWarehouses.size()];
        int i = 0;
        for (Integer each : totalWarehouses) {
            result[i++] = each;
        }
        return result;
    }
}
