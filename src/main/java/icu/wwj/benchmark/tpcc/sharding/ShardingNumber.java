package icu.wwj.benchmark.tpcc.sharding;/*
 * Copyright (c) @ justbk. 2021-2031. All rights reserved.
 */

/**
 * Title: the ShardingNumber class.
 * <p>
 * Description:
 *
 * @author Administrator
 * @version [issueManager 0.0.1, 2021/10/18]
 * @since 2021/10/18
 */
public class ShardingNumber {
    
    /**
     * To convert rndWarehouse match to warehouse.
     *
     * @param warehouse the base warehouse
     * @param rndWarehouse the random warehouse, may be not match with warehouse
     * @return the matched random warehouse
     */
    public static int oneSharding(int warehouse, int rndWarehouse) {
        switch (ShardingConfig.instance.shardingType) {
            case MOD -> {
                int shardingNumber = ShardingConfig.instance.shardingNumber;
                int sub = Math.abs(rndWarehouse - warehouse) % shardingNumber;
                if (sub == 0) {
                    return rndWarehouse;
                }
                sub = rndWarehouse > warehouse ? sub : -sub;
                if (rndWarehouse - sub > 0) {
                    return rndWarehouse - sub;
                } else {
                    return rndWarehouse + shardingNumber - sub;
                }
            }
            case RANGE -> {
                int range = ShardingConfig.instance.configuration.getWarehouses() / ShardingConfig.instance.shardingNumber;
                return rndWarehouse - range * ((rndWarehouse - 1) / range - (warehouse - 1) / range);
            }
            case NONE -> {
                return rndWarehouse;
            }
        }
        throw new UnsupportedOperationException("Unsupported shardingType: " + ShardingConfig.instance.shardingType);
    }
}
