package icu.wwj.benchmark.tpcc;

import icu.wwj.benchmark.tpcc.config.BenchmarkConfiguration;

public class TransactionMapGenerator {
    
    public static String[] generate(BenchmarkConfiguration configuration) {
        int sum = 0;
        sum += configuration.getNewOrderWeight();
        sum += configuration.getPaymentWeight();
        sum += configuration.getOrderStatusWeight();
        sum += configuration.getDeliveryWeight();
        sum += configuration.getStockLevelWeight();
        String[] result = new String[sum];
        int index = 0;
        for (int i = 0; i < configuration.getNewOrderWeight(); i++) {
            result[index++] = TPCCTransaction.NEW_ORDER.name();
        }
        for (int i = 0; i < configuration.getPaymentWeight(); i++) {
            result[index++] = TPCCTransaction.PAYMENT.name();
        }
        for (int i = 0; i < configuration.getOrderStatusWeight(); i++) {
            result[index++] = TPCCTransaction.ORDER_STATUS.name();
        }
        for (int i = 0; i < configuration.getDeliveryWeight(); i++) {
            result[index++] = TPCCTransaction.DELIVERY.name();
        }
        for (int i = 0; i < configuration.getStockLevelWeight(); i++) {
            result[index++] = TPCCTransaction.STOCK_LEVEL.name();
        }
        return result;
    }
}
