package icu.wwj.benchmark.tpcc.config;

import lombok.Getter;

import java.util.Properties;

@Getter
public class BenchmarkConfiguration {
    
    private final String conn;
    
    private final String vertxOptions;
    
    private final String poolOptions;
    
    private final int warehouses;
    
    private final boolean terminalWarehouseFixed;
    
    private final int terminals;
    
    private final int runSeconds;
    
    private final int reportIntervalSeconds;
    
    private final int newOrderWeight;
    
    private final int paymentWeight;
    
    private final int orderStatusWeight;
    
    private final int deliveryWeight;
    
    private final int stockLevelWeight;
    
    public BenchmarkConfiguration(Properties props) {
        conn = System.getProperty("conn", props.getProperty("conn"));
        vertxOptions = System.getProperty("vertxOptions", props.getProperty("vertxOptions", "{}"));
        poolOptions = System.getProperty("poolOptions", props.getProperty("poolOptions", "{}"));
        warehouses = Integer.parseInt(System.getProperty("warehouses", props.getProperty("warehouses")));
        terminalWarehouseFixed = Boolean.parseBoolean(System.getProperty("terminalWarehouseFixed", props.getProperty("terminalWarehouseFixed", Boolean.FALSE.toString())));
        terminals = Integer.parseInt(System.getProperty("terminals", props.getProperty("terminals")));
        runSeconds = Integer.parseInt(System.getProperty("runSeconds", props.getProperty("runSeconds")));
        reportIntervalSeconds = Integer.parseInt(System.getProperty("reportIntervalSeconds", props.getProperty("reportIntervalSeconds", "0")));
        newOrderWeight = Integer.parseInt(System.getProperty("newOrderWeight", props.getProperty("newOrderWeight", "45")));
        paymentWeight = Integer.parseInt(System.getProperty("paymentWeight", props.getProperty("paymentWeight", "43")));
        orderStatusWeight = Integer.parseInt(System.getProperty("orderStatusWeight", props.getProperty("orderStatusWeight", "4")));
        deliveryWeight = Integer.parseInt(System.getProperty("deliveryWeight", props.getProperty("deliveryWeight", "4")));
        stockLevelWeight = Integer.parseInt(System.getProperty("stockLevelWeight", props.getProperty("stockLevelWeight", "4")));
    }
}
