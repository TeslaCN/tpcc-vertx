package icu.wwj.benchmark.tpcc.config;

import lombok.Getter;

import java.util.Properties;

@Getter
public class BenchmarkConfiguration {
    
    private final String conn;
    
    private final String vertxOptions;
    
    private final String poolOptions;
    
    private final int warehouses;
    
    private final boolean warehouseFixed;
    
    private final int terminals;
    
    private final int runSeconds;
    
    public BenchmarkConfiguration(Properties props) {
        conn = System.getProperty("conn", props.getProperty("conn"));
        vertxOptions = System.getProperty("vertxOptions", props.getProperty("vertxOptions", "{}"));
        poolOptions = System.getProperty("poolOptions", props.getProperty("poolOptions", "{}"));
        warehouses = Integer.parseInt(System.getProperty("warehouses", props.getProperty("warehouses")));
        warehouseFixed = Boolean.parseBoolean(System.getProperty("warehouseFixed", props.getProperty("warehouseFixed", Boolean.FALSE.toString())));
        terminals = Integer.parseInt(System.getProperty("terminals", props.getProperty("terminals")));
        runSeconds = Integer.parseInt(System.getProperty("runSeconds", props.getProperty("runSeconds")));
    }
}
