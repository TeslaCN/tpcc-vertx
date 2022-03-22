package icu.wwj.benchmark.tpcc;

import java.time.LocalDateTime;

class OrderStatus {
    
    final int w_id;
    
    final int d_id;
    
    int c_id;
    
    String c_last;
    
    String c_first;
    
    String c_middle;
    
    double c_balance;
    
    int o_id;
    
    LocalDateTime o_entry_d;
    
    int o_carrier_id;
    
    final int[] ol_supply_w_id = new int[15];
    final int[] ol_i_id = new int[15];
    final int[] ol_quantity = new int[15];
    final double[] ol_amount = new double[15];
    final LocalDateTime[] ol_delivery_d = new LocalDateTime[15];
    
    public OrderStatus(int w_id, int d_id) {
        this.w_id = w_id;
        this.d_id = d_id;
    }
}
