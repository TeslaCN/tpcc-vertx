package icu.wwj.benchmark.tpcc;

import java.time.LocalDateTime;

public class NewOrder {

    final int w_id;
    final int d_id;
    final int c_id;

    final int ol_supply_w_id[] = new int[15];
    int o_all_local = 1;
    final int ol_i_id[] = new int[15];
    final int ol_quantity[] = new int[15];

    /* terminal output data */
    String c_last;
    String c_credit;
    double c_discount;
    double w_tax;
    double d_tax;
    int o_ol_cnt;
    int o_id;
    LocalDateTime o_entry_d;
    double total_amount;
    String execution_status;

    final String i_name[] = new String[15];
    final int s_quantity[] = new int[15];
    final String brand_generic[] = new String[15];
    final double i_price[] = new double[15];
    final double ol_amount[] = new double[15];

    public NewOrder(int w_id, int d_id, int c_id) {
        this.w_id = w_id;
        this.d_id = d_id;
        this.c_id = c_id;
    }
}
