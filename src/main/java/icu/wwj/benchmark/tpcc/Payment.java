package icu.wwj.benchmark.tpcc;

import java.time.LocalDateTime;

class Payment {
    /* terminal input data */
    final int w_id;
    final int d_id;
    int c_id;
    int c_d_id;
    int c_w_id;
    String c_last;
    double h_amount;

    /* terminal output data */
    String w_name;
    String w_street_1;
    String w_street_2;
    String w_city;
    String w_state;
    String w_zip;
    String d_name;
    String d_street_1;
    String d_street_2;
    String d_city;
    String d_state;
    String d_zip;
    String c_first;
    String c_middle;
    String c_street_1;
    String c_street_2;
    String c_city;
    String c_state;
    String c_zip;
    String c_phone;
    LocalDateTime c_since;
    String c_credit;
    double c_credit_lim;
    double c_discount;
    double c_balance;
    String c_data;
    LocalDateTime h_date;

    Payment(int w_id, int d_id) {
        this.w_id = w_id;
        this.d_id = d_id;
    }
}
