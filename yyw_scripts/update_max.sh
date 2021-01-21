# phcli maxauto combine --name Auto_Max_yuanyw --owner ywyuan --tag Max --jobs job1_hospital_mapping,job2_product_mapping,job3_1_data_adding,job3_2_data_adding,job4_panel,job5_max_weight,job6_max_city,job7_max_standard,job7_raw_standard

phcli maxauto dag --name Auto_Max_yuanyw
phcli maxauto publish --name Auto_Max_yuanyw

phcli maxauto dag --name Auto_Max_guor
phcli maxauto publish --name Auto_Max_guor

phcli maxauto dag --name Auto_Max_liyx
phcli maxauto publish --name Auto_Max_liyx

phcli maxauto dag --name Auto_Max_lujx
phcli maxauto publish --name Auto_Max_lujx
