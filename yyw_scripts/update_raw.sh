#phcli maxauto combine --name Auto_raw_data --owner ywyuan --tag Max  --jobs Rawdata_pretreat,Rawdata_needclean,Rawdata_check

phcli maxauto dag --name Auto_raw_data
phcli maxauto publish --name Auto_raw_data
