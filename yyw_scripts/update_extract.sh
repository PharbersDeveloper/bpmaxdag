#phcli maxauto combine --name Auto_extract_data_Crystal --owner ywyuan --tag extract_data --jobs extract_data
phcli maxauto dag --name Auto_extract_data
phcli maxauto publish --name Auto_extract_data

phcli maxauto dag --name Auto_extract_data_Crystal
phcli maxauto publish --name Auto_extract_data_Crystal

phcli maxauto dag --name Auto_extract_data_HuangXin
phcli maxauto publish --name Auto_extract_data_HuangXin

