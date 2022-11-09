<<<<<<< HEAD
# Max Project
=======
<<<<<<< HEAD
# BPBatchDAG
Blackmirro and Pharbers Batch DAG

## Datacube jobs chain:
metacube >> cleancube >> lattices(17h28min) >> bucketlattices(50h22min) >> latticesresult(14h41min) >> finalresult >> calmeasures >> horcalmeasures >> write2postgres

## Datacube jobs chain on version(20201111):
generate_runId >> metacube >> extract_data >> cleanextractdata >> lattices >> bucketlattices >> latticesresult >> finalresult >> calmeasures >> horcalmeasures >> write2postgres

通过 airflow variables 动态构建 dag：
1. 参数 Auto_Datacube_v20201111__PROJECT_LIST: 决定dag并行数(大于等于1)，可选project有[Pfizer,AZ,Sanofi,Qilu,Astellas,Servier,Sankyo,Tide,Haikun,Mylan,Jingxin,Janssen,Kangzhe,Beite,XLT,NHWA,Gilead,Beida,Huiyu];
2. 参数 Auto_Datacube_v20201111__START_JOB: 决定dag的起点，可选job有[generate_runId,extract_data_start,cleanextractdata_start,lattices_start,bucketlattices_start,latticesresult_start,finalresult_start,calmeasures_start,horcalmeasures_start,write2postgres_start]，选取一个作为 dag 的起点任务。

从 generate_runId 开始的 trigger_conf:
{"version":"2020-08-11","time_left":"201801","time_right":"201912","start":"201801","end":"201912"}

其他 job 开始的 trigger_conf 请结合具体代码或参考 dag 文件夹下的 trigger_conf 文件。
=======
# BP_Max_AutoJob
>>>>>>> Max_old/ywyuan
>>>>>>> BPBatchDAG/PBDP-1525-Max_old
