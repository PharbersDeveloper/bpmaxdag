# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
import base64
from datetime import datetime
from phcli.ph_db.ph_pg import PhPg
from phcli.ph_logs.ph_logs import phs3logger
from phcli.ph_max_auto.ph_models.asset import Asset


def execute(**kwargs):
    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
    owner = kwargs.pop('owner', None)
    run_id = kwargs.pop('run_id', None)
    job_id = kwargs.pop('job_id', None)

    logger = phs3logger(job_id)

    phjobs = {
        "extract_data_extract": {
            "input": {
                "max_path": "Empty",
                "extract_path": "Empty",
                "out_path": "Empty",
                "extract_file": "Empty",
                "time_left": "Empty",
                "time_right": "Empty",
                "molecule": "Empty",
                "molecule_sep": "Empty",
                "atc": "Empty",
                "project": "Empty",
                "doi": "Empty",
                "out_suffix": "Empty",
                "data_type": "Empty",
            },
            "output": {
                "c": "Empty",
                "d": "Empty",
            },
        },
        "extract_data_copy": {
            "input": {
                "from": "Empty",
                        "to": "Empty",
            },
            "output": {
                "extract_data_out": "Empty",
            },
        },
        "extract_data_email": {
            "input": {
                "email": "Empty",
                "subject": "Empty",
                "content_type": "Empty",
                "content": "Empty",
            },
            "output": {
                "status": "Empty",
            },
        },
    }

    # merge phjobs 和 kwargs 中的参数信息
    for job_name, job in phjobs.items():
        for input_name, input_path in job['input'].items():
            if input_name in kwargs.keys() and kwargs[input_name]:
                job['input'][input_name] = kwargs[input_name]
        for output_name, output_path in job['output'].items():
            if output_name in kwargs.keys() and kwargs[output_name]:
                job['output'][output_name] = kwargs[output_name]

    # 转换成 DAG 邻接表
    dag_node_info_map = {}
    dag_adj_lst = {}
    for job_name, job in phjobs.items():
        for output_name, output_path in job['output'].items():
            dag_node_info_map[output_path] = (output_name, job_name)
            dag_adj_lst[output_path] = dag_adj_lst.get(output_path, [])
            for input_name, input_path in job['input'].items():
                dag_node_info_map[input_path] = (input_name, job_name)
                dag_adj_lst[output_path].append(input_path)

    # 获取最近的 asset 点
    def get_latest_lst_cr(adj_lst, depend_lst):
        """
        从后向前，递归（CR）获取最近的 asset tuple list
        :param adj_lst: 当前节点依赖的节点列表
        :param depend_lst: 依赖当前节点的节点名称列表
        :return: latest_lst 当前节点最近的依赖列表
        """
        result = []
        for adj_name in adj_lst:
            # 如果循环依赖，直接略过
            if adj_name in depend_lst:
                continue

            # 如果自身是 asset，则保留迭代器
            if 's3a://' in adj_name and 'asset' in adj_name:
                result.append(adj_name)

            depend_lst.add(adj_name)
            subs = get_latest_lst_cr(dag_adj_lst.get(adj_name, []), depend_lst)
            result += subs
        return result

    # 取所有 asset output 的 parent
    asset_parents_lst = {}
    for cur_name, adj_lst in dag_adj_lst.items():
        if 's3a://' not in cur_name or 'asset' not in cur_name:
            continue
        parents = list(get_latest_lst_cr(adj_lst, {cur_name}))
        asset_parents_lst[cur_name] = parents

    if not asset_parents_lst:
        logger.warning("没有需要写入的 asset")
        return {}
    for item in list(kwargs.keys()):
        if (kwargs[item] == None
                or kwargs[item] == 'Empty'
                or item == 'spark'
                or item == 'name'
                or item == 'extract_data_out'
                or item == 'out_suffix'
                or item == 'email'):
            del kwargs[item]
    name = datetime.now().strftime("%Y%m%d%H%M_") + '_'.join(kwargs.values())
    pg = PhPg(
        base64.b64decode(
            'cGgtZGItbGFtYmRhLmNuZ2sxamV1cm1udi5yZHMuY24tbm9ydGh3ZXN0LTEuYW1hem9uYXdzLmNvbS5jbgo=').decode('utf8')[:-1],
        base64.b64decode('NTQzMgo=').decode('utf8')[:-1],
        base64.b64decode('cGhhcmJlcnMK').decode('utf8')[:-1],
        base64.b64decode('QWJjZGUxOTYxMjUK').decode('utf8')[:-1],
        db=base64.b64decode('cGhlbnRyeQo=').decode('utf8')[:-1],
    )

    for asset, parents in asset_parents_lst.items():
        logger.info(asset)
        parents_id = []
        for parent in parents:
            logger.info(parent)
            obj = pg.query(Asset(), source=parent)
            if obj:
                obj_id = obj[0].id
            else:
                obj = pg.insert(
                    # Asset(name=dag_node_info_map[parent][0], owner=owner, source=parent))
                    Asset(name=name, owner=owner, source=parent, labels="{提数数据}"))
                obj_id = obj.id
            parents_id.append(obj_id)

        obj = pg.query(Asset(), source=asset)
        if obj:
            obj = obj[0]
            obj.owner = owner
            obj.modified = datetime.now()
            pg.update(obj)
        else:
            pg.insert(
                # Asset(name=dag_node_info_map[asset][0], owner=owner, source=asset))
                Asset(name=name, owner=owner, source=asset, labels="{提数数据}"))

    pg.commit()
    return {}
