import glob
import importlib.util
import os


import pytest
from airflow import DAG
from airflow.models import DagBag

# DAG_PATH = os.path.join(
#     os.path.dirname(__file__), "..", "..", "dags", "*.py"
# )

# DAG_FILES = glob.glob(DAG_PATH, recursive=True)


# 실행방법
# docker compose exec -it airflow-webserver pytest tests/
# or
# docker exec -it airflow-airflow-webserver-1 pytest tests/

# 컨테이너 안에 들어가서 PIP로 pytest를 설치해야 함.


# 책에 나온 대로 실습하면 작동 됨
# airflow가 구동된 환경해서 테스트 해야 되고, DagBag을 호출하면, airflow가 구동된 환경에 존재하는 모든 dag를 읽어온다.


dag_bag = DagBag(include_examples=False)
#print(dag_bag.dags.keys())

# DagBag()을 호출하면 기본적으로 모든 dag를 읽어오는데, 이때 기본적인 검사를 자동으로 수행하고, 에러가 나면 dag_bag.import_errors에 저장된다.
assert dag_bag.import_errors == {}, f"Import errors: {dag_bag.import_errors}"

@pytest.mark.parametrize("dag_id", dag_bag.dags.keys())
def test_dag_integrity(dag_id):
    pass
    # 개별 dag에 대한 별도 테스트는 여기서 수행하면 될 것 같다.

    # try:
    #     dag = dag_bag.get_dag(dag_id)
    #     # Airflow 2.10+에서는 dag.test_cycle()이 제거되었기 때문에
    #     # DagBag 로딩 시점에서 cycle이 있으면 예외 발생
    #     assert dag is not None
    # except AirflowDagCycleException as e:
    #     pytest.fail(f"{dag_id} DAG has a cycle: {e}")
    # print(dag)


    #assert isinstance(dag, DAG), f"dag is not a DAG: {dag}"
    #assert dag.import_errors == {}, f"DAG import errors: {dag_bag.import_errors}"

    # print(dag)
    # assert dag_id.startswith("7"), f"DAG ID should start with '7.': {dag_id}"

    # print("=======[1]")
    # print(dag_file)
    # print(os.path.splitext(dag_file))
    # print(len(os.path.splitext(dag_file)))
    # module_name, _ = os.path.splitext(dag_file)

    # print(f"module name: { module_name }")
    # print("=======[2]")
    # module_path = os.path.join(DAG_PATH, dag_file)
    # print(f"module path: { module_path }")

    # print("=======[4]")
    # mod_spec = importlib.util.spec_from_file_location(module_name, module_path)
    # module = importlib.util.module_from_spec(mod_spec)
    # mod_spec.loader.exec_module(module)

    #print(vars(module))
    # for x in vars(module).values():
    #     print(x)
    #     # if isinstance(x, DAG):
    #     #     print(x)

    # dag_bag = DagBag()
    # assert dag_bag.dags is not None
    # assert dag_bag.import_errors == {}, f"DAG import errors: {dag_bag.import_errors}"



    # dag_objects = [var for var in vars(module).values() if isinstance(var, DAG)]

    # assert dag_objects

    # for dag in dag_objects:
    #     dag.test_cycle()



# for x in dag_bag.dags.keys():
#     test_dag_integrity(x)

# print("=======[3]")
# print(DAG_PATH)
# print(DAG_FILES)
