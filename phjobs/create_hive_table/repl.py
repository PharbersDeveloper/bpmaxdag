import sys
import time
from ph_max_auto.ph_hook.ph_hook import exec_before


def repl():
    try:
        lines = []
        line = input(">>> ").strip()
        while True:
            lines.append(line)
            if line.endswith(';') or line == '':
                break
            line = input("... ").strip()
        return ' '.join(lines)
    except Exception:
        pass


spark_session = None
def get_spark_session():
    global spark_session
    if not spark_session:
        spark_session = exec_before(
            name='phcli_sql_repl', 
            job_id=time.time(),
            enable_hive=True)['spark']()
    return spark_session


def exec_sql(sql):
    try:
        spark = get_spark_session()
        spark.sql(sql.strip(';')).show()
    except Exception as e:
        print(e)


def main():
    print("Welcome to PhCli Hive SQL REPL v1.1.0")
    print("Can use standard SQL query pharbers hive.\n")
    
    try:
        while True:
            sql = repl()
            if sql:
                exec_sql(sql)

            input("\nPress `Enter` key to continue，Press `Ctrl+D` to exit").strip()
    except EOFError as _:
        sys.exit()


if __name__ == '__main__':
    main()
