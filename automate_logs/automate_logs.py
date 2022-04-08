import os
from common.s3_utils import copy_to_s3
from common.system_utils import execute_local, del_local_file
import datetime


def generate_data():
    path = "/Users/pravesh/work/code/Log-Generator/"
    os.chdir(path)

    cmd = ["./venv/bin/python", "log-gen.py", "-n", "1000", "-o", "GZ"]
    execute_local(cmd)

    # List files in current directory
    files = os.listdir(os.curdir)
    # particular files from a directories
    list = [path + k for k in files if 'access_log' in k]
    print(list)
    return list


def run(date,location):
    log_files = generate_data()
    copy_to_s3(location + date + '/', log_files)
    del_local_file(log_files)


# this is required if you want an entry point
# for your code
if __name__ == '__main__':
    location = 's3://project-2-logs/raw/access-log/'
    current_date = datetime.datetime.today().strftime('%Y-%m-%d')
    print("Current Date :", current_date)
    run(current_date,location)
