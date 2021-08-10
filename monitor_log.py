from datetime import datetime
import re
import pprint
import json
from elasticsearch import Elasticsearch
from elasticsearch import helpers

group_id = None
curent_time = None
account = None
post_id = None
error = None

def _extract_usr_pwd(text):
    usr_pattern = re.compile(r"(?<=User: ).+(?=,)")
    pwd_pattern = re.compile(r"(?<=Password: ).+")
    try:
        usr = re.findall(usr_pattern, text)[0]
        pwd = re.findall(pwd_pattern, text)[0]
        return {usr:pwd}
    except:
        return None
def _extract_group_id(text):
    group_id_pattern = re.compile(r"(?<=group )\d+")
    try:
        return re.findall(group_id_pattern, text)[0]
    except:
        return None
def _extract_time(text):
    time_pattern = re.compile(r"\[.+\]")
    try:
        contains_time = re.match(time_pattern, text)
        if contains_time:
        #     return datetime.strptime(text[1:20], r'%Y-%m-%d %H:%M:%S')
            return text[1:20]
    except:
        return None
def _extract_post_id(text):
    return text[36:]
def _reset_values():
    group_id = None
    curent_time = None
    account = None
    post_id = None
    error = None
def _get_nday_log(number_of_day, post_list):
    if number_of_day == -1:
        return post_list
        
    i = -1
    while post_list[i]['time'] == None:
        i -= 1
    nearest_log_time = datetime.strptime(post_list[i]['time'], r'%Y-%m-%d %H:%M:%S')

    one_day_log = []
    for index, post in enumerate(post_list[::-1]):
        one_day_log.append(post)
        if post['time'] == None:
            continue
        log_time = datetime.strptime(post['time'], r'%Y-%m-%d %H:%M:%S')
        day_diff = (nearest_log_time - log_time).total_seconds()/60/60/24
        if day_diff > number_of_day:
            break
    return one_day_log, nearest_log_time, log_time
def get_posts_info(logs_data):
    post_list = []
    for line in logs_data:
        if line == "=======":
            _reset_values()
        current_time = _extract_time(line)
        if "[INFO]:User" in line:
            account = _extract_usr_pwd(line)
        elif "Crawling group" in line:
            group_id = _extract_group_id(line)
        elif "[INFO]:ID:" in line:
            post_id = _extract_post_id(line)
            post_list.append({
                "post_id": post_id,
                "account": account,
                "group_id": group_id,
                "error": "None",
                "time":current_time
            })
        elif "[ERROR]:Error get id" in line:
            post_id = "None"
            error = "Error get id"
            post_list.append({
                "post_id": post_id,
                "account": account,
                "group_id": group_id,
                "error": error,
                "time":current_time
            })
        elif "Message: Unable to locate element: ._5rgr.async_like" in line:
            post_id = "None"
            error = "Unable to read posts"
            post_list.append({
                "post_id": post_id,
                "account": account,
                "group_id": group_id,
                "error": error,
                "time":current_time
            })
    return post_list

def dump_to_elastic(log_file):
    print("starting")
    logs_data = []
    with open("check_point.txt", 'r') as f:
        last_read_line = int(f.readline())
    with open(log_file, 'r', encoding='utf-8') as f:
        for index, line in enumerate(f):
            if index > last_read_line:
                index = last_read_line
                logs_data.append(line.strip())
    post_list = get_posts_info(logs_data)
    
    print("log_data")
    print(logs_data[:10])
    es = Elasticsearch([{'host':'localhost', 'port': 9200}])
    if not es.ping():
        print("Failed to initiate connection to Elasticsearch")
        return
    print("check if index exists")
    if not es.indices.exists(index = "crawl_monitor"):
        print("Creatihg index crawl_monitor")
        es.indices.create(index = "crawl_monitor")
        print("Created index crawl_monitor")
    
    action = [
        {
        "_index": "crawl_monitor",
        "_source": post
        }
        for post in post_list
    ]
    print("action")
    print(account[:10])
    print("adding to es")
    helpers.bulk(es, action)

def get_monitoring_stat(post_list, number_of_day):
    one_day_log, end_time, start_time = _get_nday_log(number_of_day, post_list)
    post_per_day = 0
    error_post_per_day = 0
    group_info = {}
    group_error_info = {}
    accounts_info = {}
    account_error_info = {}

    for post in one_day_log:
        #calculate number of posts per day
        if post['post_id'] != "None":
            post_per_day+=1
        else:
            error_post_per_day += 1
        #calculate number of posts in each group
        if post['group_id'] not in group_info:
            if post['post_id'] != "None":
                group_info[post['group_id']] = 1
                group_error_info[post['group_id']] = 0
            else:
                group_info[post['group_id']] = 0
                group_error_info[post['group_id']] = 1
        else: 
            if post['post_id'] != "None":
                group_info[post['group_id']] += 1
            else:
                group_error_info[post['group_id']] += 1
        #calculate number of posts in each account
        username = list(post['account'].keys())[0]
        if username not in accounts_info:
            if post['post_id'] != "None":
                accounts_info[username] = 1
                account_error_info[username] = 0
            else:
                accounts_info[username] = 0
                account_error_info[username] = 1
        else: 
            if post['post_id'] != "None":
                accounts_info[username] += 1
            else:
                account_error_info[username] += 1

    print(f"Time from {start_time} to {end_time}")
    print(f"Number of post: {post_per_day}")
    print(f"Number of error post: {error_post_per_day}")
    print(f"Number of post in each group:")
    pprint.pprint(group_info)
    print(f"Number of error post in each group:")
    pprint.pprint(group_error_info)
    print(f"Number of post in each account:")
    pprint.pprint(accounts_info)
    print(f"Number of error post in each account:")
    pprint.pprint(account_error_info)

if __name__ == "__main__":
    print("Do something please")
    dump_to_elastic("crawl_public_group.log")