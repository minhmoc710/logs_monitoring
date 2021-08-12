from datetime import datetime
import re
import pprint
import json
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import time

group_id = None
curent_time = None
account = None
post_id = None
error_log = None
error = None

def _follow(logfile, checkpoint_file):
    """
    Reading logfile constantly and yield any new line written in the log file.
    Parameters:
        - logfile (string): path to file containning the log infomation
        - checkpoint_file (string): path to file json containning last run time infomation
    """
    with open(checkpoint_file, 'r') as f:
        checkpoint_data = json.load(f)
        latest_line_pos = checkpoint_data['last_read_line']
    f = open(logfile)
    f.seek(0,0)
    current_line = 0
    try:
        while True:
            line = f.readline()
            if current_line < latest_line_pos:
                current_line += 1
                continue
            if not line:
                time.sleep(0.1)
                continue
            if line.strip() != "":
                current_line += 1
            yield line
    except KeyboardInterrupt:
        f.close()
        with open(checkpoint_file, 'w') as f:
            checkpoint_data['last_read_line'] = curent_time + 1
            json.dump(checkpoint_data, f)

def _extract_account(text):
    """Return account username from a text string"""
    usr_pattern = re.compile(r"(?<=User: ).+(?=,)")
    try:
        usr = re.findall(usr_pattern, text)[0]
        return usr
    except:
        return None

def _extract_group_id(text):
    """Return group id from a text string"""
    group_id_pattern = re.compile(r"(?<=group )\d+")
    try:
        return re.findall(group_id_pattern, text)[0]
    except:
        return None

def _extract_time(text):
    """Return time from a text string"""
    time_pattern = re.compile(r"\[.+\]")
    try:
        contains_time = re.match(time_pattern, text)
        if contains_time:
            return datetime.strptime(text[1:20], r'%Y-%m-%d %H:%M:%S')
            # return text[1:20]
    except:
        return None

def _extract_post_id(text):
    """Return post id from a text string"""
    return text[36:].strip()

def _extract_found_posts(text):
    """Return number of found posts from a text string"""
    found_posts_pattern = re.compile(r"(?<=\[INFO\]:Got )\d+")
    try:
        return int(re.findall(found_posts_pattern, text)[0])
    except:
        return None

def _reset_values():
    group_id = None
    curent_time = None
    account = None
    post_id = None
    error_log = None

def _get_nday_log(number_of_day, post_list):
    if number_of_day == -1:
        return post_list, 'a','a'
        
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

def _get_posts_info(logs_data, checkpoint_file):
    """
    Return a list containning extracted data from log data
    Parameters:
        - logs_data (list): A list containing log lines
    """
    with open(checkpoint_file, "r") as f:
        checkpoint  = json.load(f)

    group_id = checkpoint['last_post_data']["group_id"]
    account = checkpoint['last_post_data']['account']
    total_posts = checkpoint['total_crawled']
    total_error = checkpoint['total_error']
    group_total = checkpoint['group_total']
    group_error_total = checkpoint['group_error_total']
    group_found_posts = checkpoint['group_found_posts']

    post_list = []

    for line in logs_data:
        contain_post_info = False
        if line == "=======":
            _reset_values()
        current_time = _extract_time(line)
        if "[INFO]:User" in line:
            account = _extract_account(line)
        elif "Crawling group" in line:
            group_id = _extract_group_id(line)
        elif "[INFO]:Got " in line:
            found_posts = _extract_found_posts(line)
            if group_id in group_found_posts:   
                group_found_posts[group_id] += found_posts
            else:
                group_found_posts[group_id] = found_posts
        else:
            if "[INFO]:ID:" in line:
                total_posts += 1
                post_id = _extract_post_id(line)
                error_log =  None,
                error = False
                if group_id in group_total:
                    group_total[group_id] += 1
                else:
                    group_total[group_id] = 1
                    group_error_total[group_id] = 0
                contain_post_info = True
            elif "[ERROR]" in line:
                total_error += 1
                post_id = None
                error_log =  line,
                error =  True

                if group_id in group_error_total:
                    group_error_total[group_id] += 1
                else:
                    group_total[group_id] = 0
                    group_error_total[group_id] = 1
                contain_post_info = True
            if contain_post_info:
                post_list.append({
                    "post_id": post_id,
                    "account": account,
                    "group_id": group_id,
                    "error_log": error_log,
                    "time":current_time,
                    "error": error,
                    "group_total_crawled_posts": group_total[group_id],
                    "group_total_errored_posts": group_error_total[group_id],
                    "total_crawled_posts": total_posts,
                    "total_errored_posts": total_error,
                    "total_group_found_posts": group_found_posts[group_id]
                })
    with open('checkpoint.json', 'w') as f:
        json.dump({
                    "last_read_line": checkpoint["last_read_line"],
                    "total_crawled": total_posts,
                    "total_error": total_error,
                    "group_total": group_total,
                    "group_error_total": group_error_total,
                    "group_found_posts": group_found_posts,
                    "last_post_data": {
                        "group_id": group_id,
                        "account":account
                    }
                },f)
    return post_list

def _get_log_from_file(log_file, checkpoint_file):
    """Return a list containning logs lines from log file"""
    logs_data = []
    with open(checkpoint_file, 'r') as f:
        try:
            checkpoint_data = json.load(f)
            last_read_line = checkpoint_data["last_read_line"]
        except:
            print("Error reading last read line")

    with open(log_file, 'r', encoding='utf-8') as f:
        for index, line in enumerate(f):
            if index > last_read_line:
                logs_data.append(line.strip())
    with open(checkpoint_file, 'w') as f:
        checkpoint_data["last_read_line"] = index
        json.dump(checkpoint_data, f)
    return logs_data

def dump_to_elastic(log_file, checkpoint_file):
    """Dump extracted info to elasticsearch"""
    logs_data = _get_log_from_file(log_file, checkpoint_file)
    post_list = _get_posts_info(logs_data, checkpoint_file)
    
    print(logs_data[:10])
    es = Elasticsearch([{'host':'localhost', 'port': 9200}])
    if not es.ping():
        print("Failed to initiate connection to Elasticsearch")
        return
    if not es.indices.exists(index = "crawl_monitor"):
        es.indices.create(index = "crawl_monitor", ignore=400)
        print("Created index crawl_monitor")
    
    action = [
        {
        "_index": "crawl_monitor",
        "_source": post
        }
        for post in post_list
    ]
    print("Inserting to es")
    helpers.bulk(es, action)

def dump_from_stream(log_file, checkpoint_file):
    """Dump extracted data from stream to elasticsearch"""
    logs_data = _follow(log_file, checkpoint_file)
    print("Getting data from stream . . .")
    for data in logs_data:
        post_list = _get_posts_info([data], checkpoint_file)
        
        es = Elasticsearch([{'host':'localhost', 'port': 9200}])
        if not es.ping():
            print("Failed to initiate connection to Elasticsearch")
            return
        if not es.indices.exists(index = "crawl_monitor"):
            es.indices.create(index = "crawl_monitor")
            print("Created index crawl_monitor")
        
        action = [
            {
            "_index": "crawl_monitor",
            "_source": post
            }
            for post in post_list
        ]
        print("Inserting to es")
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
    dump_to_elastic("crawl_public_group.log", "check_point.json")
    dump_from_stream("crawl_public_group.log", "check_point.json")