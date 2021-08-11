import time
def follow(thefile, position):
    thefile.seek(0,0)
    current_line = 0
    try:
        while True:
            line = thefile.readline()
            if current_line < position:
                current_line += 1
                continue
            if not line:
                time.sleep(0.1)
                continue
            if line.strip() != "":
                current_line += 1
            yield line
    except KeyboardInterrupt:
        with open("check_point.txt", 'w') as f:
            f.write(str(current_line + 1))
def abcxyz(loglines):
    return [line.upper() for line in loglines]
if __name__ == '__main__':
    logfile = open("test.txt","r")
    loglines = follow(logfile, 0)
    # for line in loglines:
    #     print(line, end= "")
    x = abcxyz(loglines)
    for line in x:
        print(line, end= "")