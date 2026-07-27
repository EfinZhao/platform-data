import json
import numpy as np

with open("analysis/asl_time_sample.jsonl") as lines:
    d = [json.loads(line) for line in lines.readlines()]
    temp = np.array([])
    for line in d:
        try:
            # print(line['asl message']['ASLTIME'])
            temp = np.append(temp, line['asl message']['ASLTIME'])
        except:
            pass
    # print(f"Average for {len(d)} requests: {sum(i['asl message']['ASLTIME'] for i in d if i['asl message']['ASLTIME']) / len(d)}")
# print(np.mean(temp))
# print(temp.size)
print(f"average of {np.mean(temp):.3f}s from ASL request to receipt over {temp.size} samples")
