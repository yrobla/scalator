#This script checks the results folder to see how many docs have been processed in the last minute, and puts the results to the logging queue

import os
import os.path
import time
import datetime
import logging_queue

result_folders = [('fowlt','/root/fowlt/output'),
                  ('valkuil','/root/valkuil/output')];
timestamp = time.time();

for corrector,folder in result_folders:

    recent_output = 0;

    for filename in os.listdir(folder):

        mtime = os.path.getmtime(folder+'/'+filename);

        if '.out' not in filename and '.test' not in filename and '.tok' not in filename \
          and timestamp - mtime < 599:
            recent_output += 1;

    s = str(datetime.datetime.now()) + ' ' + corrector+': '+str(recent_output);

    logging_queue.send(s);

