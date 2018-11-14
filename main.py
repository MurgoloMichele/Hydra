import sys
import os
from multiprocessing import Queue, Pool
from tqdm import tqdm
from convert import *
from merge_files import *

walk_dir = sys.argv[1]
output_dir = sys.argv[2]
print("walk_dir = " + walk_dir)
	

queue = Queue()
sorted_files = []
# put all filenames into queue with os.walk() and queue.put(filename)
for dirpath, dirs, files in os.walk(walk_dir):
				for filename in files:
								sorted_files.append(os.path.join(dirpath,filename))

sorted_files.sort()
for fname in sorted_files:
				queue.put(fname)

file_number = len(sorted_files)
pool = Pool(2, worker, (queue, output_dir, file_number))
pool.close()
pool.join()


#concat_files("docs/", "hour", "merge", "False")
