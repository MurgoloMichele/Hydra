import os

'''
Function that merge files by time

@input:
				dir_path,			path where the *.binetflow files are stored
				time,					option to group by [hour, day, month]
				output_dir,		directory where to store the merged files
				persistence,	boolean if False keep old files after merging them
'''
def concat_files(dir_path, output_dir, time, persistence):
				file_list = os.listdir(dir_path)
				out_filename = ""
				for filename in sorted(file_list):
								if time == "hour":
												out_filename = filename[:-12] + ".binetflow"
								elif time == "day":
												out_filename = filename[:-14] + ".binetflow"
								elif time == "month":
												out_filename = filename[:-16] + ".binetflow"

								if not os.path.exists(output_dir):
												os.makedirs(output_dir)
								with open(output_dir + "/" + out_filename, "a") as outfile:
												with open(dir_path + "/" + filename, "r") as infile:
																outfile.write(infile.read())

				if persistence == "False":
								os.system("rm -f " + dir_path + "/*.binetflow")
