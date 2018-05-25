from multiprocessing import Queue
import math
import gzip
import os
from merge_files import *
from network_protocol import *

flow_direction = {
								"1":"<-",
								"2":"->"
								}

'''
Binetflow file header
'''
CONST_HEADER_BINETFLOW = "StartTime,Dur,Proto,SrcAddr,Sport,Dir,DstAddr,Dport,State,sTos,dTos,TotPkts,TotBytes,SrcBytes,srcUdata,dstUdata,Label"

'''
Function that returns the number of line in a file

@input:
				fname, file name
'''
def file_len(fname):
		return sum(1 for line in gzip.open(fname))


def worker(queue, output_dir, file_number):
				""" String for every binetflow field """
				string = []
				ipv4_src_addr = []
				ipv4_dst_addr = []
				first_switched = []
				last_switched = []
				protocol = []
				src_port = []
				dst_port = []
				biflow = []
				src_tos = [] 
				in_pkts = []
				out_pkts = []
				in_bytes = []
				out_bytes = []

				""" Every processor takes a filename from the queue until is empty """
				while not queue.empty():
								filename = queue.get()
								
								#current = file_number - queue.qsize()
								#print("{0:.2f}".format(current * 100 / file_number), "%", end="\r")


								string.clear()
								ipv4_src_addr.clear()
								ipv4_dst_addr.clear()
								first_switched.clear()
								last_switched.clear()
								protocol.clear()
								src_port.clear()
								dst_port.clear()
								biflow.clear()
								src_tos.clear()
								in_pkts.clear()
								out_pkts.clear()
								in_bytes.clear()
								out_bytes.clear()

								with gzip.open(filename, "rt") as infile:
										for line in infile:
												string = line.strip().split("|")
												ipv4_src_addr.append(string[0])
												ipv4_dst_addr.append(string[1])
												first_switched.append(string[7])
												last_switched.append(string[8])
												protocol.append(string[12])
												src_port.append(string[9])
												dst_port.append(string[10])
												biflow.append(string[19])
												src_tos.append(string[13])
												in_pkts.append(string[5])
												out_pkts.append(string[22])
												in_bytes.append(string[6])
												out_bytes.append(string[23])

										ipv4_src_addr.remove("IPV4_SRC_ADDR")
										protocol.remove("PROTOCOL")
										ipv4_dst_addr.remove("IPV4_DST_ADDR")
										first_switched.remove("FIRST_SWITCHED")
										last_switched.remove("LAST_SWITCHED")
										src_port.remove("L4_SRC_PORT")
										dst_port.remove("L4_DST_PORT")
										biflow.remove("BIFLOW_DIRECTION")
										src_tos.remove("SRC_TOS")
										in_pkts.remove("IN_PKTS")
										out_pkts.remove("OUT_PKTS")
										in_bytes.remove("IN_BYTES")
										out_bytes.remove("OUT_BYTES")
										 
								i = 1

								year,month,day,hour,minute = filename.split("/")
								minute,name,extension = minute.split(".")
								
								length_file = file_len(filename)
								if not os.path.exists(output_dir):
												os.makedirs(output_dir)
								with open(output_dir + "/" + year + month + day + hour + minute + ".binetflow", "w") as outfile:
												outfile.write(CONST_HEADER_BINETFLOW)

								with open(output_dir + "/" + year + month + day + hour + minute + ".binetflow", "a") as outfile:
										while i + 1 < length_file:
												last = float(last_switched[i])
												first = float(first_switched[i])
												tot_pkts = int(in_pkts[i]) + int(out_pkts[i])
												tot_bytes = int(in_bytes[i]) + int(out_bytes[i])

												outfile.write("\n" + year + "/" + month
																				+ "/" + day + " " + hour + ":" + minute + ":" + first_switched[i][:2] + "." + 
															  first_switched[i][:6] + "," + '{:.6f}'.format(last - first) + 
															  "," + dict_protocols.get(protocol[i]) + "," + ipv4_src_addr[i] + 
															  "," + src_port[i] + "," + flow_direction.get(biflow[i]) + "," + ipv4_dst_addr[i] + "," + 
																dst_port[i] + "," + "CON" "," + src_tos[i] + "," + "," + str(tot_pkts)
															  + "," + str(tot_bytes) + "," + in_bytes[i] + ",,,")
												i = i + 1
