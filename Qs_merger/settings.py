#!/usr/bin/env python3

from os.path import join as pjoin
from helpyr.helpyr_misc import ensure_dir_exists

root_dir = "/home/alex/hacking/Qs_merger/tests/test_data"
#root_dir = "E:\LT_Qs_Combine\LT_Results" # Windows style path
#root_dir = "/home/alex/ubc/feed-timing/data" # Unix style path

lighttable_bedload_cutoff = 800 # g/s max rate


output_dir = pjoin(root_dir, "Qs-merger-output")
Qs_raw_pickles_dir = pjoin(output_dir, "raw-pickles")
Qs_merged_pickles_dir = pjoin(output_dir, "merged-pickles")
Qs_merged_txt_dir = pjoin(output_dir, "merged-txts")

ensure_dir_exists(output_dir)
ensure_dir_exists(Qs_raw_pickles_dir)
ensure_dir_exists(Qs_merged_pickles_dir)
ensure_dir_exists(Qs_merged_txt_dir)


metapickle_name = 'Qs_metapickle' 
