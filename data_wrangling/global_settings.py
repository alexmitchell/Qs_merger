#!/usr/bin/env python3
import numpy as np
from os.path import join as pjoin

root_dir = "/home/alex/feed-timing/data"
lighttable_data_dir = pjoin(root_dir, "merged-lighttable-results")
Qs_raw_pickles = pjoin(lighttable_data_dir, "raw-pickles")
Qs_primary_pickles = pjoin(lighttable_data_dir, "merged-pickles")

lighttable_bedload_cutoff = 800 # g/s max rate

