import os
from os.path import join as pjoin
import numpy as np

# From Helpyr
from helpyr import data_loading
from helpyr.helpyr_misc import nsplit
from helpyr.helpyr_misc import ensure_dir_exists
from helpyr import logger as hlp_logger
from helpyr import crawler as hlp_crawler

from xlrd.biffh import XLRDError


import settings


# ISSUE TO ADDRESS:
# Some Qs.txt and Qs1.txt files appear to be nearly identical copies. The first 
# row is usually different and sometimes a random row where a grain count is 1 
# off. Preference is given to Qs1.txt?

class QsExtractor (hlp_crawler.Crawler):
    # The Extraction Crawler does the initial work of finding all the data 
    # files and converting them to pickles

    def __init__(self, root_dir, output_dir):
        log_filepath = pjoin(root_dir, 'log-files', 'QsExtractor.txt')
        logger = hlp_logger.Logger(log_filepath, default_verbose=True)
        hlp_crawler.Crawler.__init__(self, logger)

        self.set_root(root_dir)
        self.output_dir = output_dir
        ensure_dir_exists(output_dir, logger)

        self.loader = data_loading.DataLoader(root_dir, output_dir, logger)


    def run(self):
        # Overloads Crawler.run function. The flexibility from run modes is not 
        # necessary for this project.

        logger = self.logger

        # Extract the Qs data from text files and save them as pickles
        logger.write_section_break()
        logger.write(["Extracting light table data"])

        logger.write("Finding files")
        raw_Qs_files = self.get_target_files(['Qs?.txt', 'Qs??.txt'],
                verbose_file_list=True)

        if len(raw_Qs_files) == 0:
            logger.write("No files found!")
            self.end()
            return None
        else:
            metapickle_path = self.extract_light_table(raw_Qs_files)
            self.end()
            return metapickle_path


    def extract_light_table(self, raw_Qs_txt_files):
        lg = self.logger
        lg.write("Extracting light table data")
        lg.increase_global_indent()

        # Prepare kwargs for reading Qs text files
        Qs_column_names = [
                # Timing and meta data
                #'elapsed-time sec', <- Calculate this column later
                'timestamp', 'missing ratio', 'vel', 'sd vel', 'number vel',
                # Bedload transport masses (g)
                'Bedload all', 'Bedload 0.5', 'Bedload 0.71', 'Bedload 1',
                'Bedload 1.4', 'Bedload 2', 'Bedload 2.8', 'Bedload 4',
                'Bedload 5.6', 'Bedload 8', 'Bedload 11.2', 'Bedload 16',
                'Bedload 22', 'Bedload 32', 'Bedload 45',
                # Grain counts
                'Count all', 'Count 0.5', 'Count 0.71', 'Count 1', 'Count 1.4',
                'Count 2', 'Count 2.8', 'Count 4', 'Count 5.6', 'Count 8',
                'Count 11.2', 'Count 16', 'Count 22', 'Count 32', 'Count 45',
                # Statistics
                'D10', 'D16', 'D25', 'D50', 'D75', 'D84', 'D90', 'D95', 'Dmax'
                ]
        Qs_kwargs = {
                'index_col' : None,
                'header'    : None,
                'names'     : Qs_column_names,
                }

        period_dict = self.build_period_dict(raw_Qs_txt_files)
        pickle_dict = {}

        # Create new pickles if necessary
        for period_path in period_dict:
            lg.write(f"Extracting {period_path}")
            lg.increase_global_indent()

            fnames = period_dict[period_path]
            pickle_dict[period_path] = self.pickle_Qs_text_files(
                    period_path, fnames, Qs_kwargs)
            lg.decrease_global_indent()

        # Update the metapickle
        # it describes which pkl files belong to which periods
        metapickle_name = settings.metapickle_name
        if self.loader.is_pickled(metapickle_name):
            # Pickled metapickle already exists.
            # Update the metapickle
            lg.write(f"Updating {metapickle_name}...")
            existing = self.loader.load_pickle(
                    metapickle_name, use_source=False)
            pickle_dict = self._merge_metapickle(
                    period_dict, pickle_dict, existing)

        metapickle_path = self.make_pickle(metapickle_name, pickle_dict, 
                overwrite=True)[0]

        lg.decrease_global_indent()
        lg.write("Light table data extraction complete")

        return metapickle_path
    
    def _merge_metapickle(self, period_dict, new_dict, old_dict):
        merge = lambda a, b: list(set(a + b))

        # Note: your file naming convention will be different so I can't sort 
        # the dictionary entries.
        #
        file_name = lambda path: path.rsplit('_',1)[1]
        file_num = lambda name: int(name[2:-4]) if name[2:-4].isdigit() else 0
        sort_key = lambda path: file_num(file_name(path))

        # Can't remember why I sort period_dict here..... Seems like it should 
        # be somewhere else
        for period_path in period_dict.keys():
            sorted = period_dict[period_path].sort(key=file_num)
            period_dict[period_path] = sorted

        nd = new_dict
        od = old_dict

        old_dict.update(
            {nk: merge(nd[nk], od[nk] if nk in od else []) for nk in nd.keys()})
        for key in old_dict:
            old_dict[key].sort(key=sort_key)

        return old_dict

    def build_period_dict(self, raw_Qs_txt_files):
        # Build a dict of associated Qs#.txt files per 20 minute periods
        # Dict values are sorted lists of Qs#.txt file paths
        # Dict keys are the results directory paths
        period_dict = {}
        self.logger.write("Building dict of Qs files")
        for Qs_filepath in raw_Qs_txt_files:

            # Extract meta data from filepath
            fpath, fname = nsplit(Qs_filepath, 1)

            if fpath in period_dict:
                period_dict[fpath].append(fname)
            else:
                period_dict[fpath] = [fname]

        # Note: Your project will have a different file naming convention, so 
        # the sorting method that worked for me won't work for you.  Order 
        # should not matter to the Qs_merger.
        #
        ## Sort the associated files
        #for period_path in period_dict:
        #    file_num = lambda s: int(s[2:-4]) if s[2:-4].isdigit() else 0
        #    period_dict[period_path].sort(key=file_num)
        #    #print(period_path[-30:], period_dict[period_path])

        return period_dict

    def pickle_Qs_text_files(self, period_path, Qs_names, Qs_kwargs):
        # Check for preexisting pickles.
        # If they don't exist yet, create new ones and return the filepaths

        # Note: Your naming convention will be different than mine. Therefore, 
        # this code will reuse the name that was given to it.
        
        #_, experiment, step, rtime = nsplit(period_path, 3)
        _, period_name = nsplit(period_path, 1)
        period_name = period_name.replace('results-', '')

        picklepaths = []
        for name in Qs_names:
            #pkl_name = f"{experiment}_{step}_{rtime[8:]}_{name[:-4]}"
            pkl_name = f"{period_name}_{name[:-4]}"

            if self.loader.is_pickled(pkl_name):
                self.logger.write(f'Pickle {pkl_name} preexists. Nothing to do.')
            else:
                self.logger.write(f'Pickling {pkl_name}')
                
                # Read and prep raw data
                filepath = pjoin(period_path, name)
                data = self.loader.load_txt(filepath, Qs_kwargs, add_path=False)

                # Make pickles
                picklepaths += self.make_pickle(pkl_name, data)

        return picklepaths


    def make_pickle(self, pkl_name, data, overwrite=False):
        self.logger.write(f"Performing picklery on {pkl_name}")
        self.logger.increase_global_indent()

        picklepaths = self.loader.produce_pickles({pkl_name:data}, overwrite=overwrite)
        self.logger.decrease_global_indent()
        return picklepaths


#if __name__ == "__main__":
#    crawler = QsExtractor()
#    exp_root = '/home/alex/ubc/research/feed-timing/data'
#    #crawler.set_root(f"{exp_root}/data-links/manual-data")
#    #crawler.run('extract-manual')
#    crawler.set_root(f"{exp_root}/extracted-lighttable-results")
#    crawler.run('extract-light-table')
