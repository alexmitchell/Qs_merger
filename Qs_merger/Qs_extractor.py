from os import join as pjoin
import numpy as np

# From Helpyr
import data_loading
from helpyr_misc import nsplit
from helpyr_misc import ensure_dir_exists
from logger import Logger
from crawler import Crawler

from xlrd.biffh import XLRDError



# ISSUE TO ADDRESS:
# Some Qs.txt and Qs1.txt files appear to be nearly identical copies. The first 
# row is usually different and sometimes a random row where a grain count is 1 
# off. Preference is given to Qs1.txt?

class QsExtractor (Crawler):
    # The Extraction Crawler does the initial work of finding all the data 
    # files and converting them to pickles

    def __init__(self, log_filepath="./log-files/extraction-crawler.txt"):
        logger = Logger(log_filepath, default_verbose=True)
        Crawler.__init__(self, logger)

    def end(self):

    def make_pickle(self, pkl_name, data):
        self.logger.write(f"Performing picklery on {pkl_name}")
        self.logger.increase_global_indent()
        picklepaths = self.loader.produce_pickles({pkl_name:data})
        self.logger.decrease_global_indent()
        return picklepaths


    def set_output_dir(self, dir):
        self.output_dir = dir
        ensure_dir_exists(pickle_dir, self.logger)

    def run(self):
        # Overloads Crawler.run function. The flexibility from run modes is not 
        # necessary for this project.

        # Extract the Qs data from text files and save them as pickles
        self.logger.write_section_break()
        self.logger.write(["Extracting light table data"])

        pickle_dir = self.output_dir
        self.loader = data_loading.DataLoader(data_dir, pickle_dir, self.logger)

        self.logger.write("Finding files")
        sediment_flux_files = self.get_target_files(['Qs?.txt', 'Qs??.txt'],
                verbose_file_list=True)
        if len(sediment_flux_files) == 0:
            self.logger.write("No files found!")
        else:
            self.extract_light_table(sediment_flux_files)

        self.end()

    def extract_light_table(self, sediment_flux_txt_files):
        self.logger.write("Extracting light table data")
        self.logger.increase_global_indent()

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

        period_dict = self.build_period_dict(sediment_flux_txt_files)
        pickle_dict = {}

        # Create new pickles if necessary
        for period_path in period_dict:
            self.logger.write(f"Extracting {period_path}")
            self.logger.increase_global_indent()

            fnames = period_dict[period_path]
            pickle_dict[period_path] = self.pickle_Qs_text_files(
                    period_path, fnames, Qs_kwargs)
            self.logger.decrease_global_indent()

        # Update the metapickle
        # it describes which pkl files belong to which periods
        metapickle_name = "Qs_metapickle"
        if self.loader.is_pickled(metapickle_name):
            # Pickled metapickle already exists.
            # Update the metapickle
            self.logger.write(f"Updating {metapickle_name}...")
            existing = self.loader.load_pickle(metapickle_name, use_source=False)
            pickle_dict = self._merge_metapickle(pickle_dict, existing)
        self.make_pickle(metapickle_name, pickle_dict)

        self.logger.decrease_global_indent()
        self.logger.write("Light table data extraction complete")
    
    def _merge_metapickle(self, new_dict, old_dict):
        merge = lambda a, b: list(set(a + b))
        file_name = lambda path: path.rsplit('_',1)[1]
        file_num = lambda name: int(name[2:-4]) if name[2:-4].isdigit() else 0
        sort_key = lambda path: file_num(file_name(path))

        #period_dict[period_path].sort(key=file_num)

        nd = new_dict
        od = old_dict
        old_dict.update(
            {nk: merge(nd[nk], od[nk] if nk in od else []) for nk in nd.keys()})
        for key in old_dict:
            old_dict[key].sort(key=sort_key)
        return old_dict

    def build_period_dict(self, sediment_flux_txt):
        # Build a dict of associated Qs#.txt files per 20 minute periods
        # Dict values are sorted lists of Qs#.txt file paths
        # Dict keys are the results directory paths
        period_dict = {}
        self.logger.write("Building dict of Qs files")
        for Qs_filepath in sediment_flux_txt:

            # Extract meta data from filepath
            fpath, fname = nsplit(Qs_filepath, 1)

            if fpath in period_dict:
                period_dict[fpath].append(fname)
            else:
                period_dict[fpath] = [fname]

        # Sort the associated files
        for period_path in period_dict:
            file_num = lambda s: int(s[2:-4]) if s[2:-4].isdigit() else 0
            period_dict[period_path].sort(key=file_num)
            #print(period_path[-30:], period_dict[period_path])

        return period_dict

    def pickle_Qs_text_files(self, period_path, Qs_names, Qs_kwargs):
        # Check for preexisting pickles.
        # If they don't exist yet, create new ones and return the filepaths
        _, experiment, step, rtime = nsplit(period_path, 3)

        picklepaths = []
        for name in Qs_names:
            pkl_name = f"{experiment}_{step}_{rtime[8:]}_{name[:-4]}"

            if self.loader.is_pickled(pkl_name):
                self.logger.write(f'Pickle {name} preexists. Nothing to do.')
            else:
                self.logger.write(f'Pickling {name}')
                
                # Read and prep raw data
                filepath = pjoin(period_path, name)
                data = self.loader.load_txt(filepath, Qs_kwargs, add_path=False)

                # Make pickles
                picklepaths += self.make_pickle(pkl_name, data)

        return picklepaths



#if __name__ == "__main__":
#    crawler = QsExtractor()
#    exp_root = '/home/alex/ubc/research/feed-timing/data'
#    #crawler.set_root(f"{exp_root}/data-links/manual-data")
#    #crawler.run('extract-manual')
#    crawler.set_root(f"{exp_root}/extracted-lighttable-results")
#    crawler.run('extract-light-table')
