#!/usr/bin/env python3

# Qs# pickles are panda dataframes directly translated from the raw txt files

import matplotlib.pyplot as plt
from os.path import join as pjoin
import numpy as np
import pandas as pd
from time import asctime
from time import sleep


# From Helpyr
from helpyr import data_loading
from helpyr import logger
from helpyr import helpyr_misc as hm

import settings
import Qs_extractor


# Primary Pickle Processor takes raw Qs and Qsn pickles and condenses them into 
# one Qs pickle for each period. ie. Does processing within each period.

# Note: a period is one continuous run of the lighttable. There will be one or 
# more Qs txt files associated with each period.

class QsPickleProcessor:

    # Outline:
    # 1) load Qs_metapickle
    # 2) load Qs pickles for a period
    # 3) error check raw qs dataframes
    #   - conflict between qs and qs#?
    # 4) combine qs dataframes
    # 5) error check combined qs dataframe

    # Note: The primary processor uses attribute data (eg. self.variable) to 
    # get information into functions rather than repetitively pass a list of 
    # arguments. In retrospect, kwargs may have been a better choice because 
    # using attribute variables that update every iteration of a for loop makes 
    # it hard to follow in such a large class.
    error_codes = {
            'CQF' : "Conflicting Qs files",
            'NDF' : "No Data Found",
            'MMD' : "Mismatched Data",
            }

    def __init__(self, output_txt=False, metapickle_path=None):
        # File locations
        self.root_dir = settings.root_dir
        self.pickle_source = settings.Qs_raw_pickles_dir
        self.pickle_destination = settings.Qs_merged_pickles_dir
        self.txt_destination = settings.Qs_merged_txt_dir
        self.log_filepath = pjoin(settings.root_dir, 'log-files', 'QsPickleProcessor.txt')
        self.metapickle_path = metapickle_path
        self.statspickle_name = "Qs_summary_stats"
        self.output_txt = output_txt
        
        # tolerance for difference between files
        # This value is more to highlight very different dataframes than have 
        # any physical meaning.
        self.difference_tolerance = 0.02

        # Start up logger
        self.logger = logger.Logger(self.log_filepath, default_verbose=True)
        hm.ensure_dir_exists(self.pickle_destination, self.logger)
        self.logger.write(["Begin Qs Pickle Processor output", asctime()])

        # Start up loader
        self.loader = data_loading.DataLoader(self.pickle_source, 
                self.pickle_destination, self.logger)

    def run(self):
        self.logger.write(["Running pickle processor..."])
        indent_function = self.logger.run_indented_function

        # Load Qs_metapickle
        if self.metapickle_path is None:
            self.metapickle = self.loader.load_pickle(settings.metapickle_name)
        else:
            self.metapickle = self.loader.load_pickle(self.metapickle_path, 
                    add_path=False)

        self.raw_file_counter = 0
        self.combined_file_counter = 0
        self.summary_stats = {} # (pkl_name, stat_type) : stat_row}
        self.pd_summary_stats = None

        for period_path in self.metapickle:
            # attribute data to be reset every period
            self.lingering_errors = [] # error for secondary check to look at
            self.Qs_path_list = [] # list of Qs#.txt file paths
            self.Qs0_data = None # data for Qs.txt
            self.Qsn_data = [] # data for Qs#.txt
            self.Qsn_names = [] # Names of Qs# files
            self.current_period_path = period_path # is also the metapickle key
            self.combined_Qs = None
            self.accumulating_overlap = None

            # Get meta info
            period_name = hm.nsplit(self.current_period_path, 1)[1]
            period_name = period_name.replace('results-', '')
            msg = f"Processing {period_name}..."
            self.pkl_name = '_'.join(['Qs', period_name])

            indent_function(self.process_period, before_msg=msg)

        # Make a summary stats dataframe
        self.pd_summary_stats = pd.DataFrame.from_dict(
                self.summary_stats, orient='index')
        self.update_summary_stats()

        # Save summary stats pickle/txt files
        indent_function(self.produce_stats_pickle, 
                before_msg="Producing statistics pickle",
                after_msg="Statistics pickle produced!")
        if self.output_txt:
            indent_function(self.write_stats_txt, 
                    before_msg="Writing statistics txt",
                    after_msg="Done!")

        self.logger.write([f"{self.raw_file_counter} raw pickles processed",
                           f"{self.combined_file_counter} combined pickles produced"])
        self.logger.end_output()


    def process_period(self):

        if self.loader.is_pickled(self.pkl_name):
            self.logger.write(["Nothing to do"])
            return

        indent_function = self.logger.run_indented_function
        # Load data
        indent_function(self.load_data,
                        before_msg="Loading data...",
                        after_msg="Finished loading data!")

        # Primary Error Checks
        indent_function(self.primary_error_check,
                        before_msg="Running primary error checks...",
                        after_msg="Finished primary error checks!")

        # Combining Qsn chunks
        indent_function(self.combine_Qsn_chunks,
                        before_msg="Combining Qs chunks...",
                        after_msg="Finished combining Qs chunks!")

        # Secondary Error Checks
        indent_function(self.secondary_error_check,
                        before_msg="Running secondary error checks...",
                        after_msg="Finished secondary error checks!")

        # Calc summary stats
        indent_function(self.calculate_stats,
                        before_msg="Calculating summary stats...",
                        after_msg="Summary stats calculated!")

        # Write to pickle
        indent_function(self.produce_processed_pickle,
                        before_msg="Producing processed pickles...",
                        after_msg="Processed pickles produced!")

        # Write a combined Qs txt file
        if self.output_txt:
                indent_function(self.write_combined_txt,
                        before_msg="Writing combined txt file...",
                        after_msg="Done writing file!")


    def load_data(self):
        # Load the sorted list of paths for this period
        self.Qs_path_list = self.metapickle[self.current_period_path]
        # Load the associated data

        Qs_period_data = self.loader.load_pickles(self.Qs_path_list, add_path=False)

        for Qs_path in self.Qs_path_list:
            pkl_name = hm.nsplit(Qs_path, 1)[1]
            stripped_name = pkl_name.split('.')[0]
            Qs_name = stripped_name.split('_')[-1]
            bedload_data = Qs_period_data[Qs_path]
            self.raw_file_counter += 1

            if Qs_name == 'Qs':
                assert(self.Qs0_data is None)
                self.Qs0_data = bedload_data
            else:
                assert(Qs_name[2:].isdigit())
                self.Qsn_data.append(bedload_data)
                self.Qsn_names.append(Qs_name)


    def primary_error_check(self):
        # 3) error check raw qs dataframes
        #   - conflict between qs and qs#?

        if self.Qs0_data is not None and self.Qsn_data:
            name_list = ', '.join(self.Qsn_names)
            error_msg = QsPickleProcessor.error_codes['CQF']
            self.logger.warning([error_msg,
                "Qs.txt and Qs#.txt files both exist",
               f"Qs#.txt: {name_list}"])
            self.lingering_errors.append(error_msg)


    def combine_Qsn_chunks(self):
        # 4) combine qs dataframes
        # The data is split up into multiple chunks (each Qs# file is a chunk).  
        # This functions assembles them into a complete Qs dataframe.  
        # Overlapping rows are converted to nan because I can't see any way to 
        # choose which one to keep.
        if not self.Qsn_data:
            self.logger.write("No chunks to combine.")
            return

        combined = self._make_like_df(self.Qsn_data[0], ['timestamp'])
        accumulating_overlap = None

        exclude_cols = ['timestamp', 'missing ratio', 'vel', 'sd vel', 'number vel']
        target_cols = hm.exclude_df_cols(combined, exclude_cols)

        # Set up a few lambda functions
        get_num = lambda s: int(s[2:]) # get the file number from the name
        get_target_subset = lambda c: c.loc[:, target_cols]
        # Find rows with data. Should remove meta columns beforehand
        # Will select rows with non-null values (selects zero rows)
        find_data_rows = lambda df: df.notnull().all(axis=1)

        for raw_chunk, name in zip(self.Qsn_data, self.Qsn_names):
            # Each raw dataframe contains only a chunk of the overall data.  
            # However they contain zero values for all times outside of the 
            # valid chunk time. Some chunks overlap too. 
            ch_num, max_num = get_num(name), get_num(self.Qsn_names[-1])
            self.logger.write(f"Processing chunk {ch_num} of {max_num}")

            # Get bedload subsets
            bedload_chunk = get_target_subset(raw_chunk)
            bedload_combined = get_target_subset(combined)

            # Find rows with data
            chunk_rows = find_data_rows(bedload_chunk)
            combined_rows = find_data_rows(bedload_combined)

            # Find overlap
            overlap_rows = chunk_rows & combined_rows

            # Add chunk to combined array
            combined.loc[chunk_rows, 1:] = raw_chunk[chunk_rows]
            combined.loc[overlap_rows, 1:] = np.nan

            # Keep track of overlap rows
            if accumulating_overlap is None:
                accumulating_overlap = overlap_rows
            else:
                accumulating_overlap = accumulating_overlap | overlap_rows

        self.combined_Qs = combined
        self.accumulating_overlap = accumulating_overlap

    def _make_like_df(self, like_df, columns_to_copy=[], fill_val=np.nan):
        # Make a dataframe like the Qs data with a few columns copied and the 
        # rest filled with a default value

        np_like = np.empty_like(like_df.values)
        np_like.fill(fill_val)
        pd_like = pd.DataFrame(np_like,
                columns=like_df.columns, index=like_df.index)

        for column in columns_to_copy:
            pd_like.loc[:, column] = like_df.loc[:, column]
        return pd_like


    def secondary_error_check(self):
        # 5) error check combined qs dataframe
        
        self.final_output = None

        ## Check for diff between raw_Qs and Qs_combined
        self._check_diff_raw_combined()

        ## Set rows with any Nan values to entirely Nan values
        nan_rows = self.final_output.isnull().any(axis=1)
        self.final_output.loc[nan_rows, 'missing ratio':] = np.nan

        ## Set outliers to Nan
        max_threshold = settings.lighttable_bedload_cutoff
        trim_rows = self.final_output['Bedload all'] > max_threshold
        trim_count = np.sum(trim_rows)
        if trim_count > 0:
            trim_vals = self.final_output.loc[trim_rows, 'Bedload all']
            str_trim_vals = [f'{v:0.2f}' for v in np.sort(trim_vals.values)]
            trim_sum = np.sum(trim_vals)
            total_sum = np.sum(self.final_output['Bedload all'])

            self.final_output.loc[trim_rows, 'missing ratio':] = np.nan

            self.logger.write([
                    f"{trim_count} points are above the cutoff value of {max_threshold}" +
                    f" ({trim_sum/1000:0.3f} kg of {total_sum/1000:0.3f} kg; " +
                    f"{trim_sum/total_sum:0.2%} )",
                    f"{list(str_trim_vals)}"])

            #self.final_output.hist(column='Bedload all', bins=50)
            #plt.show()
        else:
            self.logger.write("No values needed to be trimmed")

        ## Check for accumulated overlap
        self._check_accumulated_overlap()

    def _check_diff_raw_combined(self):
        # Check for diff between raw_Qs and Qs_combined
        raw_Qs = self.Qs0_data
        raw_exists = raw_Qs is not None
        combined_Qs = self.combined_Qs
        combined_exists = combined_Qs is not None
        if raw_exists and combined_exists:
            self._difference_check()
        elif not(raw_exists or combined_exists):
            error_msg = QsPickleProcessor.error_codes['NDF']
            self.logger.warning([error_msg,
                "Both the raw Qs pickle and combined Qs df are missing."])
        else:
            using = "raw Qs" if raw_exists else "combined Qs"
            self.final_output = raw_Qs if raw_exists else combined_Qs
            self.logger.write(f"Only {using} found." +
                              "No difference check needed.")

    def _difference_check(self):
        # Look at the difference between the Qs.txt and Qs combined data.
        raw_Qs = self.Qs0_data
        combined_Qs = self.combined_Qs

        # Element-wise bool difference between dataframes
        Qs_diff = (combined_Qs != raw_Qs)
        # Rows that have Nan values in both dataframes will be thrown out and 
        # should not count towards the difference.
        # Rows that started with a value and ended with Nan should count. (such 
        # as overlap rows)
        Qs_both_nan = combined_Qs.isnull() & raw_Qs.isnull()
        both_nan_rows = Qs_both_nan.any(axis=1)
        Qs_diff.loc[both_nan_rows, :] = False

        # Ignore columns that are likely to be different and don't seem to have 
        # any practical value. (I think....?)
        exclude_cols = ['missing ratio', 'vel', 'sd vel', 'number vel']
        Qs_diff.loc[:, exclude_cols] = False

        # Isolate the rows and columns where values are different
        #Qs_diff.loc[0,:] = False # ignore first row
        diff_rows = Qs_diff.any(axis=1)
        diff_cols = Qs_diff.any(axis=0)
        any_diff = diff_rows.any()

        if any_diff:
            # Get some metrics on difference
            diff_rows_count = diff_rows.sum()
            rows_count = diff_rows.shape[0]
            diff_ratio = diff_rows_count / rows_count
            tolerance = self.difference_tolerance

            is_tolerant = '' if diff_ratio < tolerance else ' NOT'
            error_msg = QsPickleProcessor.error_codes['MMD']
            msgs = [error_msg,
                    f"Difference ratio of {diff_ratio:.3f} is{is_tolerant} within tolerance of {tolerance}.",
                    f"{diff_rows_count} conflicting rows found out of {rows_count}",
                    f"Using combined Qs data",
                    ]
            self.logger.warning(msgs)

            # Write differing rows/cols to log
            diff_raw_Qs = raw_Qs.loc[diff_rows, diff_cols]
            diff_combined = combined_Qs.loc[diff_rows, diff_cols]
            self.logger.write_dataframe(diff_raw_Qs, "Raw Qs")
            self.logger.write_dataframe(diff_combined, "Combined Qs")

            #if diff_ratio < diff_tolerance:
            #    raise NotImplementedError

            self.final_output = combined_Qs
            # default to using the combined output

        else:
            self.logger.write(["Qs.txt matches combined Qs chunk data",
                              "(Excluding velocity columns and missing ratio)"])
            self.final_output = combined_Qs

    def _check_accumulated_overlap(self):
        # Check for accumulated overlap
        combined_exists = self.combined_Qs is not None
        overlap = self.accumulating_overlap
        if combined_exists and overlap.any():
            overlap_times = self.combined_Qs.loc[overlap,'timestamp']
            str_overlap_times = overlap_times.to_string(float_format="%f")

            self.logger.write(["The following timestamps were overlapped: "])
            self.logger.write(str_overlap_times.split('\n'), local_indent=1)


    def calculate_stats(self):
        # Calc column averages and sums
        name = self.pkl_name
        data = self.final_output
        av = data.mean(axis=0)
        sum = data.sum(axis=0)
        nans = data.isnull().sum(axis=0)

        for stat, row in zip(['av', 'sum', 'nans'],[av, sum, nans]):
            key = (name, stat)
            # Add the series data to the summary stats dict
            # The dict will be converted into a multiindexed dataframe later
            self.summary_stats[key] = row
            #row_str = row.to_string()
            #msg = f"Stats for {key} : {row_str}"
            #self.logger.write(msg)

    def produce_processed_pickle(self):
        if self.final_output is not None:
            prepickles = {self.pkl_name : self.final_output}
            # prepickles is a dictionary of {'pickle name':data}
            self.loader.produce_pickles(prepickles)
            self.combined_file_counter += 1
        else:
            error_msg = QsPickleProcessor.error_codes['NDF']
            self.logger.warning([error_msg,
                f"Pickle not created for {self.pkl_name}"])

    def write_combined_txt(self):
        filename = f"{self.pkl_name}.txt"
        filepath = pjoin(self.txt_destination, filename)
        data = self.final_output
        
        self.loader.save_txt(data, filepath, is_path=True)


    def update_summary_stats(self):
        summary_stats = self.pd_summary_stats
        pkl_name = self.statspickle_name 

        if summary_stats.empty:
            self.logger.write(["No new stats. Nothing to do."])
            return

        if self.loader.is_pickled(pkl_name):
            self.logger.write(["Stats pickle already exists. Updating..."])
            old_stats = self.loader.load_pickle(pkl_name, use_source=False)
            unchanged_indices = ~old_stats.index.isin(summary_stats)
            new_indices_strs = summary_stats.index.levels[0].__str__().split('\n')
            summary_stats = pd.concat([old_stats[unchanged_indices],
                                       summary_stats])
            self.logger.write(["Updated index values are:"] + new_indices_strs)
        else:
            self.logger.write(["Making new stats pickle. Updating..."])
            # prepickles is a dictionary of {'pickle name':data}

        self.pd_summary_stats = summary_stats

    def produce_stats_pickle(self):
        summary_stats = self.pd_summary_stats
        pkl_name = self.statspickle_name 

        prepickles = {pkl_name : summary_stats}
        self.loader.produce_pickles(prepickles)
        #self.combined_file_counter += 1

    def write_stats_txt(self):
        filename = f"{self.statspickle_name}.txt"
        filepath = pjoin(self.txt_destination, filename)
        data = self.pd_summary_stats
        
        kwargs = {'index'  : True,
                  'header' : True,
                  }
        self.loader.save_txt(data, filepath, kwargs=kwargs, is_path=True)



if __name__ == "__main__":
    # Run the extraction crawler
    crawler = Qs_extractor.QsExtractor(
            root_dir = settings.root_dir,
            output_dir = settings.Qs_raw_pickles_dir,
            )
    metapickle_path = crawler.run()

    # Run the script
    merger = QsPickleProcessor(output_txt=True, 
            metapickle_path=metapickle_path)
    merger.run()
