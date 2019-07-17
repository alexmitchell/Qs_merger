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

class Settings:
    root_dir = "E:\LT_Qs_Combine\LT_Results" # Windows style path
    #root_dir = "/home/alex/ubc/feed-timing/data" # Unix style path
    lighttable_data_dir = pjoin(root_dir, "merged-lighttable-results")
    Qs_raw_pickles_dir = pjoin(lighttable_data_dir, "raw-pickles")
    Qs_primary_pickles_dir = pjoin(lighttable_data_dir, "merged-pickles")

    lighttable_bedload_cutoff = 800 # g/s max rate


# Primary Pickle Processor takes raw Qs and Qsn pickles and condenses them into 
# one Qs pickle for each period. ie. Does processing within each period.

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

    def __init__(self, output_txt=False):
        # File locations
        self.root_dir = Settings.root_dir
        self.pickle_source = Settings.Qs_raw_pickles_dir
        self.pickle_destination = Settings.Qs_primary_pickles_dir
        self.txt_destination = f"{self.root_dir}/combined-txts"
        self.log_filepath = "./log-files/Qs_primary_processor.txt"
        self.metapickle_name = "Qs_metapickle"
        self.statspickle_name = "Qs_summary_stats"
        self.output_txt = output_txt
        
        # tolerance for difference between files
        # This value is more to highlight very different dataframes than have 
        # any physical meaning.
        self.difference_tolerance = 0.02

        # Start up logger
        self.logger = logger.Logger(self.log_filepath, default_verbose=True)
        hm.ensure_dir_exists(self.pickle_destination, self.logger)
        self.logger.write(["Begin Primary Pickle Processor output", asctime()])

        # Start up loader
        self.loader = data_loading.DataLoader(self.pickle_source, 
                self.pickle_destination, self.logger)

    def run(self):
        self.logger.write(["Running pickle processor..."])
        indent_function = self.logger.run_indented_function

        # Load Qs_metapickle
        self.metapickle = self.loader.load_pickle(self.metapickle_name)
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
            _, experiment, step, rperiod = hm.nsplit(self.current_period_path, 3)
            period = rperiod[8:]
            msg = f"Processing {experiment} {step} {period}..."
            self.pkl_name = '_'.join(['Qs', experiment, step, period])

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
        max_threshold = Settings.lighttable_bedload_cutoff
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

        ## Deal with special cases
        self._check_special_cases()

        ## Check for accumulated overlap
        self._check_accumulated_overlap()

        ## Check for total number of rows
        self._fix_n_rows()

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

    def _check_special_cases(self):
        _, experiment, step, rperiod = hm.nsplit(self.current_period_path, 3)
        period = rperiod[8:]

        # Deal with special cases
        # See analysis_notes.txt for more info
        special_case = ('1A', 'rising-62L', 't40-t60')
        if special_case == (experiment, step, period):
            self.logger.write(f"Addressing special case {special_case}")
            # delete several chunks of bad data from the text file
            # Longer chunk may be from pausing the lighttable program, shorter 
            # chunks may be due to low frame rate
            #
            # Delete in reverse order (lastest first) so line numbers don't 
            # shift for the next deletion.
            self.final_output = self._delete_chunk(1631, 1638, self.final_output)
            self.final_output = self._delete_chunk(726, 1076, self.final_output)
            self.final_output = self._delete_chunk(709, 714, self.final_output)
            self.final_output = self._delete_chunk(622, 703, self.final_output)
            self.final_output = self._delete_chunk(548, 561, self.final_output)
            self.final_output = self._delete_chunk(494, 526, self.final_output)
            self.final_output = self._delete_chunk(412, 417, self.final_output)
            self.final_output = self._delete_chunk(390, 397, self.final_output)

        special_case = ('2A', 'rising-50L', 't00-t60')
        if special_case == (experiment, step, period):
            self.logger.write(f"Addressing special case {special_case}")
            # delete lines 1343 to 3916 from the text file
            # Appears that I forgot to stop the lighttable program when pausing 
            # the flow to clean out the trap
            self.final_output = self._delete_chunk(1343, 3916, self.final_output)

        special_case = ('3A', 'rising-75L', 't00-t20')
        if special_case == (experiment, step, period):
            print(f"Addressing special case {special_case}")
            # delete lines 925 to 2368 from the text file
            # Appears that I forgot to stop the lighttable program when pausing 
            # the flow to clean out the trap
            self.final_output = self._delete_chunk(925, 2368, self.final_output)

        special_cases = [
                # These special cases appear to have high variability from the 
                # graphs
                ('1B', 'rising-62L', 't20-t40'),
                ('1B', 'rising-62L', 't40-t60'),
                ('2A', 'rising-87L', 't00-t20'),
                ('2A', 'rising-87L', 't20-t40'),
                ('2B', 'falling-87L', 't00-t20'),
                ('2B', 'falling-87L', 't20-t40'),
                ('2B', 'falling-87L', 't40-t60'),
                ('2B', 'falling-75L', 't00-t20'),
                ('2B', 'falling-75L', 't20-t40'),
                ('2B', 'falling-75L', 't40-t60'),
                ('3A', 'falling-87L', 't00-t20'),
                ('3A', 'falling-87L', 't20-t40'),
                ('3A', 'falling-87L', 't40-t60'),
                ('5A', 'rising-62L', 't00-t20'),
                ('5A', 'rising-62L', 't20-t40'),
                ('5A', 'rising-75', 't00-t20'),
                ]
        if (experiment, step, period) in special_cases:
            self.logger.write_blankline(2)
            self.logger.write(f"Suspicious case {(experiment, step, period)}")
            self.logger.write_blankline(2)
            sleep(3)
            #assert(False)

    def _delete_chunk(self, fstart, fend, data):
        # For ease of use, fstart and fend are the line numbers in the 
        # combined Qs file. The data between those lines (inclusive) will 
        # be deleted and the timestamps reset to remove any gap
        start, end = fstart-2, fend-2
        self.logger.write(f"Deleting data lines {start} through {end}.")

        # Delete the lines
        index = data.index
        output = data[(index < start) | (end < index)]

        # Fix timestamps and indices
        # Note, using the values method gets references to the underlying 
        # data locations. Therefore I can change them without setting of a 
        # copy warnings.
        output.loc[:, 'timestamp'].values[start:] -= end - start + 1
        output.set_index('timestamp', inplace=True)
        output.reset_index(inplace=True)

        return output

    def _fix_n_rows(self):
        # Check for total number of rows
        # eg. trim to 1200 rows for a 20 minute period (1 row / sec)
        _, experiment, step, rperiod = hm.nsplit(self.current_period_path, 3)
        period = rperiod[8:]

        start, end = [int(t[1:]) for t in period.split(sep='-')]
        target_n_rows = abs(end - start) * 60 # one row per second
        n_rows, ncols = self.final_output.shape

        def check_if_extra(row_idx):
            row = self.final_output.iloc[row_idx, :]
            empty = row.isnull().any()
            zero = (row[['Bedload all', 'Count all']] == 0).all()
            return empty or zero

        # Delete extra lines from end
        while n_rows > target_n_rows and check_if_extra(-1):
            self.final_output = self.final_output.iloc[:-1, :]
            n_rows, ncols = self.final_output.shape
            self.logger.write(f"Dropping last row (new n_rows = {n_rows})")

        # Delete extra lines from beginning
        while n_rows > target_n_rows and check_if_extra(0):
            self.final_output = self.final_output.iloc[1:, :]
            n_rows, ncols = self.final_output.shape
            self.logger.write(f"Dropping first row (new n_rows = {n_rows})")

        # Add or delete rows
        n_rows, ncols = self.final_output.shape
        if n_rows > target_n_rows:
            # Delete data from head (~25%) and tail (~75%) to reach desired # 
            # of rows
            n_drop = n_rows - target_n_rows
            self.logger.write(f"Need to drop {n_drop} rows containing data")
            head_drop = n_drop // 4
            tail_drop = n_drop - head_drop

            total_mass = self.final_output.loc[:, 'Bedload all'].sum()
            head = self.final_output.iloc[0:head_drop, :]
            tail = self.final_output.iloc[-tail_drop:, :]
            self.final_output = self.final_output.iloc[head_drop:-tail_drop, :]
            self.previous_tail = tail

            head_mass = head.loc[:, 'Bedload all'].sum()
            tail_mass = tail.loc[:, 'Bedload all'].sum()
            loss_ratio = (head_mass + tail_mass) / total_mass
            self.logger.write([
                f"Dropping {head_drop}s ({head_mass:0.2f}g) from head.",
                f"Dropping {tail_drop}s ({tail_mass:0.2f}g) from tail",
                f"Original total mass = {total_mass:0.2f}g ({loss_ratio:0.2%} trimmed)"])
            if 0.04 < loss_ratio <= 0.05:
                self.logger.write_blankline(2)
                self.logger.write(f"###  {loss_ratio:0.2%} is high!  ###")
                self.logger.write_blankline(2)
                sleep(4)
            elif 0.05 < loss_ratio:
                self.logger.write_blankline(2)
                self.logger.write(f"###  {loss_ratio:0.2%} is above tolerance!  ###")
                self.logger.write_blankline(2)
                assert(False)

        elif n_rows < target_n_rows:
            n_needed = target_n_rows - n_rows
            self.logger.write(f"Too few rows. Adding {n_needed} blank rows.")
            last_row = self.final_output.iloc[-1, :]
            last_timestamp = last_row['timestamp']
            n_cols  = last_row.shape[0]

            needed_range = np.arange(n_needed)

            np_empty = np.empty((n_needed, n_cols))
            np_empty.fill(np.nan)
            np_empty[:,0] = (needed_range + 1 + last_timestamp).T
            pd_empty = pd.DataFrame(np_empty,
                columns=last_row.index, index=needed_range + n_rows)
            self.final_output = pd.concat([self.final_output, pd_empty])

        n_rows, ncols = self.final_output.shape
        self.logger.write(f"Final number of rows = {n_rows}")


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
        self.combined_file_counter += 1

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
    crawler = QsExtractor()
    exp_root = Settings.root_dir
    crawler.set_root(f"{exp_root}/extracted-lighttable-results")
    crawler.set_output_dir(Settings.Qs_raw_pickles_dir)
    crawler.run()

    # Run the script
    primary = QsPickleProcessor(output_txt=True)
    primary.run()