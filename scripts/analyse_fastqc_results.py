"""
.. See the NOTICE file distributed with this work for additional information
   regarding copyright ownership.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

from __future__ import print_function

import argparse
from zipfile import ZipFile

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

class FastqcAdaptor(object):
    """
    Methods for extracting information from the FASTQC results zip file
    """

    def __init__(self, zip_files):
        """
        Initialise the class
        """

        self.zip_files = zip_files

    def get_read_quality_distribution(self):
        """
        Function to retrieve the per sequence quality scores
        """
        final_results = {}
        for zip_location in self.zip_files:
            zip_components = zip_location.split("/")
            with ZipFile(zip_location, "r") as zip_handle:
                results = zip_handle.read(
                    zip_components[-1].replace(".zip", "") + "/fastqc_data.txt")

            if results:
                scores = []
                print_line = False
                for line in results.split("\n"):
                    if line[0:2] == ">>":
                        if line[2:29] == "Per sequence quality scores":
                            print_line = True
                        elif line == ">>END_MODULE":
                            print_line = False

                    if print_line is True:
                        score_line = line.split()
                        try:
                            scores.append(int(float(score_line[1])))
                        except ValueError:
                            pass

                if scores:
                    final_results[zip_components[-1].replace(".zip", "")] = scores

        # print(final_results)

        colour_count = len(final_results)
        colour_map = plt.get_cmap('gist_rainbow')

        fig = plt.figure()
        fig_sub_plot = fig.add_subplot(111)
        fig_sub_plot.set_color_cycle(
            [colour_map(1.*i/colour_count) for i in range(colour_count)]
        )
        plot_handles = []
        for key, value in final_results.iteritems():
            plot_handles += fig_sub_plot.plot(value, label=key)

        fig_sub_plot.legend(handles=plot_handles, prop={'size': 6})

        print("PNG:", zip_components[-1].replace(".zip", ".pdf"))
        fig.savefig(zip_components[-1].replace(".zip", ".pdf"))


if __name__ == "__main__":
    # Set up the command line parameters
    PARSER = argparse.ArgumentParser(description="Extract quality scores")
    PARSER.add_argument("--zip", nargs='*', help="")

    # Get the matching parameters from the command line
    ARGS = PARSER.parse_args()

    FA_HANDLE = FastqcAdaptor(ARGS.zip)
    FA_HANDLE.get_read_quality_distribution()
