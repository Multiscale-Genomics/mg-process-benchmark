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

# import logging
import luigi

from luigi.contrib.lsf import LSFJobTask

from tool.bowtie_aligner import bowtie2AlignerTool
from tool.bam_utils import bamUtilsTask

# logger = logging.getLogger('luigi-interface')

class TimeTaskBowtie2(object):  # pylint: disable=too-few-public-methods
    """
    Timer object
    """

    @luigi.Task.event_handler(luigi.Event.PROCESSING_TIME)
    def print_execution_time(self, processing_time):  # pylint: disable=no-self-use
        """
        Print the length of time the task ran for (seconds)
        """
        print('### PROCESSING TIME - Bowtie2 - Single ###: ' + str(processing_time))

class ProcessAlignBowtie2(LSFJobTask, TimeTaskBowtie2):

    genome_fa = luigi.Parameter()
    genome_idx = luigi.Parameter()
    fastq_file = luigi.Parameter()
    output_bam = luigi.Parameter()

    def output(self):
        """
        Returns
        -------
        output : luigi.LocalTarget()
            Location of the aligned reads in bam format
        """
        return luigi.LocalTarget(self.output_bam)

    def work(self):
        line_count = 0
        with open(self.fastq_file, "r") as f_in:
            for line in f_in:
                line_count += 1

        bowtie2_handle = bowtie2AlignerTool({"no-untar" : True})
        bowtie2_handle.bowtie2_aligner_single(
            self.genome_fa,
            self.fastq_file,
            self.output_bam,
            self.genome_idx,
            {}
        )

        bam_handle = bamUtilsTask()
        bam_handle.bam_sort(self.output_bam)