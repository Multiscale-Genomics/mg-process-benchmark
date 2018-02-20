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

from tool.bwa_aligner import bwaAlignerTool
from tool.bam_utils import bamUtilsTask

# logger = logging.getLogger('luigi-interface')

class TimeTaskBwaAln(object):  # pylint: disable=too-few-public-methods
    """
    Timer object
    """

    @luigi.Task.event_handler(luigi.Event.PROCESSING_TIME)
    def print_execution_time(self, processing_time):  # pylint: disable=no-self-use
        """
        Print the length of time the task ran for (seconds)
        """
        print('### PROCESSING TIME - BWA ALN - Single ###: ' + str(processing_time))

class ProcessAlnBwaSingle(LSFJobTask, TimeTaskBwaAln):
    """
    Tool wrapper for aligning single end reads using BWA ALN
    """

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
        """
        Worker function for splitting the FASTQ file into smaller chunks

        Parameters
        ----------
        genome_fa : str
            Location of the FASTA file of the genome to align the reads to
        genome_idx : str
            Location of the index files in .tar.gz file prepared by the BWA
            indexer
        fastq_file : str
            Location of the FASTQ file
        output_bam : str
            Location of the aligned reads in bam format
        """
        line_count = 0
        with open(self.fastq_file, "r") as f_in:
            for line in f_in:
                line_count += 1

        bwa_handle = bwaAlignerTool({"no-untar" : True})
        bwa_handle.bwa_aligner_single(
            self.genome_fa,
            self.fastq_file,
            self.output_bam,
            self.genome_idx,
            {}
        )

        bam_handle = bamUtilsTask()
        bam_handle.bam_sort(self.output_bam)
