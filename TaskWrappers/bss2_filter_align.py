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

import os
# import logging
import luigi

from luigi.contrib.lsf import LSFJobTask

from tool.bs_seeker_aligner import bssAlignerTool
from tool.bs_seeker_filter import filterReadsTool
from tool.bam_utils import bamUtilsTask

# logger = logging.getLogger('luigi-interface')

class TimeTaskBSSeekerFilterAlign(object):  # pylint: disable=too-few-public-methods
    """
    Timer object
    """

    @luigi.Task.event_handler(luigi.Event.PROCESSING_TIME)
    def print_execution_time(self, processing_time):  # pylint: disable=no-self-use
        """
        Print the length of time the task ran for (seconds)
        """
        print('### PROCESSING TIME - BS Seeker 2 Filter and Align ###: ' + str(processing_time))

class ProcessBSSeekerFilterAlignSingle(LSFJobTask, TimeTaskBSSeekerFilterAlign):

    genome_fa = luigi.Parameter()
    genome_idx = luigi.Parameter()
    fastq_file = luigi.Parameter()
    fastq_filtered = luigi.Parameter()
    aligner = luigi.Parameter()
    aligner_path = luigi.Parameter()
    bss_path = luigi.Parameter()
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
        Worker function for aligning single ended FASTQ reads using Bowtie2

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

        frt = filterReadsTool()
        frt.bss_seeker_filter(
            self.fastq_file,
            self.fastq_filtered,
            self.bss_path
        )

        bss_aligner = bssAlignerTool({"no-untar" : True})
        bss_aligner.bs_seeker_aligner_single(
            self.fastq_filtered,
            self.aligner, self.aligner_path, self.bss_path, [],
            self.genome_fa, self.genome_idx,
            self.output_bam
        )

        bam_handle = bamUtilsTask()
        bam_handle.bam_sort(self.output_bam)

class ProcessBSSeekerFilterAlignPaired(LSFJobTask, TimeTaskBSSeekerFilterAlign):

    retry_count = 1

    genome_fa = luigi.Parameter()
    genome_idx = luigi.Parameter()
    fastq_file_1 = luigi.Parameter()
    fastq_file_2 = luigi.Parameter()
    fastq_filtered_1 = luigi.Parameter()
    fastq_filtered_2 = luigi.Parameter()
    aligner = luigi.Parameter()
    aligner_path = luigi.Parameter()
    bss_path = luigi.Parameter()
    output_bam = luigi.Parameter()

    def _filter_reads_mp(self, fastq_in, fastq_out, filter_path):
        frt = filterReadsTool()
        frt.bss_seeker_filter(fastq_in, fastq_out, filter_path)

    def _match_paired_fastq(self, fastq_1, fastq_2):
        from tool.fastqreader import fastqreader

        fqr = fastqreader()
        fqr.openFastQ(fastq_1, fastq_2)
        fqr.createOutputFiles('match')

        record1 = fqr.next(1)
        record2 = fqr.next(2)

        count_r1 = 0
        count_r2 = 0
        count_r3 = 0

        while fqr.eof(1) is False and fqr.eof(2) is False:
            r1_id = record1["id"].split(" ")
            r2_id = record2["id"].split(" ")

            if r1_id[0] == r2_id[0]:
                fqr.writeOutput(record1, 1)
                fqr.writeOutput(record2, 2)

                record1 = fqr.next(1)
                record2 = fqr.next(2)

                count_r1 += 1
                count_r2 += 1
                count_r3 += 1
            elif r1_id[0] < r2_id[0]:
                record1 = fqr.next(1)
                count_r1 += 1
            else:
                record2 = fqr.next(2)
                count_r2 += 1

        fqr.closeFastQ()
        fqr.closeOutputFiles()

        with open(fqr.f1_output_file_loc, "rb") as f1_match:
            with open(fastq_1, "wb") as f_old:
                f_old.write(f1_match.read())

        with open(fqr.f2_output_file_loc, "rb") as f2_match:
            with open(fastq_2, "wb") as f_old:
                f_old.write(f2_match.read())

        os.remove(fqr.f1_output_file_loc)
        os.remove(fqr.f2_output_file_loc)

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
        Worker function for aligning single ended FASTQ reads using Bowtie2

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

        import multiprocessing

        f1_proc = multiprocessing.Process(
            name='fastq_1', target=self._filter_reads_mp,
            args=(self.fastq_file_1, self.fastq_filtered_1, self.bss_path)
        )
        f2_proc = multiprocessing.Process(
            name='fastq_2', target=self._filter_reads_mp,
            args=(self.fastq_file_2, self.fastq_filtered_2, self.bss_path)
        )

        f1_proc.start()
        f2_proc.start()

        f1_proc.join()
        f2_proc.join()

        self._match_paired_fastq(self.fastq_filtered_1, self.fastq_filtered_2)

        bss_aligner = bssAlignerTool({"no-untar" : True})
        bss_aligner.bs_seeker_aligner(
            self.fastq_filtered_1, self.fastq_filtered_2,
            self.aligner, self.aligner_path, self.bss_path, [],
            self.genome_fa, self.genome_idx,
            self.output_bam
        )

        bam_handle = bamUtilsTask()
        bam_handle.bam_sort(self.output_bam)
