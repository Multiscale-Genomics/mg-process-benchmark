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
from tool.bam_utils import bamUtils
from tool.fastq_utils import fastqUtils

# logger = logging.getLogger('luigi-interface')

class ProcessBSSeekerFilterAlignSingle(LSFJobTask):
    """
    Filtering of FASTQ single-ended reads prior to alignment
    """

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

        bam_handle = bamUtils()
        bam_handle.bam_sort(self.output_bam)


class ProcessBSSeekerFilterAlignPaired(LSFJobTask):
    """
    Filtering of FASTQ paired-end reads prior to alignment
    """

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

        fq_handle = fastqUtils()
        fq_handle.fastq_sort_file(fastq_out)

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

        fq_handle = fastqUtils()
        fq_handle.fastq_match_paired_ends(self.fastq_filtered_1, self.fastq_filtered_2)

        bss_aligner = bssAlignerTool({"no-untar" : True})
        bss_aligner.bs_seeker_aligner(
            self.fastq_filtered_1, self.fastq_filtered_2,
            self.aligner, self.aligner_path, self.bss_path, [],
            self.genome_fa, self.genome_idx,
            self.output_bam
        )

        bam_handle = bamUtils()
        bam_handle.bam_sort(self.output_bam)
