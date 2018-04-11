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

from tool.bs_seeker_indexer import bssIndexerTool

# logger = logging.getLogger('luigi-interface')


class ProcessBSSeekerIndexer(LSFJobTask):
    """
    Filtering of FASTQ paired-end reads prior to alignment
    """

    retry_count = 1

    genome_fa = luigi.Parameter()
    genome_idx = luigi.Parameter()
    aligner = luigi.Parameter()
    aligner_path = luigi.Parameter()
    bss_path = luigi.Parameter()

    def output(self):
        """
        Returns
        -------
        output : luigi.LocalTarget()
            Location of the aligned reads in bam format
        """
        return luigi.LocalTarget(self.genome_idx)

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

        bss_idx_handle = bssIndexerTool({"no-untar": True})
        bss_idx_handle.bss_build_index(
            self.genome_fa, self.aligner, self.aligner_path, self.bss_path, [],
            self.genome_idx
        )
