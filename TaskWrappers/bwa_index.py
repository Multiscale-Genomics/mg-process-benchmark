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

import luigi

from luigi.contrib.lsf import LSFJobTask

from tool.bwa_indexer import bwaIndexerTool

class ProcessIndexBwa(LSFJobTask):
    """
    Tool wrapper for generating a BWA Index
    """

    genome_fa = luigi.Parameter()
    genome_idx = luigi.Parameter()

    def output(self):
        """
        Returns
        -------
        output : luigi.LocalTarget()
            Location of the compressed index
        """
        return luigi.LocalTarget(self.genome_idx)

    def work(self):
        """
        Worker function for indexing genomes in FASTA format using BWA

        Parameters
        ----------
        genome_fa : str
            Location of the FASTA file of the genome to index
        genome_idx : str
            Location of the aligned reads in bam format
        """

        bwa_handle = bwaIndexerTool()
        bwa_handle.bwa_indexer(self.genome_fa, self.genome_idx)
