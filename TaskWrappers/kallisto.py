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

from tool.kallisto_indexer import kallistoIndexerTool
from tool.kallisto_quant import kallistoQuantificationTool


class ProcessKallistoSingle(LSFJobTask):
    """
    Tool wrapper for filtering bams using BioBamBam.
    """

    in_fastq_file_1 = luigi.Parameter()
    genome_file = luigi.Parameter()
    index_file = luigi.Parameter()
    abundance = luigi.Parameter()
    abundance_h5 = luigi.Parameter()
    run_info = luigi.Parameter()
    user_python_path = luigi.Parameter()

    def output(self):
        """
        Returns
        -------
        output : luigi.LocalTarget()
            Location of the bam output file
        """
        return [
            luigi.LocalTarget(self.index_file),
            luigi.LocalTarget(self.abundance),
            luigi.LocalTarget(self.abundance_h5),
            luigi.LocalTarget(self.run_info)
        ]

    def work(self):
        """
        Worker function for filtering bam files using BioBamBam

        Parameters
        ----------
        in_bam_file : str
            Location of the bam file to filter
        in_bam_file : str
            Location of the filtered bam file
        """
        kallisto_index_handle = kallistoIndexerTool()
        kallisto_index_handle.kallisto_indexer(
            self.genome_file,
            self.index_file
        )

        kallisto_quant_handle = kallistoQuantificationTool()
        kallisto_quant_handle.kallisto_quant_single(
            self.index_file,
            self.in_fastq_file_1,
            self.abundance_h5,
            self.abundance,
            self.run_info
        )


class ProcessKallistoPaired(LSFJobTask):
    """
    Tool wrapper for filtering bams using BioBamBam.
    """

    in_fastq_file_1 = luigi.Parameter()
    in_fastq_file_2 = luigi.Parameter()
    genome_file = luigi.Parameter()
    index_file = luigi.Parameter()
    abundance = luigi.Parameter()
    abundance_h5 = luigi.Parameter()
    run_info = luigi.Parameter()
    user_python_path = luigi.Parameter()

    def output(self):
        """
        Returns
        -------
        output : luigi.LocalTarget()
            Location of the bam output file
        """
        return [
            luigi.LocalTarget(self.index_file),
            luigi.LocalTarget(self.abundance),
            luigi.LocalTarget(self.abundance_h5),
            luigi.LocalTarget(self.run_info)
        ]

    def work(self):
        """
        Worker function for filtering bam files using BioBamBam

        Parameters
        ----------
        in_bam_file : str
            Location of the bam file to filter
        in_bam_file : str
            Location of the filtered bam file
        """
        kallisto_index_handle = kallistoIndexerTool()
        kallisto_index_handle.kallisto_indexer(
            self.genome_file,
            self.index_file
        )

        kallisto_quant_handle = kallistoQuantificationTool()
        kallisto_quant_handle.kallisto_quant_paired(
            self.index_file,
            self.in_fastq_file_1,
            self.in_fastq_file_2,
            self.abundance_h5,
            self.abundance,
            self.run_info
        )
