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

from tool.biobambam_filter import biobambam


class ProcessBioBamBam(LSFJobTask):
    """
    Tool wrapper for filtering bams using BioBamBam.
    """

    in_bam_file = luigi.Parameter()
    out_bam_file = luigi.Parameter()

    def output(self):
        """
        Returns
        -------
        output : luigi.LocalTarget()
            Location of the bam output file
        """
        return luigi.LocalTarget(self.out_bam_file)

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
        bbb = biobambam()
        bbb.biobambam_filter_alignments(self.in_bam_file, self.out_bam_file)
