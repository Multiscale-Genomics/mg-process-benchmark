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

from tool.macs2 import macs2


class ProcessMacs2Nobgd(LSFJobTask):
    """
    Tool wrapper for filtering bams using BioBamBam.
    """

    in_bam_file = luigi.Parameter()
    macs2_params = luigi.Parameter()
    narrowPeak_file = luigi.Parameter()
    summits_file = luigi.Parameter()
    broadPeak_file = luigi.Parameter()
    gappedPeak_file = luigi.Parameter()

    def output(self):
        """
        Returns
        -------
        output : luigi.LocalTarget()
            Location of the bam output file
        """
        return [
            luigi.LocalTarget(self.narrowPeak_file),
            luigi.LocalTarget(self.summits_file),
            luigi.LocalTarget(self.broadPeak_file),
            luigi.LocalTarget(self.gappedPeak_file)
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
        macs2_handle = macs2()

        real_params = []
        if len(self.macs2_params) > 0:
            real_params = self.macs2_params.split(",")

        macs2_handle.macs2_peak_calling_nobgd(
            "luigi_lsf", self.in_bam_file, real_params,
            self.narrowPeak_file, self.summits_file,
            self.broadPeak_file, self.gappedPeak_file
        )


class ProcessMacs2bgd(LSFJobTask):
    """
    Tool wrapper for filtering bams using BioBamBam.
    """

    in_bam_file = luigi.Parameter()
    in_bgd_bam_file = luigi.Parameter()
    macs2_params = luigi.Parameter()
    narrowPeak_file = luigi.Parameter()
    summits_file = luigi.Parameter()
    broadPeak_file = luigi.Parameter()
    gappedPeak_file = luigi.Parameter()

    def output(self):
        """
        Returns
        -------
        output : luigi.LocalTarget()
            Location of the bam output file
        """
        return [
            luigi.LocalTarget(self.narrowPeak_file),
            luigi.LocalTarget(self.summits_file),
            luigi.LocalTarget(self.broadPeak_file),
            luigi.LocalTarget(self.gappedPeak_file)
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
        macs2_handle = macs2()

        real_params = []
        if len(self.macs2_params) > 0:
            real_params = self.macs2_params.split(",")

        macs2_handle.macs2_peak_calling(
            "luigi_lsf", self.in_bam_file, self.in_bgd_bam_file, real_params,
            self.narrowPeak_file, self.summits_file,
            self.broadPeak_file, self.gappedPeak_file
        )
