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

import sys
import argparse

import luigi

from TaskWrappers.macs2 import ProcessMacs2bgd, ProcessMacs2Nobgd

SHARED_TMP_DIR = ""
RESOURCE_FLAG_ALIGNMENT = "mem=16384"
MEMORY_FLAG_ALIGNMENT = "16384"
RESOURCE_FLAG_MERGE = "mem=16384"
MEMORY_FLAG_MERGE = "16384"
QUEUE_FLAG = "production-rh7"
SAVE_JOB_INFO = False


class macs2_nobgd(luigi.Task):
    """
    Pipeline for aligning single end reads using BWA ALN
    """

    in_bam_file = luigi.Parameter()
    in_bgd_bam_file = luigi.Parameter()
    macs2_params = luigi.Parameter()
    narrowPeak_file = luigi.Parameter()
    summits_file = luigi.Parameter()
    broadPeak_file = luigi.Parameter()
    gappedPeak_file = luigi.Parameter()
    user_python_path = luigi.Parameter()

    def output(self):
        """
        Returns
        -------
        output : luigi.LocalTarget()
            Location of the merged aligned reads in bam format
        """
        return [
            luigi.LocalTarget(self.narrowPeak_file),
            luigi.LocalTarget(self.summits_file),
            luigi.LocalTarget(self.broadPeak_file),
            luigi.LocalTarget(self.gappedPeak_file)
        ]

    def run(self):
        """
        Worker function for aligning single ended FASTQ reads using BWA ALN

        Parameters
        ----------
        in_bam_file : str
            Location of the bam file
        out_bam_file : str
            Location of the filtered bam file
        """
        print("### PROCESSING BAM FILE")
        macs2_handle = ProcessMacs2Nobgd(
            in_bam_file=self.in_bam_file, macs2_params=self.macs2_params,
            narrowPeak_file=self.narrowPeak_file, summits_file=self.summits_file,
            broadPeak_file=self.broadPeak_file, gappedPeak_file=self.gappedPeak_file,
            n_cpu_flag=1, shared_tmp_dir=SHARED_TMP_DIR, queue_flag=QUEUE_FLAG,
            job_name_flag="macs2bgd", save_job_info=SAVE_JOB_INFO,
            extra_bsub_args=self.user_python_path)
        yield macs2_handle


class macs2_bgd(luigi.Task):
    """
    Pipeline for aligning single end reads using BWA ALN
    """

    in_bam_file = luigi.Parameter()
    in_bgd_bam_file = luigi.Parameter()
    macs2_params = luigi.Parameter()
    narrowPeak_file = luigi.Parameter()
    summits_file = luigi.Parameter()
    broadPeak_file = luigi.Parameter()
    gappedPeak_file = luigi.Parameter()
    user_python_path = luigi.Parameter()

    def output(self):
        """
        Returns
        -------
        output : luigi.LocalTarget()
            Location of the merged aligned reads in bam format
        """
        return [
            luigi.LocalTarget(self.narrowPeak_file),
            luigi.LocalTarget(self.summits_file),
            luigi.LocalTarget(self.broadPeak_file),
            luigi.LocalTarget(self.gappedPeak_file)
        ]

    def run(self):
        """
        Worker function for aligning single ended FASTQ reads using BWA ALN

        Parameters
        ----------
        in_bam_file : str
            Location of the bam file
        out_bam_file : str
            Location of the filtered bam file
        """
        print("### PROCESSING BAM FILE")
        macs2_handle = ProcessMacs2bgd(
            in_bam_file=self.in_bam_file, in_bgd_bam_file=self.in_bgd_bam_file,
            macs2_params=self.macs2_params, narrowPeak_file=self.narrowPeak_file,
            summits_file=self.summits_file, broadPeak_file=self.broadPeak_file,
            gappedPeak_file=self.gappedPeak_file,
            n_cpu_flag=1, shared_tmp_dir=SHARED_TMP_DIR, queue_flag=QUEUE_FLAG,
            job_name_flag="macs2", save_job_info=SAVE_JOB_INFO,
            extra_bsub_args=self.user_python_path)
        yield macs2_handle


if __name__ == "__main__":
    # Set up the command line parameters
    PARSER = argparse.ArgumentParser(description="BioBamBam Pipeline Wrapper")
    PARSER.add_argument("--in_bam_file", help="")
    PARSER.add_argument("--in_bgd_bam_file", default=None, help="")
    PARSER.add_argument("--macs2_params", default="", help="")
    PARSER.add_argument("--narrowPeak_file", help="")
    PARSER.add_argument("--summits_file", help="")
    PARSER.add_argument("--broadPeak_file", help="")
    PARSER.add_argument("--gappedPeak_file", help="")
    PARSER.add_argument("--shared_tmp_dir", help="")
    PARSER.add_argument("--python_path", default=sys.executable, help="")

    # Get the matching parameters from the command line
    ARGS = PARSER.parse_args()

    SHARED_TMP_DIR = ARGS.shared_tmp_dir

    if ARGS.in_bgd_bam_file is not None:
        luigi.build(
            [
                macs2_bgd(
                    in_bam_file=ARGS.in_bam_file, in_bgd_bam_file=ARGS.in_bgd_bam_file,
                    macs2_params=ARGS.macs2_params, narrowPeak_file=ARGS.narrowPeak_file,
                    summits_file=ARGS.summits_file, broadPeak_file=ARGS.broadPeak_file,
                    gappedPeak_file=ARGS.gappedPeak_file,
                    user_python_path=ARGS.python_path
                )
            ],
            local_scheduler=True, workers=5)
    else:
        luigi.build(
            [
                macs2_nobgd(
                    in_bam_file=ARGS.in_bam_file, macs2_params=ARGS.macs2_params,
                    narrowPeak_file=ARGS.narrowPeak_file, summits_file=ARGS.summits_file,
                    broadPeak_file=ARGS.broadPeak_file, gappedPeak_file=ARGS.gappedPeak_file,
                    user_python_path=ARGS.python_path
                )
            ],
            local_scheduler=True, workers=5)
